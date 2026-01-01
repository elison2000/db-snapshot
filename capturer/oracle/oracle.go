package oracle

import (
	"bytes"
	"context"
	"database/sql"
	"db-snapshot/config"
	"db-snapshot/html"
	"db-snapshot/model"
	"db-snapshot/util"
	"fmt"
	"github.com/gookit/slog"
	"gorm.io/gorm"
	"math"
	"strconv"
	"strings"
	"time"
)

const LongOpsSQL = `select to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') create_time,sid, serial# serial, username, sql_id,time_remaining, elapsed_seconds,round(sofar/totalwork*100) completed_pct, opname, target, target_desc, sofar, totalwork, units, 
       to_char(start_time,'yyyy-mm-dd hh24:mi:ss') start_time, to_char(last_update_time,'yyyy-mm-dd hh24:mi:ss') last_update_time
from v$session_longops where time_remaining > 0 order by time_remaining desc`

const ActSessSQL = `SELECT /*+ OPT_PARAM('_optimizer_adaptive_plans','false') NO_MONITOR */
    to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as current_time,
    s.sid,
    s.serial#,
    s.username,
    s.program,
    s.machine,
    s.sql_id,
    s.prev_sql_id,
    s.last_call_et as exec_sec,
    s.blocking_session,
    s.final_blocking_session,
    s.event,
    s.wait_class,
    s.state,
    CASE 
        WHEN s.state = 'WAITING' THEN s.seconds_in_wait
        ELSE 0 
    END as wait_sec,
    p1,p2,p3
FROM 
    v$session s
WHERE   s.status = 'ACTIVE'
    AND s.type = 'USER'
    AND s.username IS NOT NULL
    AND s.sql_id IS NOT NULL
    AND s.program not like '%(MS0%)'
ORDER BY 
    s.last_call_et DESC`

const TxnSQL = `select to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') create_time,
       s.sid,
       s.username,
       s.machine,
       s.program,
       s.status,
       decode(s.command, 3, 'select', 2, 'insert', 6, 'update', 7, 'delete', 'other') command_type,
       s.sql_id,
       s.prev_sql_id,
       s.wait_class,
       s.event,
       s.blocking_session,
       s.last_call_et elapsed_sec,
       xidusn || '.' || xidslot || '.' || xidsqn xid,
       t.status txn_status,
       to_char(t.start_date,'yyyy-mm-dd hh24:mi:ss') txn_start_time,
       round((sysdate - t.start_date)*3600*24) txn_elapsed_sec,
       t.cr_get,
       t.phy_io,
       t.used_ublk used_blocks,
       t.used_urec undo_rows
  from v$session s, v$transaction t
 where s.taddr = t.addr
 order by start_date`

const BlockerSQL = `with blocker as (select /*+ materialize */ distinct final_blocking_session as sid from v$session)
SELECT /*+ LEADING(b s) USE_NL(s) NO_MERGE(b) */ 
    to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') create_time,
    s.sid,
    serial# serial,
    username,
    machine,
    program,
    decode(command, 3, 'select', 2, 'insert', 6, 'update', 7, 'delete', 'other') command_type,
    sql_id,
    prev_sql_id,
    status,
    state,
    wait_class,
    event,
    to_char(logon_time, 'yyyy-mm-dd hh24:mi:ss') logon_time,
    CASE
        WHEN state = 'WAITING' THEN seconds_in_wait
        WHEN state IN ('WAITED SHORT TIME', 'WAIT UNKNOW TIME') THEN NULL
        WHEN state = 'WAITING KNOWN TIME' THEN wait_time
        ELSE seconds_in_wait
    END wait_sec,
    last_call_et exec_sec,
    s.blocking_session,
    s.final_blocking_session,
    s.p1,s.p2,s.p3
FROM blocker b,v$session s
WHERE
    s.sid = b.sid 
ORDER BY
    last_call_et desc`

const UserSessCountSQL = `select to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') create_time,username db,count(*) cnt from v$session WHERE TYPE<>'BACKGROUND' group by username order by 3 desc`

const ClientSessCountSQL = `select * from (select to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') create_time,machine client,count(*) cnt from v$session WHERE TYPE<>'BACKGROUND' group by machine order by 3 desc) where rownum<=100`

// sql_id=  FIXED INDEX sql_id in 不走索引，构造临时表 使用USE_NL
const SQLInfo = `select /*+ LEADING(t) USE_NL(s) NO_MERGE(t) PUSH_PRED(s) */ s.sql_id,to_char(last_active_time,'yyyy-mm-dd hh24:mi:ss') last_active_time,executions,round(elapsed_time/1000000,2) elapsed_time_sec,case when executions<>0 then round(elapsed_time/executions/1000000,2) end avg_elapsed_time_sec,substr(sql_text,1,2000) 
from (SELECT column_value as sql_id FROM TABLE(sys.odcivarchar2list(%s))) t
JOIN v$sqlstats s ON s.sql_id = t.sql_id`

type Capturer struct {
	InstID     int
	Host       string
	Port       int
	CreateTime string
	DBName     string
	DB         *sql.DB
}

func (self *Capturer) Init() error {
	cfg := &model.DBConfig{self.Host, self.Port, config.Global.MonitorUser, config.Global.MonitorPassword, self.DBName}

	db, err := util.NewOracleDB(cfg)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	err = db.PingContext(ctx)
	if err != nil {
		return err
	}

	self.DB = db
	return nil
}

func (self *Capturer) Close() {
	self.DB.Close()
}

func (self *Capturer) getLongOps() ([][]string, error) {
	t := time.Now()
	defer func() {
		slog.Infof("[%s:%d] getLongOps 耗时 %d s", self.Host, self.Port, int(time.Since(t).Seconds()))
	}()
	rows, err := util.QueryReturnList(self.DB, LongOpsSQL)
	if err != nil {
		return nil, fmt.Errorf("getLongOps-> %w", err)
	}

	return rows, nil
}

func (self *Capturer) getActSess() ([][]string, error) {
	t := time.Now()
	defer func() {
		slog.Infof("[%s:%d] getActSess 耗时 %d s", self.Host, self.Port, int(time.Since(t).Seconds()))
	}()
	rows, err := util.QueryReturnList(self.DB, ActSessSQL)
	if err != nil {
		return nil, fmt.Errorf("getActSess-> %w", err)
	}
	return rows, nil
}

func (self *Capturer) getTxn() ([][]string, error) {
	t := time.Now()
	defer func() {
		slog.Infof("[%s:%d] getTxn 耗时 %d s", self.Host, self.Port, int(time.Since(t).Seconds()))
	}()
	rows, err := util.QueryReturnList(self.DB, TxnSQL)
	if err != nil {
		return nil, fmt.Errorf("getTxn-> %w", err)
	}
	return rows, nil
}

func (self *Capturer) getBlocker() ([][]string, error) {
	t := time.Now()
	defer func() {
		slog.Infof("[%s:%d] getBlocker 耗时 %d s", self.Host, self.Port, int(time.Since(t).Seconds()))
	}()
	rows, err := util.QueryReturnList(self.DB, BlockerSQL)
	if err != nil {
		return nil, fmt.Errorf("getBlocker-> %w", err)
	}
	return rows, nil
}

func (self *Capturer) getUserSessCount() ([][]string, error) {
	t := time.Now()
	defer func() {
		slog.Infof("[%s:%d] getUserSessCount 耗时 %d s", self.Host, self.Port, int(time.Since(t).Seconds()))
	}()
	rows, err := util.QueryReturnList(self.DB, UserSessCountSQL)
	if err != nil {
		return nil, fmt.Errorf("getUserSessCount-> %w", err)
	}
	return rows, nil
}

func (self *Capturer) getClientSessCount() ([][]string, error) {
	t := time.Now()
	defer func() {
		slog.Infof("[%s:%d] getClientSessCount 耗时 %d s", self.Host, self.Port, int(time.Since(t).Seconds()))
	}()
	rows, err := util.QueryReturnList(self.DB, ClientSessCountSQL)
	if err != nil {
		return nil, fmt.Errorf("getClientSessCount-> %w", err)
	}
	return rows, nil
}

func (self *Capturer) getSQLInfo(sqlIds []string) ([][]string, error) {
	t := time.Now()
	defer func() {
		slog.Infof("[%s:%d] getSQLInfo 耗时 %d s", self.Host, self.Port, int(time.Since(t).Seconds()))
	}()
	if len(sqlIds) == 0 {
		return nil, fmt.Errorf("getSQLInfo-> sqlIds is empty")
	}
	var buf bytes.Buffer
	for i, sqlId := range sqlIds {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString("'")
		buf.WriteString(sqlId)
		buf.WriteString("'")
	}
	query := fmt.Sprintf(SQLInfo, buf.String())
	rows, err := util.QueryReturnList(self.DB, query)
	if err != nil {
		return nil, fmt.Errorf("getSQLInfo-> %w", err)
	}
	return rows, nil

}

func (self *Capturer) Capture(db *gorm.DB) {
	now := time.Now()
	self.CreateTime = now.Format("2006-01-02 15:04:05")
	dirName := fmt.Sprintf("data/%s/%d/", now.Format("200601"), self.InstID)
	fileName := fmt.Sprintf("%s.html", now.Format("20060102_150405"))

	sum := &model.DBSnapshot{InstID: self.InstID, CreateTime: self.CreateTime}

	//收集快照数据
	longOpsList, err1 := self.getLongOps()
	actSessList, err2 := self.getActSess()
	txnList, err3 := self.getTxn()
	BlockerList, err4 := self.getBlocker()
	//lockObjList, err5 := self.getLockObj()

	userSessCountList, err6 := self.getUserSessCount()
	clientSessCountList, err7 := self.getClientSessCount()

	if err1 != nil {
		sum.Msg += fmt.Sprintf("%v\n", err1)
	}
	if err2 != nil {
		sum.Msg += fmt.Sprintf("%v\n", err2)
	}
	if err3 != nil {
		sum.Msg += fmt.Sprintf("%v\n", err3)
	}
	if err4 != nil {
		sum.Msg += fmt.Sprintf("%v\n", err4)
	}
	//if err5 != nil {
	//	sum.Msg += fmt.Sprintf("%v\n", err5)
	//}
	if err6 != nil {
		sum.Msg += fmt.Sprintf("%v\n", err6)
	}
	if err7 != nil {
		sum.Msg += fmt.Sprintf("%v\n", err7)
	}

	if sum.Msg != "" {
		slog.Errorf("[%s:%d] 获取快照数据报错: %s\n", self.Host, self.Port, sum.Msg)
	}

	sum.BigQueryCount = len(longOpsList)
	sum.ActSessCount = len(actSessList)
	sum.TxnCount = len(txnList)

	//计算行锁数
	sum.LockCount = func() int {
		cnt := 0
		for _, v := range actSessList {
			if strings.HasPrefix(v[11], "enq: TX") {
				cnt += 1
			}
		}
		return cnt
	}()

	sum.SessCount = func() int {
		cnt := 0
		for _, v := range userSessCountList {
			n, _ := strconv.Atoi(v[2])
			cnt += n
		}
		return cnt
	}()

	//计算等待状态的会话数
	sum.WaitSessCount = func() int {
		cnt := 0
		for _, v := range actSessList {
			if v[9] != "NULL" { //阻塞者不为 NULL就是等待状态
				cnt += 1
			}
		}
		return cnt
	}()

	//计算最大查询的执行时间
	sum.MaxQuerySeconds = func() int {
		if len(actSessList) == 0 {
			return 0
		}
		head := actSessList[0]
		n, _ := strconv.Atoi(head[8])
		return n
	}()

	//计算最大事务的执行时间
	sum.MaxTxnSeconds = func() int {
		if len(txnList) == 0 {
			return 0
		}
		head := txnList[0]
		n, _ := strconv.Atoi(head[16])
		return n
	}()

	//获取sqlid列表
	sqlIdMap := make(map[string]struct{})

	for _, row := range longOpsList {
		sqlIdMap[row[4]] = struct{}{}
	}

	for _, row := range actSessList {
		sqlIdMap[row[6]] = struct{}{}
		sqlIdMap[row[7]] = struct{}{}
	}

	for _, row := range txnList {
		sqlIdMap[row[7]] = struct{}{}
		sqlIdMap[row[8]] = struct{}{}
	}

	for _, row := range BlockerList {
		sqlIdMap[row[7]] = struct{}{}
		sqlIdMap[row[8]] = struct{}{}
	}

	var sqlIds []string
	for k := range sqlIdMap {
		sqlIds = append(sqlIds, k)
	}
	//slog.Debugf("[%s:%d]  sqlIds: %v", self.Host, self.Port, sqlIds)

	//获取sql数据
	sqlInfoList, err8 := self.getSQLInfo(sqlIds)
	if err8 != nil {
		sum.Msg += fmt.Sprintf("%v\n", err8)
	}

	page := html.Html{}
	page.AddHead1(self.CreateTime, self.InstID, self.Host, self.Port, &self.DBName)

	fieldNames := []string{"活动会话数", "事务数", "总连接数", "大查询数", "等待会话数", "行锁数", "最长查询耗时(s)", "最长事务耗时(s)"}
	refId := []string{"actSess", "txn", "sessCount", "longOps", "actSess", "actSess", "actSess", "txn"}
	page.AddHeadWithHref(fieldNames, refId, []int{sum.ActSessCount, sum.TxnCount, sum.SessCount, sum.BigQueryCount, sum.WaitSessCount, sum.LockCount, sum.MaxQuerySeconds, sum.MaxTxnSeconds})

	th1 := []string{"当前时间", "SID", "Serial", "用户", "当前SQL", "剩余时间", "执行时间(s)", "完成百分比", "操作名称", "涉及的对象", "涉及的对象说明", "已完成工作量", "总工作量", "单位", "开始时间", "最后更新时间"}
	th2 := []string{"当前时间", "SID", "Serial", "用户", "客户端程序", "客户端", "当前SQL", "上一个SQL", "执行时间(s)", "阻塞者", "最终阻塞者", "等待事件", "等待类型", "等待状态", "等待时间(s)", "P1", "P2", "P3"}
	th3 := []string{"当前时间", "SID", "用户", "客户端", "客户端程序", "会话状态", "命令类型", "当前SQL", "上一个SQL", "等待类型", "等待事件", "阻塞者", "执行时间(s)", "XID", "事务状态", "事务开始时间", "事务已耗时(s)", "一致性读", "物理IO", "已使用的块数", "undo行数"}
	th4 := []string{"当前时间", "SID", "Serial", "用户", "客户端", "客户端程序", "命令类型", "当前SQL", "上一个SQL", "会话状态", "等待状态", "等待类型", "等待事件", "登录时间", "等待时间(s)", "执行时间(s)", "阻塞者", "最终阻塞者", "P1", "P2", "P3"}
	//th5 := []string{"当前时间", "库名", "对象名", "SID", "用户", "客户端", "程序", "等待事件", "阻塞者", "最终阻塞者", "登录时间", "执行时间(s)", "等待时间(s)", "locked_mode", "P1", "P2", "P3"}
	//th5 := []string{"当前时间", "实例", "SID", "用户", "客户端程序", "状态", "等待事件", "执行时间(s)", "持有锁数", "加锁的对象"}

	th6 := []string{"SQLID", "最后活动时间", "执行次数", "执行时间(s)", "平均执行时间(s)", "SQL文本"}
	th7 := []string{"当前时间", "用户", "连接数"}
	th8 := []string{"当前时间", "客户端", "连接数"}

	page.AddTableWithClassID("长操作", "longOps", th1, longOpsList)
	page.AddTableWithClassIDAndRowHref("活动会话", "actSess", th2, actSessList, []int{6, 7})
	page.AddTableWithClassIDAndRowHref("事务", "txn", th3, txnList, []int{7, 8})
	page.AddTableWithClassIDAndRowHref("阻塞者", "blocker", th4, BlockerList, []int{7, 8})
	//page.AddTableWithClassID("加锁的会话与对象", "lockObj", th5, lockObjList)

	//不能放在页尾，跳转不精准
	page.AddTableRowWithClassID("SQL信息", th6, sqlInfoList, 0)

	page.AddTableWithClassID("连接汇总(用户)", "sessCount", th7, userSessCountList)
	page.AddTable("连接汇总(客户端)", th8, clientSessCountList)

	err := page.SaveToBrotli(dirName, fileName)
	if err != nil {
		slog.Errorf("[%s:%d] 保存快照文件报错: %v", self.Host, self.Port, err)
	}

	sum.DurationSeconds = int(math.Round(time.Since(now).Seconds()))
	//保存快照汇总数据
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	err = db.WithContext(ctx).Create(&sum).Error
	if err != nil {
		slog.Errorf("[%s:%d] 保存快照汇总数据失败: %v", self.Host, self.Port, err)
	}

	slog.Infof("[%s:%d] 保存快照汇总数据成功 %+v", self.Host, self.Port, *sum)
}
