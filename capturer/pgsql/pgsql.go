package pgsql

import (
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
	"time"
)

const ActSessSQL = `select to_char(now(), 'yyyy-mm-dd hh24:mi:ss') create_time,pid,datname as db,usename as user,application_name,backend_type,client_addr client,state,wait_event_type,wait_event,round(extract(epoch FROM (now()-query_start))::numeric,1) as duration_ses,to_char(query_start,'yyyy-mm-dd hh24:mi:ss') query_start,query sql_text from pg_stat_activity where state<>'idle' order by duration_ses desc`

const TxnSQL = `select to_char(now(), 'yyyy-mm-dd hh24:mi:ss') create_time,pid,datname as db,usename as user,application_name,backend_type,client_addr client,state,wait_event_type,wait_event,
round(extract(epoch from (now()-xact_start))::numeric,1) txn_exec_time,round(extract(epoch from (now()-query_start))::numeric,1) exec_time,
to_char(xact_start,'yyyy-mm-dd hh24:mi:ss') txn_start,to_char(query_start,'yyyy-mm-dd hh24:mi:ss') query_start,
query sql_text from pg_stat_activity 
where state in ('active', 'idle in transaction') and xact_start is not null order by xact_start`

const LockSQL = `with lck as ( SELECT pid,COUNT(*) AS lock_count,sum(CASE WHEN GRANTED = 'f' THEN 1 else 0 end) as wait_lock_count,ARRAY_AGG(DISTINCT locktype) AS lock_types FROM pg_locks GROUP BY pid)
SELECT to_char(now(), 'yyyy-mm-dd hh24:mi:ss') create_time,
       lck.pid,
       pg_blocking_pids(lck.pid) blocker_pid,
       psa.datname db,
       psa.application_name,
       to_char(LEAST (query_start, xact_start),'yyyy-mm-dd hh24:mi:ss') start_time,
       psa.STATE,
       round(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - LEAST (query_start, xact_start)))::numeric, 2) AS txn_runtime,
       COALESCE(lck.lock_count, 0) AS lock_count,
       COALESCE(lck.wait_lock_count,0) as wait_lock_count,
       COALESCE(lck.lock_types, '{}') AS lock_types,
       psa.query sqltext
FROM pg_stat_activity psa
JOIN lck ON psa.pid = lck.pid
WHERE psa.state <> 'idle'
ORDER BY xact_start`

const UserSessCountSQL = `select to_char(now(), 'yyyy-mm-dd hh24:mi:ss') create_time,datname as db,usename as user,count(*) cnt from pg_stat_activity group by datname,usename order by cnt desc`

const APPSessCountSQL = `select to_char(now(), 'yyyy-mm-dd hh24:mi:ss') create_time,datname as db,application_name,count(*) cnt from pg_stat_activity group by datname,application_name order by cnt desc`

const ClientSessCountSQL = `select to_char(now(), 'yyyy-mm-dd hh24:mi:ss') create_time,datname as db,client_addr client,count(*) cnt from pg_stat_activity group by datname,client_addr order by cnt desc`

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
	db, err := util.NewPgsqlDB(cfg)
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

func (self *Capturer) getActSess() ([][]string, error) {
	rows, err := util.QueryReturnList(self.DB, ActSessSQL)
	if err != nil {
		return nil, fmt.Errorf("getActSess-> %w", err)
	}
	return rows, nil
}

func (self *Capturer) getTxn() ([][]string, error) {
	rows, err := util.QueryReturnList(self.DB, TxnSQL)
	if err != nil {
		return nil, fmt.Errorf("getTxn-> %w", err)
	}
	return rows, nil
}

func (self *Capturer) getLock() ([][]string, error) {
	rows, err := util.QueryReturnList(self.DB, LockSQL)
	if err != nil {
		return nil, fmt.Errorf("getLock-> %w", err)
	}
	return rows, nil
}

func (self *Capturer) getUserSessCount() ([][]string, error) {
	rows, err := util.QueryReturnList(self.DB, UserSessCountSQL)
	if err != nil {
		return nil, fmt.Errorf("getUserSessCount-> %w", err)
	}
	return rows, nil
}

func (self *Capturer) getAPPSessCount() ([][]string, error) {
	rows, err := util.QueryReturnList(self.DB, APPSessCountSQL)
	if err != nil {
		return nil, fmt.Errorf("getAPPSessCount-> %w", err)
	}
	return rows, nil
}

func (self *Capturer) getClientSessCount() ([][]string, error) {
	rows, err := util.QueryReturnList(self.DB, ClientSessCountSQL)
	if err != nil {
		return nil, fmt.Errorf("getClientSessCount-> %w", err)
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

	actSessList, err1 := self.getActSess()
	txnList, err2 := self.getTxn()
	lockList, err3 := self.getLock()
	userSessCountList, err4 := self.getUserSessCount()
	appSessCountList, err5 := self.getAPPSessCount()
	clientSessCountList, err6 := self.getClientSessCount()

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
	if err5 != nil {
		sum.Msg += fmt.Sprintf("%v\n", err5)
	}
	if err6 != nil {
		sum.Msg += fmt.Sprintf("%v\n", err6)
	}

	if sum.Msg != "" {
		slog.Errorf("[%s:%d] 获取快照数据报错: %s\n", self.Host, self.Port, sum.Msg)
	}

	//统计数据
	sum.TxnCount = len(txnList)
	sum.ActSessCount = len(actSessList)

	//计算总连接数
	sum.SessCount = func() int {
		cnt := 0
		for _, v := range userSessCountList {
			n, _ := strconv.Atoi(v[3])
			cnt += n
		}
		return cnt
	}()

	//计算最大查询的执行时间
	sum.MaxQuerySeconds = func() int {
		for _, v := range actSessList {
			//客户端类型为: client backend  过滤复制会话
			if v[5] == "client backend" {
				n, _ := strconv.ParseFloat(v[10], 64)
				return int(math.Round(n))
			}
		}
		return 0
	}()

	//计算大查询个数（超过10秒为大查询）
	sum.BigQueryCount = func() int {
		cnt := 0
		for _, v := range actSessList {
			n, _ := strconv.ParseFloat(v[10], 64)
			if n > 10 {
				cnt += 1
			}
		}
		return cnt
	}()

	//计算最大事务的执行时间
	sum.MaxTxnSeconds = func() int {
		if len(txnList) == 0 {
			return 0
		}
		head := txnList[0]
		n, _ := strconv.ParseFloat(head[10], 64)
		return int(math.Round(n))
	}()

	//计算锁数
	sum.LockCount = func() int {
		n := 0
		//等待的锁数为1
		for _, v := range lockList {
			if v[9] == "1" {
				n++
			}
		}
		return n
	}()
	sum.WaitSessCount = func() int {
		n := 0
		//等待事件类型为Lock
		for _, v := range actSessList {
			if v[8] == "Lock" {
				n++
			}
		}
		return n
	}()

	page := html.Html{}
	page.AddHead1(self.CreateTime, self.InstID, self.Host, self.Port, nil)
	page.AddHead2([]string{"活动会话数", "事务数", "总连接数", "大查询数", "等待会话数", "被锁会话数", "最长查询耗时(s)", "最长事务耗时(s)"},
		[]int{sum.ActSessCount, sum.TxnCount, sum.SessCount, sum.BigQueryCount, sum.WaitSessCount, sum.LockCount, sum.MaxQuerySeconds, sum.MaxTxnSeconds})

	th1 := []string{"当前时间", "PID", "库名", "用户名", "应用类型", "客户端类型", "客户端", "状态", "等待事件类型", "等待事件", "执行时间(s)", "执行开始时间", "SQL文本"}
	th2 := []string{"当前时间", "PID", "库名", "用户名", "应用类型", "客户端类型", "客户端", "状态", "等待事件类型", "等待事件", "事务执行时间(s)", "执行时间(s)", "事务开始时间", "执行开始时间", "SQL文本"}
	th3 := []string{"当前时间", "PID", "堵塞者PID", "库名", "应用类型", "开始时间", "状态", "事务执行时间(s)", "锁数", "等待的锁数", "锁类型", "SQL文本"}

	th4 := []string{"当前时间", "库名", "用户名", "连接数"}
	th5 := []string{"当前时间", "库名", "应用类型", "连接数"}
	th6 := []string{"当前时间", "库名", "客户端", "连接数"}

	page.AddTable("活动连接", th1, actSessList)
	page.AddTable("事务", th2, txnList)
	page.AddTable("锁（按会话统计）", th3, lockList)

	page.AddTable("连接汇总(按用户)", th4, userSessCountList)
	page.AddTable("连接汇总(按应用类型)", th5, appSessCountList)
	page.AddTable("连接汇总(按客户端)", th6, clientSessCountList)

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
