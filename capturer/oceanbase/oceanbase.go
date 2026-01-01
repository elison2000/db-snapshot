package oceanbase

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

const ActSessSQL = `select curtime() create_time, svr_ip, id, user, db, user_client_ip client, tenant, round(time,3) exec_time, command, state, trans_id, info sqltext FROM oceanbase.gv$ob_processlist where state<>'SLEEP' order by exec_time desc`

const TxnSQL = `with b as (select trans_id,min(ctx_create_time) ctx_create_time from oceanbase.__all_virtual_trans_stat group by trans_id)
select curtime() create_time, svr_ip, id, user, db, user_client_ip client, tenant, round(time,3) exec_time, date_format(ctx_create_time,'%Y-%m-%d %H:%i:%s') txn_start, ifnull(timestampdiff(second,b.ctx_create_time,now()),0) txn_exec_sec,command, a.state, a.trans_id, info sqltext 
FROM oceanbase.gv$ob_processlist a join b on a.trans_id=b.trans_id order by txn_exec_sec desc`

const LockSQL = `with t as (
select a.id1 blocker_txn,a.trans_id waiter_txn,b.id1 from oceanbase.gv$ob_locks a join oceanbase.gv$ob_locks b on a.trans_id=b.trans_id and a.block=1 and a.type='TX' and b.block=1 and b.type='TR')
select bt.session_id,bt.tx_id,bt.ctx_create_time,timestampdiff(second,bt.ctx_create_time,now()) txn_exec_sec,bt.last_request_time,wt.session_id,wt.tx_id,wt.ctx_create_time,timestampdiff(second,wt.ctx_create_time,now()) txn_exec_sec,wt.last_request_time
from t left join oceanbase.gv$ob_transaction_participants bt on bt.tx_id=t.blocker_txn left join oceanbase.gv$ob_transaction_participants wt on wt.tx_id=t.waiter_txn`

const LockObjSQL = `select distinct a.trans_id,a.id1 tablet_id,a.id2 "blockingTxn-key",DATABASE_NAME,TABLE_NAME,TABLE_ID,TABLE_TYPE from oceanbase.gv$ob_locks a left join oceanbase.dba_ob_table_locations b on a.id1=b.tablet_id where a.block=1 and a.type='TR' order by ctime`

const SessCountSQL = `select curtime() create_time,user,db,count(*) cnt from oceanbase.gv$ob_processlist group by user,db order by count(*) desc limit 100`

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
	db, err := util.NewMysqlDB(cfg)
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

func (self *Capturer) getLockObj() ([][]string, error) {
	rows, err := util.QueryReturnList(self.DB, LockObjSQL)
	if err != nil {
		return nil, fmt.Errorf("getLockObj-> %w", err)
	}
	return rows, nil
}

func (self *Capturer) getSessCount() ([][]string, error) {
	rows, err := util.QueryReturnList(self.DB, SessCountSQL)
	if err != nil {
		return nil, fmt.Errorf("getSessCount-> %w", err)
	}
	return rows, nil
}

// 有性能问题，停止这个逻辑
//func (self *Capturer) getBlockerSqlHist() [][]string {
//	sql := `with t as (select t.sid pid,request_time,t.ret_code,affected_rows,t.elapsed_time / 1000000 elapsed_sec,t.query_sql sqltext from oceanbase.gv$ob_sql_audit t where sid in (select a.SESSION_ID from oceanbase.gv$ob_transaction_participants a where tx_id in( select distinct id1 from  oceanbase.gv$ob_locks  where block=1 and type='TX')))
//select pid,from_unixtime(request_time div 1000000) request_at,ret_code,affected_rows,elapsed_sec,sqltext from (select row_number() over (partition by pid order by request_time desc) as rn,* from t) tmp where rn<4 order by pid,request_time`
//	rows, err := self.DB.QueryReturnList(sql)
//	if err != nil {
//		slog.Errorf("[%s:%d] 查询活动连接数据失败: %v", self.Host, self.Port, err)
//	}
//	return rows
//}

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
	lockObjList, err4 := self.getLockObj()
	sessCountList, err5 := self.getSessCount()

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

	if sum.Msg != "" {
		slog.Errorf("[%s:%d] 获取快照数据报错: %s\n", self.Host, self.Port, sum.Msg)
	}

	//统计数据
	sum.TxnCount = len(txnList)
	sum.ActSessCount = len(actSessList)

	//计算总连接数
	sum.SessCount = func() int {
		cnt := 0
		for _, v := range sessCountList {
			n, _ := strconv.Atoi(v[3])
			cnt += n
		}
		return cnt
	}()

	//计算最大查询的执行时间
	sum.MaxQuerySeconds = func() int {
		if len(actSessList) == 0 {
			return 0
		}
		head := actSessList[0]
		n, _ := strconv.ParseFloat(head[7], 64)
		return int(math.Round(n))
	}()

	//计算大查询个数（超过10秒为大查询）
	sum.BigQueryCount = func() int {
		cnt := 0
		for _, v := range actSessList {
			n, _ := strconv.ParseFloat(v[7], 64)
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
		n, _ := strconv.Atoi(head[9])
		return n
	}()

	//计算等待状态的会话数
	sum.WaitSessCount = len(lockList)

	//计算行锁数量
	sum.LockCount = len(lockObjList)

	page := html.Html{}
	page.AddHead1(self.CreateTime, self.InstID, self.Host, self.Port, nil)
	page.AddHead2([]string{"活动会话数", "事务数", "总连接数", "大查询数", "等待会话数", "被锁对象数", "最长查询耗时(s)", "最长事务耗时(s)"},
		[]int{sum.ActSessCount, sum.TxnCount, sum.SessCount, sum.BigQueryCount, sum.WaitSessCount, sum.LockCount, sum.MaxQuerySeconds, sum.MaxTxnSeconds})

	th1 := []string{"当前时间", "节点", "PID", "用户", "库名", "客户端", "租户", "执行时间(s)", "命令", "状态", "事务ID", "SQL文本"}
	th2 := []string{"当前时间", "节点", "PID", "用户", "库名", "客户端", "租户", "执行时间(s)", "事务开始时间", "事务执行时间(s)", "命令", "状态", "事务ID", "SQL文本"}
	th3 := []string{"堵塞者PID", "堵塞者事务ID", "事务开始时间", "事务耗时(s)", "最后请求时间", "等待者PID", "等待者事务ID", "事务开始时间", "事务耗时(s)", "最后请求时间"}
	th4 := []string{"事务ID", "被锁对象", "持有锁事务和ID", "库名", "表名", "表ID", "表类型"}
	//th5 := []string{"堵塞者PID", "请求时间", "返回码", "影响行数", "耗时(s)", "SQL文本"}
	th5 := []string{"当前时间", "用户", "库名", "连接数"}

	page.AddTable("活动连接", th1, actSessList)
	page.AddTable("事务", th2, txnList)
	page.AddTable("堵塞会话", th3, lockList)
	page.AddTable("被锁对象", th4, lockObjList)
	page.AddTable("连接汇总", th5, sessCountList)

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
