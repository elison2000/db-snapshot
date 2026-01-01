package mysql

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
	"strings"
	"time"
)

const ActSessSQL = `select now() create_time, id,user,db,substring_index(host,':',1) client,time exec_time,command,state,info sql_text
	 from information_schema.processlist
	where id <> connection_id() and user not in ('system user','event_scheduler','replicator','aurora')
	  and command not in ( 'sleep','Binlog Dump','Binlog Dump GTID') order by exec_time desc`

const TxnSQL = `select
	now() create_time,
	trx_mysql_thread_id p_id,
	b.user,
	b.db,
	substring_index(b.host,':',1) client,
	b.command p_command,
	b.state p_state,
	b.time p_exec_time,
	trx_id,
	trx_started,
	trx_state,
	trx_operation_state,
	timestampdiff(second,trx_started,now()) txn_exec_time,
	ifnull(timestampdiff(second, trx_wait_started, now()), 0) txn_wait_time,
	trx_tables_locked,
	trx_rows_locked,
	trx_rows_modified,
	trx_isolation_level,
	trx_query
from
	information_schema.innodb_trx a left join information_schema.processlist b on b.id = a.trx_mysql_thread_id
order by
	txn_exec_time desc`

const SessCountSQL = `select now() create_time,user,db,count(*) cnt from information_schema.processlist group by user,db order by count(*) desc limit 100`

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

func (self *Capturer) getSessCount() ([][]string, error) {
	rows, err := util.QueryReturnList(self.DB, SessCountSQL)
	if err != nil {
		return nil, fmt.Errorf("getSessCount-> %w", err)
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
	sessCountList, err3 := self.getSessCount()

	if err1 != nil {
		sum.Msg += fmt.Sprintf("%v\n", err1)
	}
	if err2 != nil {
		sum.Msg += fmt.Sprintf("%v\n", err2)
	}
	if err3 != nil {
		sum.Msg += fmt.Sprintf("%v\n", err3)
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
		n, _ := strconv.Atoi(head[5])
		return n
	}()

	//计算大查询个数（超过10秒为大查询）
	sum.BigQueryCount = func() int {
		cnt := 0
		for _, v := range actSessList {
			n, _ := strconv.Atoi(v[5])
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
		n, _ := strconv.Atoi(head[12])
		return n
	}()

	//计算等待状态的会话数
	sum.WaitSessCount = func() int {
		cnt := 0
		for _, v := range actSessList {
			if strings.Contains(v[7], "Waiting for ") {
				cnt += 1
			}
		}
		return cnt
	}()

	//计算行锁数量
	sum.LockCount = func() int {
		cnt := 0
		for _, v := range txnList {
			if v[10] == "LOCK WAIT" {
				cnt += 1
			}
		}
		return cnt
	}()

	page := html.Html{}
	page.AddHead1(self.CreateTime, self.InstID, self.Host, self.Port, nil)
	page.AddHead2([]string{"活动会话数", "事务数", "总连接数", "大查询数", "等待会话数", "被锁事务数", "最长查询耗时(s)", "最长事务耗时(s)"},
		[]int{sum.ActSessCount, sum.TxnCount, sum.SessCount, sum.BigQueryCount, sum.WaitSessCount, sum.LockCount, sum.MaxQuerySeconds, sum.MaxTxnSeconds})

	th1 := []string{"当前时间", "PID", "用户", "库名", "客户端", "执行时间(s)", "命令", "状态", "SQL文本"}
	th2 := []string{"当前时间", "PID", "用户", "库名", "客户端", "线程命令", "线程状态", "线程执行时间(s)", "事务ID", "事务开始时间", "事务状态", "事务操作状态", "事务执行时间(s)", "等待时间(s)", "锁表数", "锁记录数", "修改行数", "事务隔离级别", "SQL文本"}
	th3 := []string{"当前时间", "用户", "库名", "连接数"}

	page.AddTable("活动会话", th1, actSessList)
	page.AddTable("事务", th2, txnList)
	page.AddTable("连接汇总", th3, sessCountList)

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
