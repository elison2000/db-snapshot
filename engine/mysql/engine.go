package mysql

import (
	"context"
	"db-snapshot/model"
	"db-snapshot/storage"
	"db-snapshot/util"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/gookit/slog"
	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
)

type Engine struct {
	Name       string
	InstID     int
	Cfg        model.DBConfig
	CreateTime string
	DB         *sqlx.DB
}

func (e *Engine) Init() error {

	db, err := util.NewMysqlDB(&e.Cfg)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	err = db.PingContext(ctx)
	if err != nil {
		return err
	}

	e.DB = db
	return nil
}

func (e *Engine) Close() {
	e.DB.Close()
}

func (e *Engine) getActSess(ctx context.Context) (rows []ActiveSession, err error) {
	err = e.DB.SelectContext(ctx, &rows, ActSessSQL)
	if err != nil {
		return nil, fmt.Errorf("getActSess-> %w", err)
	}
	return rows, nil
}

func (e *Engine) getTxn(ctx context.Context) (rows []Transaction, err error) {
	err = e.DB.SelectContext(ctx, &rows, TxnSQL)
	if err != nil {
		return nil, fmt.Errorf("getTxn-> %w", err)
	}
	return rows, nil
}

func (e *Engine) getUserSessCount(ctx context.Context) (rows []UserSessionCount, err error) {
	err = e.DB.SelectContext(ctx, &rows, UserSessCountSQL)
	if err != nil {
		return nil, fmt.Errorf("getSessCount-> %w", err)
	}
	return rows, nil
}

func (e *Engine) getDBSessCount(ctx context.Context) (rows []DBSessionCount, err error) {
	err = e.DB.SelectContext(ctx, &rows, DBSessCountSQL)
	if err != nil {
		return nil, fmt.Errorf("getSessCount-> %w", err)
	}
	return rows, nil
}

func (e *Engine) getClientSessCount(ctx context.Context) (rows []ClientSessionCount, err error) {
	err = e.DB.SelectContext(ctx, &rows, ClientSessCountSQL)
	if err != nil {
		return nil, fmt.Errorf("getSessCount-> %w", err)
	}
	return rows, nil
}

func (e *Engine) Capture(db *gorm.DB) {
	now := time.Now()
	e.CreateTime = now.Format("2006-01-02 15:04:05")
	snapshotID := now.Format("20060102_150405")

	sum := &model.DBSnapshot{InstID: e.InstID, CreateTime: e.CreateTime}

	//收集快照数据
	var actSessList []ActiveSession
	var txnList []Transaction
	var userSessCountList []UserSessionCount
	var dbSessCountList []DBSessionCount
	var clientSessCountList []ClientSessionCount

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(4)

	g.Go(func() error {
		var err error
		actSessList, err = e.getActSess(ctx)
		return err
	})

	g.Go(func() error {
		var err error
		txnList, err = e.getTxn(ctx)
		return err
	})

	g.Go(func() error {
		var err error
		userSessCountList, err = e.getUserSessCount(ctx)
		return err
	})

	g.Go(func() error {
		var err error
		dbSessCountList, err = e.getDBSessCount(ctx)
		return err
	})

	g.Go(func() error {
		var err error
		clientSessCountList, err = e.getClientSessCount(ctx)
		return err
	})

	if err := g.Wait(); err != nil {
		sum.Msg = err.Error()
		slog.Errorf("[%s] 获取快照数据报错: %s\n", e.Name, sum.Msg)
	}

	//统计数据
	sum.TxnCount = len(txnList)
	sum.ActSessCount = len(actSessList)

	//计算总连接数
	sum.SessCount = func() int {
		cnt := 0
		for _, v := range userSessCountList {
			cnt += v.Count
		}
		return cnt
	}()

	//计算最长查询的执行时间
	sum.MaxQuerySeconds = func() int {
		if len(actSessList) == 0 {
			return 0
		}
		head := actSessList[0]
		return int(head.ExecSec)
	}()

	//计算长查询个数（超过10秒为大查询）
	sum.LongQueryCount = func() int {
		cnt := 0
		for _, v := range actSessList {
			if v.ExecSec > 10 {
				cnt += 1
			}
		}
		return cnt
	}()

	//计算最长事务的执行时间
	sum.MaxTxnSeconds = func() int {
		if len(txnList) == 0 {
			return 0
		}
		head := txnList[0]
		return int(head.TxnExecSec)
	}()

	//计算长事务个数（超过10秒为长事务）
	sum.LongTxnCount = func() int {
		cnt := 0
		for _, v := range txnList {
			if v.TxnExecSec > 10 {
				cnt += 1
			}
		}
		return cnt
	}()

	//表锁数
	tableLock := func() int {
		cnt := 0
		for _, v := range actSessList {
			if v.State != nil && strings.Contains(*v.State, "Waiting for ") {
				cnt += 1
			}
		}
		return cnt
	}()

	//行锁数
	rowLock := func() int {
		cnt := 0
		for _, v := range txnList {
			if v.TxnState == "LOCK WAIT" {
				cnt += 1
			}
		}
		return cnt
	}()

	// 计算被阻塞的会话数
	sum.BlockedSessCount = tableLock + rowLock

	// 保存json文件
	snapshotData := SnapshotData{
		Base:                model.Base{InstID: e.InstID, InstName: e.Name, CreateTime: e.CreateTime},
		Summary:             sum,
		ActiveSessions:      actSessList,
		Transactions:        txnList,
		UserSessionCounts:   userSessCountList,
		DBSessionCounts:     dbSessCountList,
		ClientSessionCounts: clientSessCountList,
	}

	boltStore := storage.NewBoltStore()
	err := boltStore.SaveSnapshot(strconv.Itoa(e.InstID), snapshotID, snapshotData)
	if err != nil {
		slog.Errorf("[%s] 保存快照到BoltDB失败: %v", e.Name, err)
	} else {
		slog.Infof("[%s] 保存快照到BoltDB成功: %s", e.Name, snapshotID)
	}

	sum.DurationSeconds = int(math.Round(time.Since(now).Seconds()))
	//保存快照汇总数据
	ctxCreate, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	err = db.WithContext(ctxCreate).Create(&sum).Error
	if err != nil {
		slog.Errorf("[%s] 保存快照汇总数据失败: %v", e.Name, err)
	}

	slog.Infof("[%s] 保存快照汇总数据成功 %+v", e.Name, *sum)

}

func (e *Engine) Run(db *gorm.DB) {
	t := time.Now()
	slog.Infof("[%s] 开始快照", e.Name)
	defer func() {
		slog.Infof("[%s] 快照完成，耗时%ds", e.Name, int(time.Since(t).Seconds()))
	}()
	defer func() {
		if r := recover(); r != nil {
			slog.Errorf("[%s] 运行失败: %v", e.Name, r)
		}
	}()

	err := e.Init()
	if err != nil {
		slog.Errorf("[%s] 初始化失败: %v", e.Name, err)
		return
	}

	defer func() {
		e.Close()
	}()
	e.Capture(db)
}
