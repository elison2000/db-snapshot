package pgsql

import (
	"context"
	"db-snapshot/model"
	"db-snapshot/storage"
	"db-snapshot/util"
	"fmt"
	"math"
	"strconv"
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

	db, err := util.NewPgsqlDB(&e.Cfg)
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

func (e *Engine) getActSess(ctx context.Context) ([]ActiveSession, error) {
	var rows []ActiveSession
	err := e.DB.SelectContext(ctx, &rows, ActSessSQL)
	if err != nil {
		return nil, fmt.Errorf("getActSess-> %w", err)
	}
	return rows, nil
}

func (e *Engine) getTxn(ctx context.Context) ([]Transaction, error) {
	var rows []Transaction
	err := e.DB.SelectContext(ctx, &rows, TxnSQL)
	if err != nil {
		return nil, fmt.Errorf("getTxn-> %w", err)
	}
	return rows, nil
}

func (e *Engine) getLock(ctx context.Context) ([]Lock, error) {
	var rows []Lock
	err := e.DB.SelectContext(ctx, &rows, LockSQL)
	if err != nil {
		return nil, fmt.Errorf("getLock-> %w", err)
	}
	return rows, nil
}

func (e *Engine) getUserSessCount(ctx context.Context) ([]UserSessionCount, error) {
	var rows []UserSessionCount
	err := e.DB.SelectContext(ctx, &rows, UserSessCountSQL)
	if err != nil {
		return nil, fmt.Errorf("getUserSessCount-> %w", err)
	}
	return rows, nil
}

func (e *Engine) getAPPSessCount(ctx context.Context) ([]AppSessionCount, error) {
	var rows []AppSessionCount
	err := e.DB.SelectContext(ctx, &rows, AppSessCountSQL)
	if err != nil {
		return nil, fmt.Errorf("getAPPSessCount-> %w", err)
	}
	return rows, nil
}

func (e *Engine) getClientSessCount(ctx context.Context) ([]ClientSessionCount, error) {
	var rows []ClientSessionCount
	err := e.DB.SelectContext(ctx, &rows, ClientSessCountSQL)
	if err != nil {
		return nil, fmt.Errorf("getClientSessCount-> %w", err)
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
	var lockList []Lock
	var userSessCountList []UserSessionCount
	var appSessCountList []AppSessionCount
	var clientSessCountList []ClientSessionCount

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(6)

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
		lockList, err = e.getLock(ctx)
		return err
	})

	g.Go(func() error {
		var err error
		userSessCountList, err = e.getUserSessCount(ctx)
		return err
	})

	g.Go(func() error {
		var err error
		appSessCountList, err = e.getAPPSessCount(ctx)
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
		for _, v := range actSessList {
			//客户端类型为: client backend  过滤复制会话
			if v.BackendType == "client backend" {
				return int(math.Round(v.ExecSec))
			}
		}
		return 0
	}()

	//计算长查询个数（超过10秒为大查询）
	sum.LongQueryCount = func() int {
		cnt := 0
		for _, v := range actSessList {
			if v.ExecSec > 10 && v.BackendType == "client backend" {
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
		return int(math.Round(head.TxnExecSec))
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

	// 计算被阻塞的会话数
	sum.BlockedSessCount = func() int {
		n := 0
		//等待事件类型为Lock
		for _, v := range actSessList {
			if v.WaitClass == "Lock" {
				n++
			}
		}
		return n
	}()

	// 保存json文件
	snapshotData := SnapshotData{
		Base:                model.Base{InstID: e.InstID, InstName: e.Name, CreateTime: e.CreateTime},
		Summary:             sum,
		ActiveSessions:      actSessList,
		Transactions:        txnList,
		Locks:               lockList,
		UserSessionCounts:   userSessCountList,
		AppSessionCounts:    appSessCountList,
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
