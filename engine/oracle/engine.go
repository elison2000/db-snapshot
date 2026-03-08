package oracle

import (
	"bytes"
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

	db, err := util.NewOracleDB(&e.Cfg)
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

func (e *Engine) getLongOps(ctx context.Context) ([]LongOps, error) {
	t := time.Now()
	defer func() {
		slog.Infof("[%s] getLongOps 耗时 %d s", e.Name, int(time.Since(t).Seconds()))
	}()
	var rows []LongOps
	err := e.DB.SelectContext(ctx, &rows, LongOpsSQL)
	if err != nil {
		return nil, fmt.Errorf("getLongOps-> %w", err)
	}

	return rows, nil
}

func (e *Engine) getActSess(ctx context.Context) ([]ActSess, error) {
	t := time.Now()
	defer func() {
		slog.Infof("[%s] getActSess 耗时 %d s", e.Name, int(time.Since(t).Seconds()))
	}()
	var rows []ActSess
	err := e.DB.SelectContext(ctx, &rows, ActSessSQL)
	if err != nil {
		return nil, fmt.Errorf("getActSess-> %w", err)
	}
	return rows, nil
}

func (e *Engine) getTxn(ctx context.Context) ([]Txn, error) {
	t := time.Now()
	defer func() {
		slog.Infof("[%s] getTxn 耗时 %d s", e.Name, int(time.Since(t).Seconds()))
	}()
	var rows []Txn
	err := e.DB.SelectContext(ctx, &rows, TxnSQL)
	if err != nil {
		return nil, fmt.Errorf("getTxn-> %w", err)
	}
	return rows, nil
}

func (e *Engine) getBlocker(ctx context.Context) ([]BlockingSession, error) {
	t := time.Now()
	defer func() {
		slog.Infof("[%s] getBlocker 耗时 %d s", e.Name, int(time.Since(t).Seconds()))
	}()
	var rows []BlockingSession
	err := e.DB.SelectContext(ctx, &rows, BlockerSQL)
	if err != nil {
		return nil, fmt.Errorf("getBlocker-> %w", err)
	}
	return rows, nil
}

//func (e *Engine) getLockObj() ([][]string, error) {
//  t := time.Now()
//  defer func() {
//      slog.Infof("[%s:%d] getLockObj 耗时 %d s", e.Host, e.Port, int(time.Since(t).Seconds()))
//  }()
//  rows, err := util.QueryReturnList(e.DB, LockObjSQL)
//  if err != nil {
//      return nil, fmt.Errorf("getLockObj-> %w", err)
//  }
//  return rows, nil
//}

func (e *Engine) getUserSessCount(ctx context.Context) ([]UserSessCount, error) {
	t := time.Now()
	defer func() {
		slog.Infof("[%s] getUserSessCount 耗时 %d s", e.Name, int(time.Since(t).Seconds()))
	}()
	var rows []UserSessCount
	err := e.DB.SelectContext(ctx, &rows, UserSessCountSQL)
	if err != nil {
		return nil, fmt.Errorf("getUserSessCount-> %w", err)
	}
	return rows, nil
}

func (e *Engine) getClientSessCount(ctx context.Context) ([]ClientSessCount, error) {
	t := time.Now()
	defer func() {
		slog.Infof("[%s] getClientSessCount 耗时 %d s", e.Name, int(time.Since(t).Seconds()))
	}()
	var rows []ClientSessCount
	err := e.DB.SelectContext(ctx, &rows, ClientSessCountSQL)
	if err != nil {
		return nil, fmt.Errorf("getClientSessCount-> %w", err)
	}
	return rows, nil
}

func (e *Engine) getSQLInfo(ctx context.Context, sqlIds []string) ([]SQLInfo, error) {
	t := time.Now()
	defer func() {
		slog.Infof("[%s] getSQLInfo 耗时 %d s", e.Name, int(time.Since(t).Seconds()))
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
	query := fmt.Sprintf(SQLInfoSQL, buf.String())
	var rows []SQLInfo
	err := e.DB.SelectContext(ctx, &rows, query)
	if err != nil {
		return nil, fmt.Errorf("getSQLInfo-> %w", err)
	}
	return rows, nil

}

func (e *Engine) Capture(db *gorm.DB) {
	now := time.Now()
	e.CreateTime = now.Format("2006-01-02 15:04:05")
	snapshotID := now.Format("20060102_150405")

	sum := &model.DBSnapshot{InstID: e.InstID, CreateTime: e.CreateTime}

	//收集快照数据
	var longOpsList []LongOps
	var actSessList []ActSess
	var txnList []Txn
	var blockingSessionList []BlockingSession
	var userSessCountList []UserSessCount
	var clientSessCountList []ClientSessCount
	var sqlInfoList []SQLInfo

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(6)

	g.Go(func() error {
		var err error
		longOpsList, err = e.getLongOps(ctx)
		return err
	})

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
		blockingSessionList, err = e.getBlocker(ctx)
		return err
	})

	g.Go(func() error {
		var err error
		userSessCountList, err = e.getUserSessCount(ctx)
		return err
	})

	g.Go(func() error {
		var err error
		clientSessCountList, err = e.getClientSessCount(ctx)
		return err
	})

	if err := g.Wait(); err != nil {
		sum.Msg += fmt.Sprintf("%v\n", err)
		slog.Errorf("[%s] 获取快照数据报错: %s\n", e.Name, err.Error())
	}

	sum.ActSessCount = len(actSessList)
	sum.TxnCount = len(txnList)

	sum.SessCount = func() int {
		cnt := 0
		for _, v := range userSessCountList {
			cnt += int(v.Count)
		}
		return cnt
	}()

	//计算最长查询的执行时间
	sum.MaxQuerySeconds = func() int {
		if len(actSessList) == 0 {
			return 0
		}

		return int(actSessList[0].ExecSec)
	}()

	sum.LongQueryCount = len(longOpsList)

	//计算最长事务的执行时间
	sum.MaxTxnSeconds = func() int {
		if len(txnList) == 0 {
			return 0
		}
		return int(txnList[0].TxnExecSec)
	}()

	//计算长事务个数（超过10秒为大查询）
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
		cnt := 0
		for _, v := range actSessList {
			if v.BlockingSession != nil { //阻塞者不为 NULL就是被阻塞
				cnt += 1
			}
		}
		return cnt
	}()

	//获取sqlid列表
	sqlIdMap := make(map[string]struct{})

	for _, row := range longOpsList {
		if row.SqlID != nil {
			sqlIdMap[*row.SqlID] = struct{}{}
		}
	}

	for _, row := range actSessList {
		if row.SqlID != nil {
			sqlIdMap[*row.SqlID] = struct{}{}
		}
		if row.PrevSqlID != nil {
			sqlIdMap[*row.PrevSqlID] = struct{}{}
		}
	}

	for _, row := range txnList {
		if row.SqlID != nil {
			sqlIdMap[*row.SqlID] = struct{}{}
		}
		if row.PrevSqlID != nil {
			sqlIdMap[*row.PrevSqlID] = struct{}{}
		}
	}

	for _, row := range blockingSessionList {
		if row.SqlID != nil {
			sqlIdMap[*row.SqlID] = struct{}{}
		}
		if row.PrevSqlID != nil {
			sqlIdMap[*row.PrevSqlID] = struct{}{}
		}
	}

	var sqlIds []string
	for k := range sqlIdMap {
		sqlIds = append(sqlIds, k)
	}
	//slog.Debugf("[%s:%d]  sqlIds: %v", e.Host, e.Port, sqlIds)

	//获取sql数据
	if len(sqlIds) > 0 {
		err := func() error {
			var err error
			sqlInfoList, err = e.getSQLInfo(ctx, sqlIds)
			return err
		}()
		if err != nil {
			sum.Msg += fmt.Sprintf("%v\n", err)
			slog.Errorf("[%s] 获取快照数据报错: %s\n", e.Name, err.Error())
		}
	}

	// 保存json文件
	snapshotData := SnapshotData{
		Base:                model.Base{InstID: e.InstID, InstName: e.Name, CreateTime: e.CreateTime},
		Summary:             sum,
		LongOps:             longOpsList,
		ActiveSessions:      actSessList,
		Transactions:        txnList,
		BlockingSessions:    blockingSessionList,
		UserSessionCounts:   userSessCountList,
		ClientSessionCounts: clientSessCountList,
		SQLInfo:             sqlInfoList,
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
