package main

import (
	"context"
	"db-snapshot/capture"
	"db-snapshot/config"
	"db-snapshot/http"
	"db-snapshot/model"
	"db-snapshot/threading"
	"db-snapshot/util"
	"github.com/gookit/slog"
	"gorm.io/gorm"
	"sync/atomic"
	"time"
)

/*
####################################################################################################
#  Name        :  DBSnapshot
#  Author      :  Elison
#  Date        :  2023-11-18
#  Description :  数据库快照
#  Updates     :
#      Version     When            What
#      --------    -----------     -----------------------------------------------------------------
#      v1.0        2023-11-18      重构python版本
#      v1.1        2024-12-16      增加oceanbase
#      v1.2        2025-12-13      统一所有数据库指标和看板
####################################################################################################
*/

////go:embed web/*.html web/static/*.js web/static/*.css
//var webFiles embed.FS

var Instances atomic.Value
var DB *gorm.DB

func init() {
	// 创建文本格式化器
	formatter := slog.NewTextFormatter()
	formatter.SetTemplate("[{{datetime}}] [{{level}}] [{{caller}}] {{message}}\n")
	formatter.TimeFormat = "2006-01-02T15:04:05.000"
	formatter.EnableColor = false // 禁用颜色输出

	// 应用全局格式
	slog.SetFormatter(formatter)
}

func GetInstances() {

	sql := "select inst_id,db_type,host,port,db_name from db_snapshot_config"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var rows []model.Instance
	err := DB.WithContext(ctx).Raw(sql).Find(&rows).Error
	if err != nil {
		slog.Errorf("获取实例失败: %v", err)
		return
	}
	slog.Infof("获取实例成功, %d rows", len(rows))
	if len(rows) > 0 {
		Instances.Store(rows)
		for _, v := range rows {
			slog.Debugf("载入实例: %+v", v)
		}
	}
}

func main() {
	// 进入工作目录
	util.EnterWorkDir()

	//采集间隔时间
	var interval = time.Second * time.Duration(config.Global.Interval)
	var parallel = config.Global.Parallel

	var err error
	DB, err = util.NewMysqlORM(&config.Global.DB)
	if err != nil {
		slog.Errorf("连接数据库报错: %s", err)
		return
	}

	//启动http服务
	go func() {
		http.StartService(DB, config.Global.HttpPort)
	}()

	//载入实例
	go func() {
		reloadAt := time.Now().Add(-time.Hour)
		for range config.Global.ReloadConfigChan {
			if time.Since(reloadAt) >= 5*time.Second {
				GetInstances()
				reloadAt = time.Now()
			} else {
				slog.Infof("不能频繁重载配置")
			}
		}
	}()

	//10分钟刷新一次实例
	go func() {
		for {
			config.Global.ReloadConfigChan <- struct{}{}
			time.Sleep(10 * time.Minute)
		}
	}()

	pool := threading.NewPool(parallel, 1000)
	pool.Start() //先执行Start，防止queue满导致堵塞

	go func() {
		addTasks := func() {
			val := Instances.Load()
			if val == nil {
				slog.Warn("实例配置尚未加载，跳过本次快照")
				return
			}
			curInstances := val.([]model.Instance)
			slog.Infof("开始执行%d个实例的快照任务", len(curInstances))
			for _, inst := range curInstances {
				c := capture.NewCapturer(inst)
				pool.AddTask(func() {
					c.Run(DB)
				})
			}
		}

		now := time.Now()
		next := now.Truncate(time.Minute).Add(time.Minute)
		sleep := time.Until(next)
		time.Sleep(sleep)

		ticker := time.NewTicker(interval) //计时开始时刻对齐到00秒
		defer ticker.Stop()

		addTasks() //立即执行第一次任务
		for range ticker.C {
			addTasks()
		}
	}()

	pool.Join()
	slog.Infof("程序退出成功")

}

//func PrintEmbedFiles() {
//	err := fs.WalkDir(webFiles, ".", func(path string, d fs.DirEntry, err error) error {
//		if err != nil {
//			return err
//		}
//
//		if !d.IsDir() {
//			fmt.Printf("%s\n", path)
//		}
//		return nil
//	})
//	if err != nil {
//		panic(err)
//	}
//}
