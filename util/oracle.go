package util

import (
	"database/sql"
	"db-snapshot/model"
	"fmt"
	_ "github.com/sijms/go-ora/v2"
	"time"
)

func NewOracleDB(cfg *model.DBConfig) (db *sql.DB, err error) {
	//获取数据库连接
	// prefetch_rows设置太大会报错：driver: bad connection
	dsn := fmt.Sprintf("oracle://%s:%s@%s:%d/%s?prefetch_rows=10000", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	db, err = sql.Open("oracle", dsn)
	if err != nil {
		return
	}

	db.SetMaxOpenConns(64)                  //最大连接数
	db.SetMaxIdleConns(32)                  //连接池里最大空闲连接数。必须要比maxOpenConns小
	db.SetConnMaxLifetime(time.Second * 60) //最大存活保持时间
	db.SetConnMaxIdleTime(time.Second * 5)  //最大空闲保持时间

	return
}
