package util

import (
	"database/sql"
	"db-snapshot/model"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

func NewOceanbaseDB(cfg *model.DBConfig) (db *sql.DB, err error) {
	//获取数据库连接
	//sessionVariables=ob_query_timeout=3600000000
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=5s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return
	}
	db.SetMaxOpenConns(64)                  //最大连接数
	db.SetMaxIdleConns(32)                  //连接池里最大空闲连接数。必须要比maxOpenConns小
	db.SetConnMaxLifetime(time.Second * 60) //最大存活保持时间
	db.SetConnMaxIdleTime(time.Second * 5)  //最大空闲保持时间
	return
}
