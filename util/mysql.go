package util

import (
	"database/sql"
	"db-snapshot/model"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"time"
)

func NewMysqlORM(cfg *model.DBConfig) (*gorm.DB, error) {

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=5s&loc=Local", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)

	config := &gorm.Config{
		PrepareStmt:            true,
		SkipDefaultTransaction: true,
		NamingStrategy:         schema.NamingStrategy{SingularTable: true},
	}

	db, err := gorm.Open(mysql.Open(dsn), config)
	if err != nil {
		return nil, err
	}

	return db, nil

	//sqlDB, err := db.DB()
	//if err != nil {
	//	slog.Errorf("[%s:%d] 关闭连接失败: %v", cfg.Host, cfg.Port, err)
	//	return nil
	//}
	//
	//sqlDB.SetMaxOpenConns(100)                 //最大连接数
	//sqlDB.SetMaxIdleConns(10)                  //连接池里最大空闲连接数。必须要比maxOpenConns小
	//sqlDB.SetConnMaxLifetime(time.Second * 10) //最大存活保持时间
	//sqlDB.SetConnMaxIdleTime(time.Second * 10) //最大空闲保持时间
	//
	//return &DB{InstId: instId, Host: host, Port: port, GormDB: db, SqlDB: sqlDB}
}

func NewMysqlDB(cfg *model.DBConfig) (*sql.DB, error) {
	//获取数据库连接
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=5s&loc=Local", cfg.User, cfg.Password, cfg.Host, cfg.Port, "information_schema")
	db, err := sql.Open("mysql", dsn)

	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(64)                  //最大连接数
	db.SetMaxIdleConns(32)                  //连接池里最大空闲连接数。必须要比maxOpenConns小
	db.SetConnMaxLifetime(time.Second * 60) //最大存活保持时间
	db.SetConnMaxIdleTime(time.Second * 5)  //最大空闲保持时间
	return db, nil
}
