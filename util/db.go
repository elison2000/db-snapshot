package util

import (
	"context"
	"database/sql"
	"db-snapshot/model"
	"fmt"
	"time"
)

func PingDB(dbType string, cfg *model.DBConfig) (err error) {
	var db *sql.DB
	switch dbType {
	case "mysql", "polar", "tdsqlc":
		db, err = NewMysqlDB(cfg)
	case "oracle":
		db, err = NewOracleDB(cfg)
	case "pgsql":
		db, err = NewPgsqlDB(cfg)
	case "oceanbase":
		db, err = NewOceanbaseDB(cfg)
	default:
		err = fmt.Errorf("不支持该数据库类型: %s", dbType)
		return
	}

	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	err = db.PingContext(ctx)
	if err != nil {
		return err
	}
	return
}

func QueryReturnList(db *sql.DB, sqlText string) (rows [][]string, err error) {
	//执行sql，返回二维数组

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	var cur *sql.Rows
	cur, err = db.QueryContext(ctx, sqlText)
	if err != nil {
		return nil, fmt.Errorf("error: %s sql:\n %s", err, sqlText)
	}
	defer cur.Close()

	var cols []string
	cols, err = cur.Columns()
	if err != nil {
		return
	}

	values := make([]sql.RawBytes, len(cols))
	valuesP := make([]interface{}, len(cols))
	for i := range values {
		valuesP[i] = &values[i]
	}

	for cur.Next() {
		err = cur.Scan(valuesP...)
		if err != nil {
			return
		}
		row := make([]string, len(cols)) //不能在循环外层定义，否则是浅拷贝
		for i, v := range values {
			if v == nil {
				row[i] = "NULL"
			} else {
				row[i] = string(v)
			}
		}

		rows = append(rows, row)
	}

	if err = cur.Err(); err != nil {
		return
	}

	return
}
