package pgsql

import (
	"db-snapshot/model"
	"github.com/lib/pq"
)

type SnapshotData struct {
	Base                model.Base           `json:"base"`
	Summary             *model.DBSnapshot    `json:"summary"`
	ActiveSessions      []ActiveSession      `json:"active_sessions"`
	Transactions        []Transaction        `json:"transactions"`
	Locks               []Lock               `json:"locks"`
	UserSessionCounts   []UserSessionCount   `json:"user_session_counts"`
	AppSessionCounts    []AppSessionCount    `json:"app_session_counts"`
	ClientSessionCounts []ClientSessionCount `json:"client_session_counts"`
}

type ActiveSession struct {
	CreateTime  string  `json:"create_time" db:"create_time"`
	SessionID   int     `json:"session_id" db:"pid"`
	DB          string  `json:"db" db:"db"`
	User        string  `json:"user" db:"user"`
	AppName     string  `json:"application_name" db:"application_name"`
	BackendType string  `json:"backend_type" db:"backend_type"`
	Client      string  `json:"client" db:"client"`
	State       string  `json:"state" db:"state"`
	WaitClass   string  `json:"wait_class" db:"wait_event_type"`
	WaitEvent   string  `json:"wait_event" db:"wait_event"`
	ExecSec     float64 `json:"exec_sec" db:"duration_ses"`
	QueryStart  string  `json:"query_start" db:"query_start"`
	SQLText     string  `json:"sql_text" db:"sql_text"`
}

type Transaction struct {
	CreateTime   string  `json:"create_time" db:"create_time"`
	SessionID    int     `json:"session_id" db:"pid"`
	DB           string  `json:"db" db:"db"`
	User         string  `json:"user" db:"user"`
	AppName      string  `json:"application_name" db:"application_name"`
	BackendType  string  `json:"backend_type" db:"backend_type"`
	Client       string  `json:"client" db:"client"`
	State        string  `json:"state" db:"state"`
	WaitClass    string  `json:"wait_class" db:"wait_event_type"`
	WaitEvent    string  `json:"wait_event" db:"wait_event"`
	TxnExecSec   float64 `json:"txn_exec_sec" db:"txn_exec_time"`
	ExecSec      float64 `json:"exec_sec" db:"exec_time"`
	TxnStartTime string  `json:"txn_start_time" db:"txn_start"`
	QueryStart   string  `json:"query_start" db:"query_start"`
	SQLText      string  `json:"sql_text" db:"sql_text"`
}

type Lock struct {
	CreateTime      string         `json:"create_time" db:"create_time"`
	SessionID       int            `json:"session_id" db:"pid"`
	BlockingSession pq.Int64Array  `json:"blocking_session" db:"blocking_pid"`
	DB              string         `json:"db" db:"db"`
	AppName         string         `json:"application_name" db:"application_name"`
	StartTime       string         `json:"start_time" db:"start_time"`
	State           string         `json:"state" db:"state"`
	TxnRuntime      float64        `json:"txn_runtime" db:"txn_runtime"`
	LockCount       int            `json:"lock_count" db:"lock_count"`
	WaitLockCount   int            `json:"wait_lock_count" db:"wait_lock_count"`
	LockTypes       pq.StringArray `json:"lock_types" db:"lock_types"`
	SQLText         string         `json:"sql_text" db:"sqltext"`
}

type UserSessionCount struct {
	CreateTime string  `json:"create_time" db:"create_time"`
	DB         string  `json:"db" db:"db"`
	User       *string `json:"user" db:"user"`
	Count      int     `json:"cnt" db:"cnt"`
}

type AppSessionCount struct {
	CreateTime string  `json:"create_time" db:"create_time"`
	DB         string  `json:"db" db:"db"`
	AppName    *string `json:"application_name" db:"application_name"`
	Count      int     `json:"cnt" db:"cnt"`
}

type ClientSessionCount struct {
	CreateTime string  `json:"create_time" db:"create_time"`
	DB         string  `json:"db" db:"db"`
	Client     *string `json:"client" db:"client"`
	Count      int     `json:"cnt" db:"cnt"`
}
