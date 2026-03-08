package mysql

import "db-snapshot/model"

type SnapshotData struct {
	Base                model.Base           `json:"base"`
	Summary             *model.DBSnapshot    `json:"summary"`
	ActiveSessions      []ActiveSession      `json:"active_sessions"`
	Transactions        []Transaction        `json:"transactions"`
	UserSessionCounts   []UserSessionCount   `json:"user_session_counts"`
	DBSessionCounts     []DBSessionCount     `json:"db_session_counts"`
	ClientSessionCounts []ClientSessionCount `json:"client_session_counts"`
}

type ActiveSession struct {
	CreateTime string  `json:"create_time" db:"create_time"` // now()
	SessionID  int64   `json:"session_id" db:"session_id"`
	User       *string `json:"user" db:"user"`
	DB         *string `json:"db" db:"db"`
	Client     *string `json:"client" db:"client"`
	ExecSec    int64   `json:"exec_sec" db:"exec_sec"`
	Command    *string `json:"command" db:"command"`
	State      *string `json:"state" db:"state"`
	SQLText    *string `json:"sql_text" db:"sql_text"`
}

type Transaction struct {
	CreateTime string `json:"create_time" db:"create_time"`

	SessionID int64   `json:"session_id" db:"session_id"`
	User      *string `json:"user" db:"user"`
	DB        *string `json:"db" db:"db"`
	Client    *string `json:"client" db:"client"`

	Command *string `json:"command" db:"command"`
	State   *string `json:"state" db:"state"`
	ExecSec *int64  `json:"exec_sec" db:"exec_sec"`

	TxnID             string `json:"txn_id" db:"txn_id"`
	TxnState          string `json:"txn_state" db:"txn_state"`
	TxnOperationState string `json:"txn_operation_state" db:"txn_operation_state"`

	TxnStartTime string `json:"txn_start_time" db:"txn_start_time"`
	TxnExecSec   int64  `json:"txn_exec_sec" db:"txn_exec_sec"`
	TxnWaitSec   int64  `json:"txn_wait_sec" db:"txn_wait_sec"`

	TablesLocked int64 `json:"tables_locked" db:"tables_locked"`
	RowsLocked   int64 `json:"rows_locked" db:"rows_locked"`
	RowsModified int64 `json:"rows_modified" db:"rows_modified"`

	IsolationLevel string `json:"isolation_level" db:"isolation_level"`
	SQLText        string `json:"sql_text" db:"sql_text"`
}

type UserSessionCount struct {
	CreateTime string  `json:"create_time" db:"create_time"`
	User       *string `json:"user" db:"user"`
	Count      int     `json:"cnt" db:"cnt"`
}

type DBSessionCount struct {
	CreateTime string  `json:"create_time" db:"create_time"`
	DB         *string `json:"db" db:"db"`
	Count      int     `json:"cnt" db:"cnt"`
}

type ClientSessionCount struct {
	CreateTime string  `json:"create_time" db:"create_time"`
	Client     *string `json:"client" db:"client"`
	Count      int     `json:"cnt" db:"cnt"`
}
