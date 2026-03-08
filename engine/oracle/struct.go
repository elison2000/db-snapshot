package oracle

import "db-snapshot/model"

type SnapshotData struct {
	Base                model.Base        `json:"base"`
	Summary             *model.DBSnapshot `json:"summary"`
	LongOps             []LongOps         `json:"long_ops"`
	ActiveSessions      []ActSess         `json:"active_sessions"`
	Transactions        []Txn             `json:"transactions"`
	BlockingSessions    []BlockingSession `json:"blocking_sessions"`
	UserSessionCounts   []UserSessCount   `json:"user_session_counts"`
	ClientSessionCounts []ClientSessCount `json:"client_session_counts"`
	SQLInfo             []SQLInfo         `json:"sql_info"`
}

type LongOps struct {
	CreateTime     string  `json:"create_time" db:"create_time"`
	SessionID      int64   `json:"session_id" db:"sid"`
	Serial         int64   `json:"serial" db:"serial"`
	Username       *string `json:"username" db:"username"`
	SqlID          *string `json:"sql_id" db:"sql_id"`
	TimeRemaining  int64   `json:"time_remaining" db:"time_remaining"`
	ElapsedSeconds int64   `json:"elapsed_seconds" db:"elapsed_seconds"`
	CompletedPct   int64   `json:"completed_pct" db:"completed_pct"`
	OpName         *string `json:"op_name" db:"op_name"`
	Target         *string `json:"target" db:"target"`
	TargetDesc     *string `json:"target_desc" db:"target_desc"`
	SoFar          int64   `json:"so_far" db:"so_far"`
	TotalWork      int64   `json:"total_work" db:"total_work"`
	Units          string  `json:"units" db:"units"`
	StartTime      string  `json:"start_time" db:"start_time"`
	LastUpdateTime string  `json:"last_update_time" db:"last_update_time"`
}

type ActSess struct {
	CreateTime           string  `json:"create_time" db:"create_time"`
	SessionID            int64   `json:"session_id" db:"sid"`
	Serial               int64   `json:"serial" db:"serial"`
	Username             *string `json:"username" db:"username"`
	Program              *string `json:"program" db:"program"`
	Client               *string `json:"client" db:"machine"`
	SqlID                *string `json:"sql_id" db:"sql_id"`
	PrevSqlID            *string `json:"prev_sql_id" db:"prev_sql_id"`
	ExecSec              int64   `json:"exec_sec" db:"exec_sec"`
	BlockingSession      *int64  `json:"blocking_session" db:"blocking_session"`
	FinalBlockingSession *int64  `json:"final_blocking_session" db:"final_blocking_session"`
	WaitEvent            *string `json:"wait_event" db:"event"`
	WaitClass            *string `json:"wait_class" db:"wait_class"`
	State                *string `json:"state" db:"state"`
	WaitSec              *int64  `json:"wait_sec" db:"wait_sec"`
	P1                   *string `json:"p1" db:"p1"`
	P2                   *string `json:"p2" db:"p2"`
	P3                   *string `json:"p3" db:"p3"`
}

type Txn struct {
	CreateTime      string  `json:"create_time" db:"create_time"`
	SessionID       *int64  `json:"session_id" db:"sid"`
	Username        *string `json:"username" db:"username"`
	Client          *string `json:"client" db:"machine"`
	Program         *string `json:"program" db:"program"`
	Status          *string `json:"status" db:"status"`
	CommandType     *string `json:"command_type" db:"command_type"`
	SqlID           *string `json:"sql_id" db:"sql_id"`
	PrevSqlID       *string `json:"prev_sql_id" db:"prev_sql_id"`
	WaitClass       *string `json:"wait_class" db:"wait_class"`
	WaitEvent       *string `json:"wait_event" db:"event"`
	BlockingSession *string `json:"blocking_session" db:"blocking_session"`
	ExecSec         *int64  `json:"exec_sec" db:"exec_sec"`
	TxnID           string  `json:"txn_id" db:"xid"`
	TxnState        string  `json:"txn_state" db:"txn_status"`
	TxnStartTime    string  `json:"txn_start_time" db:"txn_start_time"`
	TxnExecSec      int64   `json:"txn_exec_sec" db:"txn_exec_sec"`
	CrGet           int64   `json:"cr_get" db:"cr_get"`
	PhyIO           int64   `json:"phy_io" db:"phy_io"`
	UsedBlocks      int64   `json:"used_blocks" db:"used_blocks"`
	UndoRows        int64   `json:"undo_rows" db:"undo_rows"`
}

type BlockingSession struct {
	CreateTime           string  `json:"create_time" db:"create_time"`
	SessionID            int64   `json:"session_id" db:"sid"`
	Serial               int64   `json:"serial" db:"serial"`
	Username             *string `json:"username" db:"username"`
	Client               *string `json:"client" db:"machine"`
	Program              *string `json:"program" db:"program"`
	CommandType          *string `json:"command_type" db:"command_type"`
	SqlID                *string `json:"sql_id" db:"sql_id"`
	PrevSqlID            *string `json:"prev_sql_id" db:"prev_sql_id"`
	Status               *string `json:"status" db:"status"`
	State                *string `json:"state" db:"state"`
	WaitClass            *string `json:"wait_class" db:"wait_class"`
	WaitEvent            *string `json:"wait_event" db:"event"`
	LogonTime            *string `json:"logon_time" db:"logon_time"`
	WaitSec              *int64  `json:"wait_sec" db:"wait_sec"`
	ExecSec              *int64  `json:"exec_sec" db:"exec_sec"`
	BlockingSession      *int64  `json:"blocking_session" db:"blocking_session"`
	FinalBlockingSession *int64  `json:"final_blocking_session" db:"final_blocking_session"`
	P1                   *string `json:"p1" db:"p1"`
	P2                   *string `json:"p2" db:"p2"`
	P3                   *string `json:"p3" db:"p3"`
}

type UserSessCount struct {
	CreateTime string  `json:"create_time" db:"create_time"`
	Username   *string `json:"username" db:"username"`
	Count      int64   `json:"count" db:"cnt"`
}

type ClientSessCount struct {
	CreateTime string  `json:"create_time" db:"create_time"`
	Client     *string `json:"client" db:"client"`
	Count      int64   `json:"count" db:"cnt"`
}

type SQLInfo struct {
	SqlID          string   `json:"sql_id" db:"sql_id"`
	LastActiveTime *string  `json:"last_active_time" db:"last_active_time"`
	Executions     *int64   `json:"executions" db:"executions"`
	ExecSec        *float64 `json:"exec_sec" db:"exec_sec"`
	AvgExecSec     *float64 `json:"avg_exec_sec" db:"avg_exec_sec"`
	SqlText        *string  `json:"sql_text" db:"sql_text"`
}
