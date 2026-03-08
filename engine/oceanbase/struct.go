package oceanbase

import "db-snapshot/model"

type SnapshotData struct {
	Base           model.Base        `json:"base"`
	Summary        *model.DBSnapshot `json:"summary"`
	ActiveSessions []ActiveSession   `json:"active_sessions"`
	Transactions   []Transaction     `json:"transactions"`
	Locks          []Lock            `json:"locks"`
	LockObjects    []LockObj         `json:"lock_objects"`
	SessionCounts  []SessionCount    `json:"session_counts"`
}

type ActiveSession struct {
	CreateTime string  `json:"create_time" db:"create_time"`
	SvrIP      string  `json:"svr_ip" db:"svr_ip"`
	SessionID  int64   `json:"session_id" db:"session_id"`
	User       *string `json:"user" db:"user"`
	DB         *string `json:"db" db:"db"`
	Client     *string `json:"client" db:"client"`
	Tenant     string  `json:"tenant" db:"tenant"`
	ExecSec    float64 `json:"exec_sec" db:"exec_sec"`
	Command    string  `json:"command" db:"command"`
	State      string  `json:"state" db:"state"`
	TxnID      int64   `json:"txn_id" db:"txn_id"`
	SQLText    *string `json:"sql_text" db:"sql_text"`
}

type Transaction struct {
	CreateTime   string  `json:"create_time" db:"create_time"`
	SvrIP        string  `json:"svr_ip" db:"svr_ip"`
	SessionID    int64   `json:"session_id" db:"session_id"`
	User         *string `json:"user" db:"user"`
	DB           *string `json:"db" db:"db"`
	Client       *string `json:"client" db:"client"`
	Tenant       string  `json:"tenant" db:"tenant"`
	ExecSec      float64 `json:"exec_sec" db:"exec_sec"`
	TxnStartTime string  `json:"txn_start_time" db:"txn_start_time"`
	TxnExecSec   int64   `json:"txn_exec_sec" db:"txn_exec_sec"`
	Command      string  `json:"command" db:"command"`
	State        string  `json:"state" db:"state"`
	TxnID        int64   `json:"txn_id" db:"txn_id"`
	SQLText      *string `json:"sql_text" db:"sql_text"`
}

type Lock struct {
	BlockingSessID     int64  `json:"blocking_sess_id" db:"blocking_sess_id"`
	BlockingTxnID      int64  `json:"blocking_txn_id" db:"blocking_txn_id"`
	BlockingCreateTime string `json:"blocking_create_time" db:"blocking_create_time"`
	BlockingExecSec    int64  `json:"blocking_exec_sec" db:"blocking_exec_sec"`
	BlockingLastReqAt  string `json:"blocking_last_req_at" db:"blocking_last_req_at"`

	BlockedSessID     int64  `json:"blocked_sess_id" db:"blocked_sess_id"`
	BlockedTxnID      int64  `json:"blocked_txn_id" db:"blocked_txn_id"`
	BlockedCreateTime string `json:"blocked_create_time" db:"blocked_create_time"`
	BlockedExecSec    int64  `json:"blocked_exec_sec" db:"blocked_exec_sec"`
	BlockedLastReqAt  string `json:"blocked_last_req_at" db:"blocked_last_req_at"`
}

type LockObj struct {
	SvrIP    string `json:"svr_ip" db:"svr_ip"`
	SvrPort  int64  `json:"svr_port" db:"svr_port"`
	TenantID int64  `json:"tenant_id" db:"tenant_id"`
	TxnID    int64  `json:"txn_id" db:"txn_id"`
	ID1      string `json:"id1" db:"id1"`
	ID2      string `json:"id2" db:"id2"`
	Type     string `json:"type" db:"type"`
	LockMode string `json:"lock_mode" db:"lock_mode"`
	Block    int    `json:"block" db:"block"`
	LockSec  int64  `json:"lock_sec" db:"lock_sec"`
}

type SessionCount struct {
	CreateTime string  `json:"create_time" db:"create_time"`
	User       *string `json:"user" db:"user"`
	DB         *string `json:"db" db:"db"`
	Count      int     `json:"cnt" db:"cnt"`
}
