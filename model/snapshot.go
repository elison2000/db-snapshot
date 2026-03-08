package model

type Base struct {
	InstID     int    `json:"inst_id"`
	InstName   string `json:"inst_name"`
	CreateTime string `json:"create_time"`
}

type DBSnapshot struct {
	InstID           int    `gorm:"column:inst_id"`
	CreateTime       string `gorm:"column:create_time"`
	TxnCount         int    `gorm:"column:txn_count"`
	ActSessCount     int    `gorm:"column:act_sess_count"`
	SessCount        int    `gorm:"column:sess_count"`
	LongQueryCount   int    `gorm:"column:long_query_count"`
	MaxQuerySeconds  int    `gorm:"column:max_query_seconds"`
	LongTxnCount     int    `gorm:"column:long_txn_count"`
	MaxTxnSeconds    int    `gorm:"column:max_txn_seconds"`
	BlockedSessCount int    `gorm:"column:blocked_sess_count"`

	DurationSeconds int    `gorm:"column:duration_seconds"`
	Msg             string `gorm:"column:msg"`
}

func (self DBSnapshot) TableName() string {
	return "db_snapshot"
}

type DBSnapshotConfig struct {
	InstID int64  `gorm:"column:inst_id" json:"InstID"` // 显式声明，前后端对齐
	DBType string `gorm:"column:db_type" json:"DBType"`
	Host   string `gorm:"column:host"    json:"Host"`
	Port   int    `gorm:"column:port"    json:"Port"`
	DBName string `gorm:"column:db_name" json:"DBName"`
}

func (DBSnapshotConfig) TableName() string {
	return "db_snapshot_config"
}
