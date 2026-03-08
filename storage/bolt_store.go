package storage

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/bbolt"
)

// BoltStore 封装 BoltDB 操作
type BoltStore struct{}

// NewBoltStore 创建一个新的 BoltStore 实例
func NewBoltStore() *BoltStore {
	return &BoltStore{}
}

// GetDBPath 根据 instID 和 snapshotID 获取数据库文件路径
// 目录结构: data/YYYYMM/inst_id/YYYYMMDD.db
// 例如: data/202602/278/20260226.db
func (s *BoltStore) GetDBPath(instID string, snapshotID string) string {
	// snapshotID 格式假定为 YYYYMMDD_HHMMSS，例如 20260226_105400
	if len(snapshotID) < 8 {
		return ""
	}
	month := snapshotID[0:6]
	date := snapshotID[0:8]

	return filepath.Join("data", month, instID, date+".db")
}

// SaveSnapshot 保存快照数据
// instID: 实例ID (用于路径)
// snapshotID: 快照ID (作为 Key)
// data: 快照数据对象
func (s *BoltStore) SaveSnapshot(instID string, snapshotID string, data interface{}) error {
	dbPath := s.GetDBPath(instID, snapshotID)
	if dbPath == "" {
		return fmt.Errorf("invalid snapshotID: %s", snapshotID)
	}

	// 确保目录存在
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create directory error: %v", err)
	}

	// 打开/创建数据库
	db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return fmt.Errorf("open db error: %v", err)
	}
	defer db.Close()

	// 序列化并压缩数据
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal json error: %v", err)
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(jsonData); err != nil {
		return fmt.Errorf("gzip write error: %v", err)
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("gzip close error: %v", err)
	}
	compressedData := buf.Bytes()

	// 写入 BoltDB
	return db.Update(func(tx *bbolt.Tx) error {
		// 创建/获取 Bucket (固定 bucket 名称 "snapshots")
		b, err := tx.CreateBucketIfNotExists([]byte("snapshots"))
		if err != nil {
			return err
		}
		// 写入数据
		return b.Put([]byte(snapshotID), compressedData)
	})
}

// GetSnapshot 读取快照数据
func (s *BoltStore) GetSnapshot(instID string, snapshotID string) ([]byte, error) {
	dbPath := s.GetDBPath(instID, snapshotID)
	if dbPath == "" {
		return nil, fmt.Errorf("invalid snapshotID: %s", snapshotID)
	}

	// 检查文件是否存在
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("snapshot db file not found: %s", dbPath)
	}

	db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{Timeout: 1 * time.Second, ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("open db error: %v", err)
	}
	defer db.Close()

	var compressedData []byte

	err = db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("snapshots"))
		if b == nil {
			return fmt.Errorf("bucket snapshots not found")
		}
		v := b.Get([]byte(snapshotID))
		if v == nil {
			return fmt.Errorf("snapshot %s not found in db", snapshotID)
		}
		// 拷贝数据
		compressedData = make([]byte, len(v))
		copy(compressedData, v)
		return nil
	})

	if err != nil {
		return nil, err
	}

	// 解压数据
	gr, err := gzip.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		return nil, fmt.Errorf("gzip reader error: %v", err)
	}
	defer gr.Close()

	return io.ReadAll(gr)
}
