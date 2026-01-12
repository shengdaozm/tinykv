package engine_util

import (
	"os"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/log"
)

// Engines keeps references to and data for the engines used by unistore.
// All engines are badger key/value databases.
// the Path fields are the filesystem path to where the data is stored.
type Engines struct {
	// Data, including data which is committed (i.e., committed across other nodes) and un-committed (i.e., only present
	// locally).
	Kv     *badger.DB
	KvPath string
	// Metadata used by Raft.
	Raft     *badger.DB
	RaftPath string
}

func NewEngines(kvEngine, raftEngine *badger.DB, kvPath, raftPath string) *Engines {
	return &Engines{
		Kv:       kvEngine,
		KvPath:   kvPath,
		Raft:     raftEngine,
		RaftPath: raftPath,
	}
}

func (en *Engines) WriteKV(wb *WriteBatch) error {
	return wb.WriteToDB(en.Kv)
}

func (en *Engines) WriteRaft(wb *WriteBatch) error {
	return wb.WriteToDB(en.Raft)
}

// Close 关闭所有数据库连接，如果任一数据库关闭失败则返回错误
func (en *Engines) Close() error {
	dbs := []*badger.DB{en.Kv, en.Raft}
	for _, db := range dbs {
		if db == nil {
			continue
		}
		if err := db.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Destroy 关闭引擎并删除所有相关的数据文件
// 返回操作过程中遇到的任何错误
func (en *Engines) Destroy() error {
	if err := en.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(en.KvPath); err != nil {
		return err
	}
	if err := os.RemoveAll(en.RaftPath); err != nil {
		return err
	}
	return nil
}

// CreateDB creates a new Badger DB on disk at path.
// CreateDB 创建并返回一个配置好的BadgerDB实例
// path: 数据库文件存储路径
// raft: 是否为Raft引擎使用，为true时会优化配置
// 返回: 初始化好的BadgerDB指针，出错时会直接终止程序
func CreateDB(path string, raft bool) *badger.DB {
	opts := badger.DefaultOptions
	if raft {
		// Do not need to write blob for raft engine because it will be deleted soon.
		opts.ValueThreshold = 0
	}
	opts.Dir = path
	opts.ValueDir = opts.Dir
	if err := os.MkdirAll(opts.Dir, os.ModePerm); err != nil {
		log.Fatal(err)
	}
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	return db
}
