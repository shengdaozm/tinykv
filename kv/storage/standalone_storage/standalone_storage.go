package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engine *engine_util.Engines // kv、raft的句柄
	conf   *config.Config       // kv存储路径
}

// StandAloneReader is the reader implementation for StandAloneStorage
type StandAloneReader struct {
	txn *badger.Txn
	kvdb *badger.DB
}
func (r *StandAloneReader)GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCF(r.kvdb,cf,key)
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	
	return nil
}

// Close就是把当前的事务提交
func (r *StandAloneReader) Close() {
	if r.txn != nil {
		r.txn.Commit()
	}
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// TODO: 这个地方感觉new一个很奇怪，实际上句柄是在Start()函数里面拿到的
	return &StandAloneStorage{
		conf: conf,
	}
}

// Start 初始化并启动独立存储引擎，返回启动过程中的任何错误
func (s *StandAloneStorage) Start() error {
	s.engine = engine_util.NewEngines(
		engine_util.CreateDB(s.conf.DBPath, s.conf.Raft),
		nil,
		s.conf.DBPath,
		"",
	)
	return nil
}

// 关闭数据库
func (s *StandAloneStorage) Stop() error {
	return s.engine.Close()
}

// 快照读
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {

	return nil, nil
}

// 写操作
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {

	return nil
}
