package standalone_storage

import (
	"errors"

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

type StandAloneStorageReader struct {
	txn *badger.Txn // 用于读取数据的事务，由Reader方法传入
}

func (r *StandAloneStorageReader)GetCF(cf string, key []byte) ([]byte, error) {
	if r.txn ==nil {
		return nil, errors.New("transaction is nil")
	}
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err != nil {
		// 如果key不存在，返回nil
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

// CF迭代器
func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	if r.txn == nil {
		return nil
	}
	
	iter := engine_util.NewCFIterator(cf, r.txn)
	return iter
}

// Close就是把当前的事务提交
func (r *StandAloneStorageReader) Close() {
	if r.txn != nil {
		r.txn.Discard() // 只读事务必须discard
		r.txn = nil
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

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	if s.engine.Kv == nil {
		return nil, errors.New("kv is nil")
	}
	
	txn := s.engine.Kv.NewTransaction(false)
	return &StandAloneStorageReader{txn: txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	if s.engine.Kv == nil {
		return errors.New("kv is nil")
	}

	// 创建写事务,失败的情况下回滚
	txn := s.engine.Kv.NewTransaction(true)
	defer txn.Discard()

	for _, mod := range batch {
		fullkey := engine_util.KeyWithCF(mod.Cf(), mod.Key())
		switch mod.Data.(type) {
		case storage.Put:
			err := s.engine.Kv.Update(func(txn *badger.Txn) error {
				return txn.Set(fullkey, mod.Value())
			})
			if err != nil {
				return err
			}
		case storage.Delete:
			err := s.engine.Kv.Update(func(txn *badger.Txn) error {
				return txn.Delete(fullkey)
			})
			if err != nil {
				return err
			}
		}
	}
	return txn.Commit()
}
