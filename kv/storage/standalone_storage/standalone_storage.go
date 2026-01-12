package standalone_storage

import (
	"errors"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engine *engine_util.Engines
	opt *badger.Options // 启动存储引擎的配置
	db  *badger.DB      // 底层存储句柄
	// TODO: 考虑添加列族
}

// 初始化对应的配置参数
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	opt := badger.DefaultOptions
	// 根据测试文件，这两个变量值应该是一样的
	opt.Dir = conf.DBPath
	opt.ValueDir = conf.DBPath
	return &StandAloneStorage{
		opt: &opt,
	}
}

// Start 初始化并启动独立存储引擎，返回启动过程中的任何错误
func (s *StandAloneStorage) Start() error {
	if s.opt == nil {
		return errors.New("options cant be nil")
	}
	var err error
	s.db, err = badger.Open(*(s.opt))
	return err
}

// 关闭数据库
func (s *StandAloneStorage) Stop() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// 快照读
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	if s.db == nil {
		return nil, errors.New("database is not open")
	}

	return nil, nil
}

// 写操作
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	if s.db == nil {
		return errors.New("database is not open")
	}

	return nil
}
