package standalone_storage

import (
	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
	config *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath
	kvPath := path.Join(dbPath, "kv")
	raftPath := path.Join(dbPath, "raft")

	// 创建磁盘目录与DB对象
	kvEngine := engine_util.CreateDB(kvPath, false)
	raftEngine := engine_util.CreateDB(raftPath, true)

	store := StandAloneStorage{
		engine: engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath),
		config: conf,
	}
	return &store
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).

	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.engine.Kv.NewTransaction(false)
	return NewStandAloneReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			return engine_util.PutCF(s.engine.Kv, put.Cf, put.Key, put.Value)
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			return engine_util.DeleteCF(s.engine.Kv, delete.Cf, delete.Key)
		}
	}
	return nil

}

type StandAloneReader struct {
	txn *badger.Txn
}

func NewStandAloneReader(txn *badger.Txn) *StandAloneReader {
	return &StandAloneReader{
		txn: txn,
	}
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	return val, err
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneReader) Close() {
	r.txn.Discard()
}
