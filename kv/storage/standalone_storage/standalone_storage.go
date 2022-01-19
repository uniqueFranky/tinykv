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
	// Your Data Here (1).
	engine engine_util.Engines
}

type StandAloneReader struct {
	*StandAloneStorage
	txn *badger.Txn
}

func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	ans, err := engine_util.GetCF(s.engine.Kv, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return ans, err
}

func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	s.txn = s.engine.Kv.NewTransaction(false)
	it := engine_util.NewCFIterator(cf, s.txn)
	return it
}

func (s *StandAloneReader) Close() {
	defer s.txn.Discard()
	s.txn.Commit()
	return
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	var newEngine engine_util.Engines
	newEngine.KvPath = conf.DBPath
	newEngine.Kv = engine_util.CreateDB(conf.DBPath, false)
	return &(StandAloneStorage{newEngine})
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	//Franky: It seems nothing needs to be done
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	//Franky: It seems nothing needs to be done
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneReader{s, nil}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			var data = modify.Data.(storage.Put)
			err := engine_util.PutCF(s.engine.Kv, data.Cf, data.Key, data.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			var data = modify.Data.(storage.Delete)
			err := engine_util.DeleteCF(s.engine.Kv, data.Cf, data.Key)
			if err != nil {
				return err
			}
		default:
			return errors.New("(Franky Set) @StandAloneStorage.Write() Unsupported Operation")
		}
	}
	return nil
}
