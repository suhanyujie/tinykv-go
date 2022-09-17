package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pkg/errors"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	db := engine_util.CreateDB(conf.DBPath, false)
	st := &StandAloneStorage{
		engine: db,
	}

	return st
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	// do nothing
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.engine.Close(); err != nil {
		return errors.Wrap(err, "close err")
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	trx := s.engine.NewTransaction(false)
	reader := &SAReader{
		Fd: trx,
	}

	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	trx := s.engine.NewTransaction(true)

	for _, one := range batch {
		switch one.Data.(type) {
		case storage.Put:
			putObj := one.Data.(storage.Put)
			key := engine_util.KeyWithCF(putObj.Cf, putObj.Key)
			if err := trx.Set(key, putObj.Value); err != nil {
				return errors.Wrap(err, "set err")
			}
		case storage.Delete:
			delObj := one.Data.(storage.Delete)
			key := engine_util.KeyWithCF(delObj.Cf, delObj.Key)
			if err := trx.Delete(key); err != nil {
				return errors.Wrap(err, "del err")
			}
		}
	}
	if err := trx.Commit(); err != nil {
		return errors.Wrap(err, "Commit err")
	}

	return nil
}
