package standalone_storage

import (
	"github.com/Alorun/stonekv/kv/config"
	"github.com/Alorun/stonekv/kv/storage"
	"github.com/Alorun/stonekv/kv/util/engine_util"
	"github.com/Alorun/stonekv/proto/pkg/kvrpcpb"
	"github.com/Connor1996/badger"
)

// It is an implementation of `Storage` for a single-node KV instance.
// It does not communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engines_ engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	s := new(StandAloneStorage)
	s.engines_.KvPath = conf.DBPath
	s.engines_.Kv = engine_util.CreateDB(conf.DBPath, false)
	return s
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.engines_.Kv.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.engines_.Kv.NewTransaction(false)
	return &storageReader{txn_: txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	wb := new(engine_util.WriteBatch)
	for _, entry := range batch {
		switch entry.Data.(type) {
		case storage.Put: 
			wb.SetCF(entry.Cf(), entry.Key(), entry.Value())
		case storage.Delete:
			wb.DeleteCF(entry.Cf(),entry.Key())
		}
	}

	return wb.WriteToDB(s.engines_.Kv)
}

type storageReader struct {
	txn_ *badger.Txn
}

func (r *storageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn_, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, nil
}

func (r *storageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn_)
}

func (r *storageReader) Close() {
	r.txn_.Discard()
}
