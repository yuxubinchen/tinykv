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
	DBPath string // Directory to store the data in. Should exist and be writable.
	raft   bool
	en     *engine_util.Engines
	batch  *engine_util.WriteBatch
	//badgeread DBIterator
}
type Standread struct {
	txn      *badger.Txn
	db       *badger.DB
	iterator *engine_util.BadgerIterator
}
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		DBPath: conf.DBPath,
		raft:   conf.Raft,
		en:     nil,
		batch:  new(engine_util.WriteBatch),
		//badgeread: engine_util.DBIterator
	}
}
func (s *StandAloneStorage) Start() error {
	s.en.Kv = engine_util.CreateDB(s.DBPath, s.raft)
	return nil
}
func (s *StandAloneStorage) Stop() error {
	return s.en.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {

	if ctx == nil {
		return nil, nil
	}
	var sr Standread
	sr.txn = s.en.Kv.NewTransaction(false)
	sr.db = s.en.Kv
	defer sr.txn.Discard()
	return &sr, nil
}
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {

	for _, pt := range batch {
		switch pt.Data.(type) {
		case storage.Put:
			s.batch.SetCF(engine_util.CfDefault, pt.Key(), pt.Value())
		case storage.Delete:
			s.batch.DeleteCF(engine_util.CfDefault, pt.Key())
		}
	}
	err := s.batch.WriteToDB(s.en.Kv)
	return err
}
func (s *Standread) GetCF(cf string, key []byte) ([]byte, error) {
	if key == nil {
		return nil, nil
	}
	return engine_util.GetCF(s.db, engine_util.CfDefault, key)
}
func (s *Standread) IterCF(cf string) engine_util.DBIterator {
	s.iterator = engine_util.NewCFIterator(engine_util.CfDefault, s.txn)
	return s.iterator
}
func (s *Standread) Close() {
	defer s.txn.Discard()
	s.iterator.Close()
	return
}
