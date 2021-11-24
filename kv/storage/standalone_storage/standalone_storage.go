package standalone_storage

import (
	"path/filepath"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	en     *engine_util.Engines
	config *config.Config
	//badgeread DBIterator
}
type Standread struct {
	txn      *badger.Txn
	db       *badger.DB
	iterator *engine_util.BadgerIterator
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	//t := new(StandAloneStorage)
	kvpath := filepath.Join(conf.DBPath, "kv")
	raftpath := filepath.Join(conf.DBPath, "raft")
	raftdb := engine_util.CreateDB(raftpath, true)
	kvdb := engine_util.CreateDB(kvpath, false)
	eng := engine_util.NewEngines(kvdb, raftdb, kvpath, raftpath)
	//config = conf
	return &StandAloneStorage{
		en:     eng,
		config: conf,
	}
}
func (s *StandAloneStorage) Start() error {

	return nil
}
func (s *StandAloneStorage) Stop() error {
	return s.en.Close()
}
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {

	txn := s.en.Kv.NewTransaction(false)
	//sr.db := s.en.Kv
	return &Standread{
		txn: txn,
	}, nil
}
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	bat := new(engine_util.WriteBatch)
	for _, pt := range batch {
		switch pt.Data.(type) {
		case storage.Put:
			bat.SetCF(pt.Cf(), pt.Key(), pt.Value())
		case storage.Delete:
			bat.DeleteCF(pt.Cf(), pt.Key())
		}
	}
	err := bat.WriteToDB(s.en.Kv)
	return err
}
func (s *Standread) GetCF(cf string, key []byte) ([]byte, error) {
	a, b := engine_util.GetCFFromTxn(s.txn, cf, key)
	if b != nil {
		a = nil
		b = nil
	}
	return a, b
}
func (s *Standread) IterCF(cf string) engine_util.DBIterator {
	s.iterator = engine_util.NewCFIterator(cf, s.txn)
	return s.iterator
}
func (s *Standread) Close() {
	defer s.txn.Discard()
	s.iterator.Close()
	return
}
