package standalone_storage

import (
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
	badgeread DBIterator
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		DBPath:    conf.DBPath,
		raft:      conf.Raft,
		en:        nil,
		batch:     new(engine_util.WriteBatch),
		badgeread: engine_util.DBIterator
	}
}
func (s *StandAloneStorage) Start() error {
	s.en.Kv = engine_util.CreateDB(s.DBPath, s.raft)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {

	if ctx == nil {
		return nil, nil
	}

	return nil, nil
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
func (s *storage.StorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	if key==nil{
		return nil,nil
	}
	return engine_util.GetCF(s.en.Kv, engine_util.CfDefault, pt.Key())
}
func (s *storage.StorageReader) IterCF(cf string) engine_util.DBIterator {
    txn := s.en.Kv.
	defaultIter := engine_uengine_util.NewCFIterator 
	defer txn.Discard()
	return nil
}
func (s *storage.StorageReader) Close() {
   
}
