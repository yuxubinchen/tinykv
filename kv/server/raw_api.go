package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	wer, _ := server.storage.Reader(nil)
	t1, _ := wer.GetCF(req.Cf, req.Key)
	var notFound bool
	if t1 == nil {
		notFound = true
	} else {
		notFound = false
	}
	return &kvrpcpb.RawGetResponse{
		Value:    t1,
		NotFound: notFound,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {

	t1 := make([]storage.Modify, 1)
	t1[0].Data = storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	t := new(kvrpcpb.RawPutResponse)
	err := server.storage.Write(nil, t1)
	if err != nil {
		t.Error = err.Error()
	}
	return t, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	t1 := make([]storage.Modify, 1)
	t1[0].Data = storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	t := new(kvrpcpb.RawDeleteResponse)
	err := server.storage.Write(nil, t1)
	if err != nil {
		t.Error = err.Error()
	}
	return t, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	wer, err := server.storage.Reader(nil)
	t := new(kvrpcpb.RawScanResponse)
	s := new(kvrpcpb.KvPair)
	t1 := wer.IterCF(req.Cf)
	t1.Seek(req.StartKey)
	for i := 0; i < int(req.Limit); i++ {
		if t1.Valid() != true {
			break
		}
		Value, _ := t1.Item().Value()
		s.Value = Value
		s.Key = t1.Item().Key()
		t.Kvs = append(t.Kvs, &kvrpcpb.KvPair{
			Key:   s.Key,
			Value: s.Value,
		})
		t1.Next()
	}
	return t, err
}
