package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	values, err := reader.GetCF(req.GetCf(), req.GetKey())
	if kv.IsErrNotFound(err) || values == nil {
		return &kvrpcpb.RawGetResponse{
			Value:    values,
			NotFound: true,
		}, nil
	}
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawGetResponse{
		Value: values,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	if err := server.storage.Write(req.Context, []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	}); err != nil {
		return nil, err
	}
	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	if err := server.storage.Write(req.Context, []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	}); err != nil {
		return nil, err
	}
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	iter := reader.IterCF(req.Cf)
	var kvs []*kvrpcpb.KvPair
	cnt := uint32(0)
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		v, _ := item.Value()
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: v,
		})
		cnt++
		if cnt == req.Limit {
			break
		}
	}

	return &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}, nil
}
