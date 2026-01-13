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
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	val, err := reader.GetCF(req.Cf, req.Key)
	if val == nil {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, nil
	}
	return &kvrpcpb.RawGetResponse{
		Error: "",
		Value: val,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Hint: Consider using Storage.Modify to store data to be modified
	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	})
	return &kvrpcpb.RawPutResponse{}, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Hint: Consider using Storage.Modify to store data to be deleted
	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		},
	})
	return &kvrpcpb.RawDeleteResponse{}, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	kvPairs := make([]*kvrpcpb.KvPair, 0)
	for i := 0; i < int(req.Limit) && iter.Valid(); i++ {
		item := iter.Item()
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		kvPairs = append(kvPairs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: val,
		})
		iter.Next()
	}
	return &kvrpcpb.RawScanResponse{
		Kvs: kvPairs,
	}, nil
}