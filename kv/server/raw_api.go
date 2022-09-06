package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"

	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pkg/errors"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	result := &kvrpcpb.RawGetResponse{
		Value: nil,
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, errors.Wrap(err, "Reader err")
	}
	defer reader.Close()
	data, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, errors.Wrap(err, "GetCF err")
	}
	result.Value = data
	if data == nil {
		result.NotFound = true
	}

	return result, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	result := &kvrpcpb.RawPutResponse{}
	mArr := []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	}
	if err := server.storage.Write(req.Context, mArr); err != nil {
		return result, errors.Wrap(err, "Write err")
	}

	return result, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	result := &kvrpcpb.RawDeleteResponse{}
	mArr := []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	}
	if err := server.storage.Write(req.Context, mArr); err != nil {
		return result, errors.Wrap(err, "Write err")
	}

	return result, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	result := &kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, errors.Wrap(err, "Reader err")
	}
	defer reader.Close()
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	limit := req.Limit
	pairs := make([]*kvrpcpb.KvPair, 0, limit)
	var count uint32
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		val := iter.Item()
		curPair := &kvrpcpb.KvPair{
			Key:   make([]byte, 0),
			Value: make([]byte, 0),
		}
		curPair.Key = val.KeyCopy(nil)
		curPair.Value, err = val.ValueCopy(nil)
		if err != nil {
			return result, errors.Wrap(err, "ValueCopy err")
		}
		pairs = append(pairs, curPair)
		count += 1
		if count >= limit {
			break
		}
	}
	result.Kvs = pairs

	return result, nil
}
