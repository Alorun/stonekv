package server

import (
	"context"

	"github.com/Alorun/stonekv/kv/storage"
	"github.com/Alorun/stonekv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements StoneKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}

	resp := &kvrpcpb.RawGetResponse{}
	if val == nil {
		resp.NotFound = true
	} else {
		resp.Value = val
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	putEntry := storage.Modify {
		Data: storage.Put{
			Cf:		req.Cf,
			Key:	req.Key,
			Value: 	req.Value,
		},
	}

	err := server.storage.Write(req.Context, []storage.Modify{putEntry})
	resp := &kvrpcpb.RawPutResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	deleteEntry := storage.Modify {
		Data: storage.Delete{
			Cf:		req.Cf,
			Key:	req.Key,

		},
	}

	err := server.storage.Write(req.Context, []storage.Modify{deleteEntry})
	resp := &kvrpcpb.RawDeleteResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	iter := reader.IterCF(req.Cf)
	defer iter.Close()

	resp := &kvrpcpb.RawScanResponse{}
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		if uint32(len(resp.Kvs)) >= req.Limit {
			break
		}
		item := iter.Item()
		key := item.KeyCopy(nil)
		value, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Key: key,
			Value: value,
		})
	}

	return resp, nil
}
