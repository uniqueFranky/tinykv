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
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	val, err := reader.GetCF(req.Cf, req.Key)
	res := kvrpcpb.RawGetResponse{
		RegionError:          nil,
		Error:                "",
		Value:                val,
		NotFound:             val == nil,
		XXX_NoUnkeyedLiteral: req.XXX_NoUnkeyedLiteral,
		XXX_unrecognized:     req.XXX_unrecognized,
		XXX_sizecache:        req.XXX_sizecache,
	}
	return &res, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	slc := make([]storage.Modify, 0, 1)
	err := server.storage.Write(nil, append(slc, storage.Modify{put}))
	return nil, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	myDelete := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	slc := make([]storage.Modify, 0, 1)
	err := server.storage.Write(nil, append(slc, storage.Modify{myDelete}))
	return nil, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	it := reader.IterCF(req.Cf)
	defer reader.Close()
	defer it.Close()
	it.Seek(req.StartKey)
	slc := make([]*kvrpcpb.KvPair, 0, req.Limit)
	var sat uint32 = 0
	for {
		if it.Valid() == false {
			break
		}
		var tmp []byte
		val, err := it.Item().ValueCopy(tmp)
		if err != nil {
			return nil, err
		}
		slc = append(slc, &kvrpcpb.KvPair{
			Error:                nil,
			Key:                  it.Item().KeyCopy(tmp),
			Value:                val,
			XXX_NoUnkeyedLiteral: req.XXX_NoUnkeyedLiteral,
			XXX_unrecognized:     req.XXX_unrecognized,
			XXX_sizecache:        req.XXX_sizecache,
		})
		sat++
		if sat >= req.Limit {
			break
		}
		it.Next()
	}
	res := kvrpcpb.RawScanResponse{
		RegionError:          nil,
		Error:                "",
		Kvs:                  slc,
		XXX_NoUnkeyedLiteral: req.XXX_NoUnkeyedLiteral,
		XXX_unrecognized:     req.XXX_unrecognized,
		XXX_sizecache:        req.XXX_sizecache,
	}
	return &res, nil
}
