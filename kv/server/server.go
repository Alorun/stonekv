package server

import (
	"context"

	"github.com/Alorun/stonekv/kv/coprocessor"
	"github.com/Alorun/stonekv/kv/storage"
	"github.com/Alorun/stonekv/kv/storage/raft_storage"
	"github.com/Alorun/stonekv/kv/transaction/latches"
	"github.com/Alorun/stonekv/kv/transaction/mvcc"
	coppb "github.com/Alorun/stonekv/proto/pkg/coprocessor"
	"github.com/Alorun/stonekv/proto/pkg/kvrpcpb"
	"github.com/Alorun/stonekv/proto/pkg/stonekvpb"
	"github.com/pingcap/tidb/kv"
)

var _ stonekvpb.StoneKvServer = new(Server)

// Server is a StoneKV server, it 'faces outwards', sending and receiving messages from clients such as StoneSQL.
type Server struct {
	storage storage.Storage

	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements StoneKvServer).

// Raft commands (stonekv <-> stonekv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream stonekvpb.StoneKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (stonekv <-> stonekv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream stonekvpb.StoneKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	resp := &kvrpcpb.GetResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	// Having a lock and the time of locking < the time of reading
	if lock != nil && lock.Ts <= req.Version {
		resp.Error = &kvrpcpb.KeyError{
			Locked: lock.Info(req.Key),
		}
	}

	value, err := txn.GetValue(req.Key)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
	}

	if value == nil {
		resp.NotFound = true
	} else {
		resp.Value = value
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	resp := &kvrpcpb.PrewriteResponse{}

	var keys [][]byte
	for _, m := range req.Mutations {
		keys = append(keys, m.Key)
	}

	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	var keyErrors []*kvrpcpb.KeyError
	for _, m := range req.Mutations {
		write, commitTs, err := txn.MostRecentWrite(m.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}

		// Check for write conflicts.
		if write != nil && commitTs > req.StartVersion {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:   	req.StartVersion,
					ConflictTs: commitTs,
					Key: 		m.Key,
					Primary: 	req.PrimaryLock,
				}, 
			})
			continue
		}

		// Check for lock conflicts.
		lock, err := txn.GetLock(m.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if lock != nil {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Locked: lock.Info(m.Key),
			})
			continue
		}

		var kind mvcc.WriteKind
		switch m.Op {
		case kvrpcpb.Op_Put:
			kind = mvcc.WriteKindPut
			txn.PutValue(m.Key, m.Value)
		case kvrpcpb.Op_Del:
			kind = mvcc.WriteKindDelete
		default:
			continue
		}

		txn.PutLock(m.Key, &mvcc.Lock{
			Primary: 	req.PrimaryLock,
			Ts: 		req.StartVersion,
			Ttl: 		req.LockTtl,
			Kind: 		kind,
		})
	}

	if len(keyErrors) > 0 {
		resp.Errors = keyErrors
		return resp, nil
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	resp := &kvrpcpb.CommitResponse{}

	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}

		if lock != nil && lock.Ts == req.StartVersion {
			txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
				StartTS: 	req.StartVersion,
				Kind: 		lock.Kind,
			})
			txn.DeleteLock(key)
			continue
		}

		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{
					Retryable: "transaction has been rolled back",
				}
				return resp, nil
			}
			continue
		}

		if lock == nil {
			continue
		}

		resp.Error = &kvrpcpb.KeyError{
			Retryable: "lock is held by another transaction",
		}
		return resp, nil
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	resp := &kvrpcpb.ScanResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()

	var pairs []*kvrpcpb.KvPair
	for uint32(len(pairs)) < req.Limit {
		key, value, err := scanner.Next()
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		// All keys have been iterated over.
		if key == nil {
			break
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}

		// The lock time is earlier than the current transaction time.
		// Record the current error and continue scanning other keys.
		if lock != nil && lock.Ts <= req.Version {
			pairs = append(pairs, &kvrpcpb.KvPair{
				Error: 	&kvrpcpb.KeyError{
					Locked: lock.Info(key),
				},
				Key: 	key,
			})
			continue
		}

		if value == nil {
			continue
		}
		
		pairs = append(pairs, &kvrpcpb.KvPair{
			Key: 	key,
			Value: 	value,
		})
	}

	resp.Pairs = pairs
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	resp := &kvrpcpb.CheckTxnStatusResponse{}

	keys := [][]byte{req.PrimaryKey}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.LockTs)

	// Get primary key status.
	write, commitTs, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	if write != nil {
		// The final state is already in place and no changes are needed.
		if write.Kind != mvcc.WriteKindRollback {
			// It has been submitted.
			resp.CommitVersion = commitTs
		}
		resp.Action = kvrpcpb.Action_NoAction
		return resp, nil
	}

	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	if lock == nil {
		// Since there was neither a write nor a lock, it is treated as if the transaction did not occur.
		// Leaving a rollback marker.
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: 	req.LockTs,
			Kind: 		mvcc.WriteKindRollback,
		})
		resp.Action = kvrpcpb.Action_LockNotExistRollback
	} else if mvcc.PhysicalTime(req.CurrentTs) >= mvcc.PhysicalTime(lock.Ts) + lock.Ttl {
		// The lock is active, but the transaction time out.
		// Leaving a rollback marker.
		txn.DeleteValue(req.PrimaryKey)
		txn.DeleteLock(req.PrimaryKey)
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS:	req.LockTs,
			Kind: 		mvcc.WriteKindRollback,
		})
		resp.Action = kvrpcpb.Action_TTLExpireRollback
	} else {
		// The lock is active and the transaction has not time out.
		// No changes made.
		resp.LockTtl = lock.Ttl
		resp.Action = kvrpcpb.Action_NoAction
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
