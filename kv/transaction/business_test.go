package transaction

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Alorun/stonekv/kv/server"
	"github.com/Alorun/stonekv/kv/storage"
	"github.com/Alorun/stonekv/kv/transaction/mvcc"
	"github.com/Alorun/stonekv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
)

func prewriteBusinessValue(t *testing.T, srv *server.Server, key, value []byte, startTS uint64) *kvrpcpb.PrewriteResponse {
	t.Helper()
	resp, err := srv.KvPrewrite(context.Background(), &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: key, Value: value}},
		PrimaryLock:  key,
		StartVersion: startTS,
		LockTtl:      100,
	})
	require.NoError(t, err)
	return resp
}

func commitBusinessValue(t *testing.T, srv *server.Server, key []byte, startTS, commitTS uint64) *kvrpcpb.CommitResponse {
	t.Helper()
	resp, err := srv.KvCommit(context.Background(), &kvrpcpb.CommitRequest{
		Keys: []([]byte){key}, StartVersion: startTS, CommitVersion: commitTS,
	})
	require.NoError(t, err)
	return resp
}

func TestTransactionEmptyValueLifecycle(t *testing.T) {
	srv := server.NewServer(storage.NewMemStorage())
	key := []byte("empty-transaction-value")

	require.Empty(t, prewriteBusinessValue(t, srv, key, []byte{}, 10).Errors)
	require.Nil(t, commitBusinessValue(t, srv, key, 10, 20).Error)

	get, err := srv.KvGet(context.Background(), &kvrpcpb.GetRequest{Key: key, Version: 25})
	require.NoError(t, err)
	require.False(t, get.NotFound)
	require.Empty(t, get.Value)

	prewrite, err := srv.KvPrewrite(context.Background(), &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Del, Key: key}},
		PrimaryLock:  key,
		StartVersion: 30,
		LockTtl:      100,
	})
	require.NoError(t, err)
	require.Empty(t, prewrite.Errors)
	require.Nil(t, commitBusinessValue(t, srv, key, 30, 40).Error)

	historical, err := srv.KvGet(context.Background(), &kvrpcpb.GetRequest{Key: key, Version: 25})
	require.NoError(t, err)
	require.False(t, historical.NotFound)
	require.Empty(t, historical.Value)
	latest, err := srv.KvGet(context.Background(), &kvrpcpb.GetRequest{Key: key, Version: 50})
	require.NoError(t, err)
	require.True(t, latest.NotFound)
}

func TestTransactionRollbackMarkerDoesNotHideCommittedValue(t *testing.T) {
	srv := server.NewServer(storage.NewMemStorage())
	key := []byte("account/42")

	require.Empty(t, prewriteBusinessValue(t, srv, key, []byte("balance=100"), 10).Errors)
	require.Nil(t, commitBusinessValue(t, srv, key, 10, 20).Error)

	rollback, err := srv.KvBatchRollback(context.Background(), &kvrpcpb.BatchRollbackRequest{
		Keys: []([]byte){key}, StartVersion: 30,
	})
	require.NoError(t, err)
	require.Nil(t, rollback.Error)

	get, err := srv.KvGet(context.Background(), &kvrpcpb.GetRequest{Key: key, Version: 40})
	require.NoError(t, err)
	require.False(t, get.NotFound)
	require.Equal(t, []byte("balance=100"), get.Value)

	scan, err := srv.KvScan(context.Background(), &kvrpcpb.ScanRequest{
		StartKey: key, Limit: 1, Version: 40,
	})
	require.NoError(t, err)
	require.Len(t, scan.Pairs, 1)
	require.Equal(t, key, scan.Pairs[0].Key)
	require.Equal(t, []byte("balance=100"), scan.Pairs[0].Value)
}

func TestTransactionPrewriteRetryIsIdempotent(t *testing.T) {
	srv := server.NewServer(storage.NewMemStorage())
	key := []byte("order/2026")

	first := prewriteBusinessValue(t, srv, key, []byte("created"), 100)
	require.Empty(t, first.Errors)
	second := prewriteBusinessValue(t, srv, key, []byte("created"), 100)
	require.Empty(t, second.Errors)
	require.Nil(t, commitBusinessValue(t, srv, key, 100, 120).Error)

	get, err := srv.KvGet(context.Background(), &kvrpcpb.GetRequest{Key: key, Version: 130})
	require.NoError(t, err)
	require.Equal(t, []byte("created"), get.Value)
}

func TestTransactionRolledBackTimestampCannotPrewriteAgain(t *testing.T) {
	srv := server.NewServer(storage.NewMemStorage())
	key := []byte("payment/already-cancelled")

	rollback, err := srv.KvBatchRollback(context.Background(), &kvrpcpb.BatchRollbackRequest{
		Keys: [][]byte{key}, StartVersion: 50,
	})
	require.NoError(t, err)
	require.Nil(t, rollback.Error)

	prewrite := prewriteBusinessValue(t, srv, key, []byte("must-not-commit"), 50)
	require.Len(t, prewrite.Errors, 1)
	require.NotNil(t, prewrite.Errors[0].Conflict)
	require.Equal(t, uint64(50), prewrite.Errors[0].Conflict.StartTs)
	require.Equal(t, uint64(50), prewrite.Errors[0].Conflict.ConflictTs)

	get, err := srv.KvGet(context.Background(), &kvrpcpb.GetRequest{Key: key, Version: mvcc.TsMax})
	require.NoError(t, err)
	require.True(t, get.NotFound)
}

func TestTransactionConcurrentPrewriteHasSingleWinner(t *testing.T) {
	srv := server.NewServer(storage.NewMemStorage())
	key := []byte("inventory/item-7")
	type result struct {
		startTS uint64
		value   []byte
		resp    *kvrpcpb.PrewriteResponse
		err     error
	}

	requests := []struct {
		startTS uint64
		value   []byte
	}{{10, []byte("reserved-by-a")}, {11, []byte("reserved-by-b")}}
	results := make(chan result, len(requests))
	var wg sync.WaitGroup
	for _, request := range requests {
		request := request
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := srv.KvPrewrite(context.Background(), &kvrpcpb.PrewriteRequest{
				Mutations:   []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: key, Value: request.value}},
				PrimaryLock: key, StartVersion: request.startTS, LockTtl: 100,
			})
			results <- result{startTS: request.startTS, value: request.value, resp: resp, err: err}
		}()
	}
	wg.Wait()
	close(results)

	var winner *result
	losers := 0
	for got := range results {
		require.NoError(t, got.err)
		if len(got.resp.Errors) == 0 {
			copy := got
			winner = &copy
			continue
		}
		require.NotNil(t, got.resp.Errors[0].Locked)
		losers++
	}
	require.NotNil(t, winner)
	require.Equal(t, 1, losers)
	require.Nil(t, commitBusinessValue(t, srv, key, winner.startTS, 20).Error)

	get, err := srv.KvGet(context.Background(), &kvrpcpb.GetRequest{Key: key, Version: 30})
	require.NoError(t, err)
	require.Equal(t, winner.value, get.Value)
}

func TestTransactionRollbackWaitsForExistingCommandLatch(t *testing.T) {
	srv := server.NewServer(storage.NewMemStorage())
	key := []byte("wallet/customer-9")
	keys := []([]byte){key}
	srv.Latches.WaitForLatches(keys)

	done := make(chan error, 1)
	go func() {
		_, err := srv.KvBatchRollback(context.Background(), &kvrpcpb.BatchRollbackRequest{
			Keys: keys, StartVersion: 50,
		})
		done <- err
	}()

	releasedByOtherCommand := false
	select {
	case err := <-done:
		require.NoError(t, err)
		releasedByOtherCommand = true
		t.Errorf("rollback bypassed an existing latch")
	case <-time.After(75 * time.Millisecond):
	}

	if !releasedByOtherCommand {
		srv.Latches.ReleaseLatches(keys)
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("rollback did not resume after latch release")
		}
	}
}

func TestTransactionSnapshotReadKeepsHistoricalVersion(t *testing.T) {
	srv := server.NewServer(storage.NewMemStorage())
	key := []byte("profile/name")

	require.Empty(t, prewriteBusinessValue(t, srv, key, []byte("alice"), 10).Errors)
	require.Nil(t, commitBusinessValue(t, srv, key, 10, 20).Error)
	require.Empty(t, prewriteBusinessValue(t, srv, key, []byte("bob"), 30).Errors)
	require.Nil(t, commitBusinessValue(t, srv, key, 30, 40).Error)

	oldRead, err := srv.KvGet(context.Background(), &kvrpcpb.GetRequest{Key: key, Version: 25})
	require.NoError(t, err)
	require.Equal(t, []byte("alice"), oldRead.Value)
	newRead, err := srv.KvGet(context.Background(), &kvrpcpb.GetRequest{Key: key, Version: mvcc.TsMax})
	require.NoError(t, err)
	require.Equal(t, []byte("bob"), newRead.Value)
}
