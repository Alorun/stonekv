package server

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Alorun/stonekv/kv/config"
	"github.com/Alorun/stonekv/kv/storage/standalone_storage"
	"github.com/Alorun/stonekv/kv/util/engine_util"
	"github.com/Alorun/stonekv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
)

func newRawBusinessStorage(t *testing.T) (*config.Config, *standalone_storage.StandAloneStorage, *Server) {
	t.Helper()
	conf := config.NewTestConfig()
	conf.DBPath = filepath.Join(t.TempDir(), "raw-kv")
	store := standalone_storage.NewStandAloneStorage(conf)
	require.NoError(t, store.Start())
	return conf, store, NewServer(store)
}

func rawBusinessPut(t *testing.T, server *Server, cf string, key, value []byte) {
	t.Helper()
	resp, err := server.RawPut(context.Background(), &kvrpcpb.RawPutRequest{
		Cf: cf, Key: key, Value: value,
	})
	require.NoError(t, err)
	require.Empty(t, resp.Error)
}

func rawBusinessGet(t *testing.T, server *Server, cf string, key []byte) *kvrpcpb.RawGetResponse {
	t.Helper()
	resp, err := server.RawGet(context.Background(), &kvrpcpb.RawGetRequest{Cf: cf, Key: key})
	require.NoError(t, err)
	return resp
}

func TestRawKVEmptyValueSurvivesRestart(t *testing.T) {
	conf, store, server := newRawBusinessStorage(t)
	key := []byte("empty-value")

	rawBusinessPut(t, server, engine_util.CfDefault, key, []byte{})
	before := rawBusinessGet(t, server, engine_util.CfDefault, key)
	require.False(t, before.NotFound)
	require.Empty(t, before.Value)

	require.NoError(t, store.Stop())
	store = standalone_storage.NewStandAloneStorage(conf)
	require.NoError(t, store.Start())
	defer store.Stop()

	after := rawBusinessGet(t, NewServer(store), engine_util.CfDefault, key)
	require.False(t, after.NotFound)
	require.Empty(t, after.Value)
}

func TestRawKVColumnFamiliesRemainIndependentAfterRestart(t *testing.T) {
	conf, store, server := newRawBusinessStorage(t)
	key := []byte("shared-key")

	rawBusinessPut(t, server, engine_util.CfDefault, key, []byte("default-value"))
	rawBusinessPut(t, server, engine_util.CfLock, key, []byte("lock-value"))
	rawBusinessPut(t, server, engine_util.CfWrite, key, []byte("write-value"))
	_, err := server.RawDelete(context.Background(), &kvrpcpb.RawDeleteRequest{Cf: engine_util.CfLock, Key: key})
	require.NoError(t, err)

	require.NoError(t, store.Stop())
	store = standalone_storage.NewStandAloneStorage(conf)
	require.NoError(t, store.Start())
	defer store.Stop()
	server = NewServer(store)

	require.Equal(t, []byte("default-value"), rawBusinessGet(t, server, engine_util.CfDefault, key).Value)
	require.True(t, rawBusinessGet(t, server, engine_util.CfLock, key).NotFound)
	require.Equal(t, []byte("write-value"), rawBusinessGet(t, server, engine_util.CfWrite, key).Value)
}

func TestRawKVScanPaginationAfterUpdatesAndDeletes(t *testing.T) {
	_, store, server := newRawBusinessStorage(t)
	defer store.Stop()

	for _, pair := range []struct{ key, value string }{
		{"a", "value-a"}, {"b", "value-b"}, {"c", "value-c"}, {"d", "old-d"}, {"e", "value-e"},
	} {
		rawBusinessPut(t, server, engine_util.CfDefault, []byte(pair.key), []byte(pair.value))
	}
	_, err := server.RawDelete(context.Background(), &kvrpcpb.RawDeleteRequest{Cf: engine_util.CfDefault, Key: []byte("b")})
	require.NoError(t, err)
	rawBusinessPut(t, server, engine_util.CfDefault, []byte("d"), []byte("new-d"))

	first, err := server.RawScan(context.Background(), &kvrpcpb.RawScanRequest{
		Cf: engine_util.CfDefault, StartKey: []byte("a"), Limit: 2,
	})
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a"), []byte("c")}, [][]byte{first.Kvs[0].Key, first.Kvs[1].Key})

	second, err := server.RawScan(context.Background(), &kvrpcpb.RawScanRequest{
		Cf: engine_util.CfDefault, StartKey: append(first.Kvs[1].Key, 0), Limit: 2,
	})
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("d"), []byte("e")}, [][]byte{second.Kvs[0].Key, second.Kvs[1].Key})
	require.Equal(t, []byte("new-d"), second.Kvs[0].Value)
}

func TestRawKVConcurrentWritesRemainDurable(t *testing.T) {
	conf, store, server := newRawBusinessStorage(t)
	const workers = 6
	const writesPerWorker = 40

	errCh := make(chan error, workers*writesPerWorker)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := 0; seq < writesPerWorker; seq++ {
				key := []byte(fmt.Sprintf("worker/%02d/%03d", worker, seq))
				value := []byte(fmt.Sprintf("value-%02d-%03d", worker, seq))
				resp, err := server.RawPut(context.Background(), &kvrpcpb.RawPutRequest{
					Cf: engine_util.CfDefault, Key: key, Value: value,
				})
				if err != nil {
					errCh <- err
					continue
				}
				if resp.Error != "" {
					errCh <- fmt.Errorf("raw put failed: %s", resp.Error)
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	require.NoError(t, store.Stop())
	store = standalone_storage.NewStandAloneStorage(conf)
	require.NoError(t, store.Start())
	defer store.Stop()
	server = NewServer(store)

	resp, err := server.RawScan(context.Background(), &kvrpcpb.RawScanRequest{
		Cf: engine_util.CfDefault, StartKey: []byte("worker/"), Limit: workers * writesPerWorker,
	})
	require.NoError(t, err)
	require.Len(t, resp.Kvs, workers*writesPerWorker)
	for worker := 0; worker < workers; worker++ {
		for seq := 0; seq < writesPerWorker; seq++ {
			key := []byte(fmt.Sprintf("worker/%02d/%03d", worker, seq))
			value := []byte(fmt.Sprintf("value-%02d-%03d", worker, seq))
			require.Equal(t, value, rawBusinessGet(t, server, engine_util.CfDefault, key).Value)
		}
	}
}
