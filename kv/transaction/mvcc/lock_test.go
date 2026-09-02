package mvcc

import (
	"testing"

	"github.com/Alorun/stonekv/kv/config"
	"github.com/Alorun/stonekv/kv/storage"
	"github.com/Alorun/stonekv/kv/storage/standalone_storage"
	"github.com/Alorun/stonekv/kv/util/engine_util"
	"github.com/stretchr/testify/require"
)

func TestAllLocksForTxnStartsRocketDBIterator(t *testing.T) {
	conf := config.NewTestConfig()
	conf.DBPath = t.TempDir()
	store := standalone_storage.NewStandAloneStorage(conf)
	defer store.Stop()

	for _, entry := range []struct {
		key []byte
		ts  uint64
	}{
		{key: []byte("matching-a"), ts: 42},
		{key: []byte("matching-b"), ts: 42},
		{key: []byte("other-transaction"), ts: 99},
	} {
		lock := (&Lock{Primary: []byte("matching-a"), Ts: entry.ts, Ttl: 100, Kind: WriteKindPut}).ToBytes()
		require.NoError(t, store.Write(nil, []storage.Modify{{Data: storage.Put{
			Cf: engine_util.CfLock, Key: entry.key, Value: lock,
		}}}))
	}

	reader, err := store.Reader(nil)
	require.NoError(t, err)
	defer reader.Close()

	locks, err := AllLocksForTxn(NewMvccTxn(reader, 42))
	require.NoError(t, err)
	require.Len(t, locks, 2)
	require.Equal(t, []byte("matching-a"), locks[0].Key)
	require.Equal(t, []byte("matching-b"), locks[1].Key)
}
