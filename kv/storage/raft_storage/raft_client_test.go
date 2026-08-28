package raft_storage

import (
	"context"
	"testing"

	"github.com/Alorun/stonekv/kv/config"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

func TestRaftClientStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	clientConn, err := grpc.Dial("passthrough:///unused", grpc.WithInsecure())
	require.NoError(t, err)
	conn := &raftConn{ctx: ctx, cancel: cancel, clientConn: clientConn}
	client := newRaftClient(config.NewTestConfig())
	client.conns["store"] = conn
	client.addrs[1] = "store"

	client.Stop()
	client.Stop()

	select {
	case <-ctx.Done():
	default:
		t.Fatal("raft stream context was not canceled")
	}
	require.Empty(t, client.conns)
	require.Empty(t, client.addrs)
	require.Equal(t, connectivity.Shutdown, clientConn.GetState())
	_, err = client.getConn("store", 1)
	require.ErrorIs(t, err, errRaftClientStopped)
}
