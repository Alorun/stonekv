package stonekvpb_test

import (
	"context"
	"net"
	"testing"

	"github.com/Alorun/stonekv/kv/server"
	"github.com/Alorun/stonekv/kv/storage"
	"github.com/Alorun/stonekv/kv/util/engine_util"
	"github.com/Alorun/stonekv/proto/pkg/kvrpcpb"
	"github.com/Alorun/stonekv/proto/pkg/stonekvpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func TestTinyKvCompatibilityService(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	kvServer := server.NewServer(storage.NewMemStorage())
	stonekvpb.RegisterStoneKvServer(grpcServer, kvServer)
	stonekvpb.RegisterTinyKvCompatibilityServer(grpcServer, kvServer)

	go func() {
		_ = grpcServer.Serve(listener)
	}()
	defer grpcServer.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithInsecure(),
	)
	require.NoError(t, err)
	defer conn.Close()

	putResp := new(kvrpcpb.RawPutResponse)
	err = conn.Invoke(ctx, "/tinykvpb.TinyKv/RawPut", &kvrpcpb.RawPutRequest{
		Key:   []byte("hello"),
		Value: []byte("stonekv"),
		Cf:    engine_util.CfDefault,
	}, putResp)
	require.NoError(t, err)

	getResp := new(kvrpcpb.RawGetResponse)
	err = conn.Invoke(ctx, "/tinykvpb.TinyKv/RawGet", &kvrpcpb.RawGetRequest{
		Key: []byte("hello"),
		Cf:  engine_util.CfDefault,
	}, getResp)
	require.NoError(t, err)
	require.Equal(t, []byte("stonekv"), getResp.Value)
}
