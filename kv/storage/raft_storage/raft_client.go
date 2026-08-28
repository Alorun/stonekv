package raft_storage

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Alorun/stonekv/kv/config"
	"github.com/Alorun/stonekv/log"
	"github.com/Alorun/stonekv/proto/pkg/raft_serverpb"
	"github.com/Alorun/stonekv/proto/pkg/stonekvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type raftConn struct {
	streamMu   sync.Mutex
	stream     stonekvpb.StoneKv_RaftClient
	ctx        context.Context
	cancel     context.CancelFunc
	clientConn *grpc.ClientConn
	stopOnce   sync.Once
}

func newRaftConn(addr string, cfg *config.Config) (*raftConn, error) {
	cc, err := grpc.Dial(addr, grpc.WithInsecure(),
		grpc.WithInitialWindowSize(2*1024*1024),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                3 * time.Second,
			Timeout:             60 * time.Second,
			PermitWithoutStream: true,
		}))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := stonekvpb.NewStoneKvClient(cc).Raft(ctx)
	if err != nil {
		cancel()
		_ = cc.Close()
		return nil, err
	}
	return &raftConn{
		stream:     stream,
		ctx:        ctx,
		cancel:     cancel,
		clientConn: cc,
	}, nil
}

func (c *raftConn) Stop() {
	c.stopOnce.Do(func() {
		c.cancel()
		if c.clientConn != nil {
			_ = c.clientConn.Close()
		}
	})
}

func (c *raftConn) Send(msg *raft_serverpb.RaftMessage) error {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	return c.stream.Send(msg)
}

type RaftClient struct {
	config *config.Config
	sync.RWMutex
	conns   map[string]*raftConn
	addrs   map[uint64]string
	stopped bool
}

var errRaftClientStopped = errors.New("raft client stopped")

func newRaftClient(config *config.Config) *RaftClient {
	return &RaftClient{
		config: config,
		conns:  make(map[string]*raftConn),
		addrs:  make(map[uint64]string),
	}
}

func (c *RaftClient) getConn(addr string, regionID uint64) (*raftConn, error) {
	c.RLock()
	if c.stopped {
		c.RUnlock()
		return nil, errRaftClientStopped
	}
	conn, ok := c.conns[addr]
	if ok {
		c.RUnlock()
		return conn, nil
	}
	c.RUnlock()
	newConn, err := newRaftConn(addr, c.config)
	if err != nil {
		return nil, err
	}
	c.Lock()
	defer c.Unlock()
	if c.stopped {
		newConn.Stop()
		return nil, errRaftClientStopped
	}
	if conn, ok := c.conns[addr]; ok {
		newConn.Stop()
		return conn, nil
	}
	c.conns[addr] = newConn
	return newConn, nil
}

func (c *RaftClient) Send(storeID uint64, addr string, msg *raft_serverpb.RaftMessage) error {
	conn, err := c.getConn(addr, msg.GetRegionId())
	if err != nil {
		return err
	}
	err = conn.Send(msg)
	if err == nil {
		return nil
	}

	log.Error("raft client failed to send")
	c.Lock()
	defer c.Unlock()
	conn.Stop()
	delete(c.conns, addr)
	if oldAddr, ok := c.addrs[storeID]; ok && oldAddr == addr {
		delete(c.addrs, storeID)
	}
	return err
}

func (c *RaftClient) GetAddr(storeID uint64) string {
	c.RLock()
	defer c.RUnlock()
	v, _ := c.addrs[storeID]
	return v
}

func (c *RaftClient) InsertAddr(storeID uint64, addr string) {
	c.Lock()
	defer c.Unlock()
	if c.stopped {
		return
	}
	c.addrs[storeID] = addr
}

func (c *RaftClient) Flush() {
	// Not support BufferHint
}

func (c *RaftClient) Stop() {
	c.Lock()
	if c.stopped {
		c.Unlock()
		return
	}
	c.stopped = true
	conns := c.conns
	c.conns = make(map[string]*raftConn)
	c.addrs = make(map[uint64]string)
	c.Unlock()

	for _, conn := range conns {
		conn.Stop()
	}
}
