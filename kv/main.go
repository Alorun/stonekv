package main

import (
	"flag"
	"net"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Alorun/stonekv/kv/config"
	"github.com/Alorun/stonekv/kv/server"
	"github.com/Alorun/stonekv/kv/storage"
	"github.com/Alorun/stonekv/kv/storage/raft_storage"
	"github.com/Alorun/stonekv/kv/storage/standalone_storage"
	"github.com/Alorun/stonekv/log"
	"github.com/Alorun/stonekv/proto/pkg/stonekvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	schedulerAddr = flag.String("scheduler", "", "scheduler address")
	storeAddr     = flag.String("addr", "", "store address")
	dbPath        = flag.String("path", "", "directory path of db")
	logLevel      = flag.String("loglevel", "", "the level of log")
)

func main() {
	flag.Parse()
	conf := config.NewDefaultConfig()
	if *schedulerAddr != "" {
		conf.SchedulerAddr = *schedulerAddr
	}
	if *storeAddr != "" {
		conf.StoreAddr = *storeAddr
	}
	if *dbPath != "" {
		conf.DBPath = *dbPath
	}
	if *logLevel != "" {
		conf.LogLevel = *logLevel
	}

	log.SetLevelByString(conf.LogLevel)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Infof("Server started with conf %+v", conf)
	if err := run(conf); err != nil {
		log.Fatal(err)
	}
}

func run(conf *config.Config) (err error) {
	var store storage.Storage
	if conf.Raft {
		store = raft_storage.NewRaftStorage(conf)
	} else {
		store = standalone_storage.NewStandAloneStorage(conf)
	}
	if err = store.Start(); err != nil {
		return err
	}
	defer func() {
		if stopErr := store.Stop(); stopErr != nil {
			if err == nil {
				err = stopErr
			} else {
				log.Errorf("stop storage failed: %v", stopErr)
			}
		}
	}()

	kvServer := server.NewServer(store)

	// If a client pings more than once every 2 seconds, terminate the connection
	// Allow pings even when there are no active streams
	var alivePolicy = keepalive.EnforcementPolicy{
		MinTime:             2 * time.Second, 
		PermitWithoutStream: true,            
	}

	grpcServer := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(alivePolicy),
		grpc.InitialWindowSize(1<<30),
		grpc.InitialConnWindowSize(1<<30),
		grpc.MaxRecvMsgSize(10*1024*1024),
	)
	stonekvpb.RegisterStoneKvServer(grpcServer, kvServer)
	stonekvpb.RegisterTinyKvCompatibilityServer(grpcServer, kvServer)
	listenAddr := conf.StoreAddr[strings.IndexByte(conf.StoreAddr, ':'):]
	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}
	handleSignal(grpcServer)

	if err = grpcServer.Serve(l); err != nil {
		return err
	}
	log.Info("Server stopped.")
	return nil
}

func handleSignal(grpcServer *grpc.Server) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		sig := <-sigCh
		log.Infof("Got signal [%s] to exit.", sig)
		grpcServer.Stop()
	}()
}
