package stonekvpb

import "google.golang.org/grpc"

// RegisterTinyKvCompatibilityServer registers the StoneKV implementation under
// TinyKV's original fully-qualified gRPC service name. TinySQL uses this name,
// while the request and response protobuf messages are wire-compatible with
// StoneKV's messages.
func RegisterTinyKvCompatibilityServer(s *grpc.Server, srv StoneKvServer) {
	desc := _StoneKv_serviceDesc
	desc.ServiceName = "tinykvpb.TinyKv"
	desc.Metadata = "tinykvpb.proto"
	s.RegisterService(&desc, srv)
}
