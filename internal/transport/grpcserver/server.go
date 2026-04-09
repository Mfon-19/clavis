// Package grpcserver implements the LockService gRPC server. It is a pure
// adapter layer. Each method maps a protobuf request to a service call and
// translates the response back. Error mapping in centralized in errors.go
package grpcserver

import pb "github.com/Mfon-19/clavis/api/v1"

type Server struct {
	pb.UnimplementedLockServiceServer
	pb.UnimplementedAdminServiceServer
}
