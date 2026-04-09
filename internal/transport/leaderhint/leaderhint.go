package leaderhint

import (
	pb "github.com/Mfon-19/clavis/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Error returns a gRPC status error with a structured leader hint.
func Error(grpcAddress string) error {
	st := status.New(codes.Unavailable, "not leader")
	if grpcAddress == "" {
		return st.Err()
	}

	stWithDetails, err := st.WithDetails(&pb.LeaderHint{
		GrpcAddress: grpcAddress,
	})
	if err != nil {
		return st.Err()
	}

	return stWithDetails.Err()
}

// FromError extracts a leader gRPC address from structured status details.
func FromError(err error) string {
	if err == nil {
		return ""
	}

	st, ok := status.FromError(err)
	if !ok {
		return ""
	}

	for _, detail := range st.Details() {
		if hint, ok := detail.(*pb.LeaderHint); ok && hint.GetGrpcAddress() != "" {
			return hint.GetGrpcAddress()
		}
	}
	return ""
}
