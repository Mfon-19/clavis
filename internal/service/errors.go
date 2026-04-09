package service

// InvalidArgumentError is returned when request parameters fail validation.
// The transport layer maps this to gRPC codes.InvalidArgument.
type InvalidArgumentError struct {
	Message string
}

func (e *InvalidArgumentError) Error() string {
	return e.Message
}

// NotLeaderError is returned when a write operation is attempted on a
// follower node. It carries the current leader's gRPC address so clients
// can redirect automatically. The transport layer maps this to
// gRPC codes.Unavailable with a structured LeaderHint status detail.
type NotLeaderError struct {
	LeaderGRPCAddress string
}

func (e *NotLeaderError) Error() string {
	return "not leader"
}
