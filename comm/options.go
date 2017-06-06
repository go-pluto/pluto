package comm

import (
	"time"

	"crypto/tls"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// ReceiverOptions returns a list of gRPC server
// options that the internal receiver uses for RPCs.
func ReceiverOptions(tlsConfig *tls.Config) []grpc.ServerOption {

	// Use pluto-internal TLS config for credentials.
	creds := credentials.NewTLS(tlsConfig)

	// Use the custom NoOp codec that simply passes
	// through received binary messages.
	codec := NoOpCodec{}

	kaParams := keepalive.ServerParameters{
		// Any internal connection will be closed after
		// 5 minutes of being in idle state.
		MaxConnectionIdle: 5 * time.Minute,
		// The receiver will ping the other node after
		// 1 minute of inactivity for keepalive.
		Time: 1 * time.Minute,
		// If no response to such keepalive ping is received
		// after 30 seconds, the connection is closed.
		Timeout: 30 * time.Second,
	}

	enfPolicy := keepalive.EnforcementPolicy{
		// Clients connecting to this receiver should wait
		// at least 1 minute before sending a keepalive.
		MinTime: 1 * time.Minute,
		// Expect keepalives even when no streams are active.
		PermitWithoutStream: true,
	}

	// Use GZIP for compression and decompression.
	comp := grpc.NewGZIPCompressor()
	decomp := grpc.NewGZIPDecompressor()

	// TODO: Think about clever stats handler. Prometheus-exposed?
	// stats := grpc.StatsHandler(h)

	// Set the maximum number of bytes a message is allowed to
	// carry to (256 * 1024 * 1024 B) + 2048 B (buffer) > 256 MiB.
	// Symmetric - send and receive option.
	maxMsgSize := 268437504

	return []grpc.ServerOption{
		grpc.Creds(creds),
		grpc.CustomCodec(codec),
		grpc.RPCCompressor(comp),
		grpc.RPCDecompressor(decomp),
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
		grpc.KeepaliveParams(kaParams),
		grpc.KeepaliveEnforcementPolicy(enfPolicy),
	}
}
