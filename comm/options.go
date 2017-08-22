package comm

import (
	"time"

	"crypto/tls"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// Set the maximum number of bytes a message is allowed to
// carry to (256 * 1024 * 1024 B) + 2048 B (buffer) > 256 MiB.
// Symmetric - send and receive option.
var maxMsgSize = 268437504

// ReceiverOptions returns a list of gRPC server
// options that the internal receiver uses for RPCs.
func ReceiverOptions(tlsConfig *tls.Config) []grpc.ServerOption {

	// Use pluto-internal TLS config for credentials.
	creds := credentials.NewTLS(tlsConfig)

	enfPolicy := keepalive.EnforcementPolicy{
		// Clients connecting to this receiver should wait
		// at least 30 seconds before sending a keepalive.
		MinTime: 30 * time.Second,
		// Expect keepalives even when no streams are active.
		PermitWithoutStream: true,
	}

	kaParams := keepalive.ServerParameters{
		// The receiver will ping the other node after
		// 30 seconds of inactivity for keepalive.
		Time: 30 * time.Second,
		// If no response to such keepalive ping is received
		// after 20 seconds, the connection is closed.
		Timeout: 20 * time.Second,
	}

	// Use GZIP for compression and decompression.
	comp := grpc.NewGZIPCompressor()
	decomp := grpc.NewGZIPDecompressor()

	return []grpc.ServerOption{
		grpc.Creds(creds),
		grpc.KeepaliveEnforcementPolicy(enfPolicy),
		grpc.KeepaliveParams(kaParams),
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
		grpc.RPCCompressor(comp),
		grpc.RPCDecompressor(decomp),
	}
}

// SenderOptions defines gRPC options for connection
// attempts from a sender to a receiver.
func SenderOptions(tlsConfig *tls.Config) []grpc.DialOption {

	// Use GZIP for compression and decompression.
	comp := grpc.NewGZIPCompressor()
	decomp := grpc.NewGZIPDecompressor()

	// These call options will be used for every call
	// via this connection.
	callOpts := []grpc.CallOption{
		// Fail immediately if connection is closed.
		grpc.FailFast(true),
		// Set maximum receive and send sizes.
		grpc.MaxCallRecvMsgSize(maxMsgSize),
		grpc.MaxCallSendMsgSize(maxMsgSize),
	}

	kaParams := keepalive.ClientParameters{
		// The client will ping the other node after
		// 30 seconds of inactivity for keepalive.
		Time: 30 * time.Second,
		// If no response to such keepalive ping is received
		// after 20 seconds, the connection is closed.
		Timeout: 20 * time.Second,
		// Expect keepalives even when no streams are active.
		PermitWithoutStream: true,
	}

	// Use pluto-internal TLS config for credentials.
	creds := credentials.NewTLS(tlsConfig)

	return []grpc.DialOption{
		grpc.WithBackoffMaxDelay(8 * time.Second),
		grpc.WithBlock(),
		grpc.WithCompressor(comp),
		grpc.WithDecompressor(decomp),
		grpc.WithDefaultCallOptions(callOpts...),
		grpc.WithKeepaliveParams(kaParams),
		grpc.WithTimeout(26 * time.Second),
		grpc.WithTransportCredentials(creds),
	}
}
