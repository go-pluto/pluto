package imap

import (
	"fmt"
	"testing"

	"crypto/tls"

	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/crypto"
	"github.com/stretchr/testify/assert"
)

func TestInternalConnect(t *testing.T) {

	// Read configuration from file.
	config, err := config.LoadConfig("../test-config.toml")
	if err != nil {
		t.Fatal(err)
	}

	// Shortcut for config values for distributor.
	cDistr := config.Distributor

	// Shortcut for config values for "worker-1".
	cWorker := config.Workers["worker-1"]
	addrWorker := fmt.Sprintf("%s:%s", cWorker.ListenIP, cWorker.SyncPort)

	// Shortcut for config values for "storage".
	cStorage := config.Storage
	addrStorage := fmt.Sprintf("%s:%s", cStorage.ListenIP, cStorage.SyncPort)

	// Load internal TLS config for distributor.
	// This will be used as the connecting TLS config.
	tlsConfigD, err := crypto.NewInternalTLSConfig(cDistr.InternalTLS.CertLoc, cDistr.InternalTLS.KeyLoc, config.RootCertLoc)
	if err != nil {
		t.Fatal(err)
	}

	// Load internal TLS config for worker.
	tlsConfigW, err := crypto.NewInternalTLSConfig(cWorker.TLS.CertLoc, cWorker.TLS.KeyLoc, config.RootCertLoc)
	if err != nil {
		t.Fatal(err)
	}

	// Load internal TLS config for storage.
	tlsConfiS, err := crypto.NewInternalTLSConfig(cStorage.TLS.CertLoc, cStorage.TLS.KeyLoc, config.RootCertLoc)
	if err != nil {
		t.Fatal(err)
	}

	// Listen on a port for TLS connections on worker.
	worker, err := tls.Listen("tcp", addrWorker, tlsConfigW)
	if err != nil {
		t.Fatal(err)
	}

	go func() {

		conn, err := worker.Accept()
		if err != nil {
			t.Fatal(err)
		}

		tlsConn, ok := conn.(*tls.Conn)
		assert.Equal(t, true, ok, "Worker-1 should be able to assert TLS connection")

		err = tlsConn.Handshake()
		assert.Nil(t, err, "TLS handshake should not yield an error")

		tlsConn.Close()
		fmt.Println("Closed conn at worker-1")
	}()

	// Listen on a port for TLS connections on storage.
	storage, err := tls.Listen("tcp", addrStorage, tlsConfiS)
	if err != nil {
		t.Fatal(err)
	}

	go func() {

		conn, err := storage.Accept()
		if err != nil {
			t.Fatal(err)
		}

		tlsConn, ok := conn.(*tls.Conn)
		assert.Equal(t, true, ok, "Storage should be able to assert TLS connection")

		err = tlsConn.Handshake()
		assert.Nil(t, err, "TLS handshake should not yield an error")

		tlsConn.Close()
		fmt.Println("Closed conn at storage")
	}()

	_, err = InternalConnect(addrWorker, tlsConfigD, 0, false, "")
	assert.Nil(t, err, "InternalConnect (worker-only) should not return an error")

	_, err = InternalConnect(addrWorker, tlsConfigD, 0, true, addrStorage)
	assert.Nil(t, err, "InternalConnect (failover to storage) should not return an error")
}
