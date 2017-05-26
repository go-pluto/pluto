package imap

import (
	"bufio"
	"fmt"
	"testing"
	"time"

	"crypto/tls"

	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/crypto"
	"github.com/stretchr/testify/assert"
)

// TestInternalConnectAndSend tests two of the
// main internal communication functions of pluto.
func TestInternalConnectAndSend(t *testing.T) {

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

		var c Connection

		conn, err := worker.Accept()
		if err != nil {
			t.Fatal(err)
		}

		tlsConn, ok := conn.(*tls.Conn)
		assert.Equal(t, true, ok, "Worker-1 should be able to assert TLS connection")

		c.IncConn = tlsConn
		c.IncReader = bufio.NewReader(tlsConn)

		text, err := c.InternalReceive(true)
		assert.Nil(t, err, "InternalReceive at worker-1 should not return an error")
		assert.Equal(t, "test", text, "InternalReceive at worker-1 should have received 'test'")

		c.Terminate()
		worker.Close()
	}()

	// Listen on a port for TLS connections on storage.
	storage, err := tls.Listen("tcp", addrStorage, tlsConfiS)
	if err != nil {
		t.Fatal(err)
	}

	go func() {

		var c Connection

		conn, err := storage.Accept()
		if err != nil {
			t.Fatal(err)
		}

		tlsConn, ok := conn.(*tls.Conn)
		assert.Equal(t, true, ok, "Storage should be able to assert TLS connection")

		c.IncConn = tlsConn
		c.IncReader = bufio.NewReader(tlsConn)

		text, err := c.InternalReceive(true)
		assert.Nil(t, err, "InternalReceive at storage should not return an error")
		assert.Equal(t, "rofl", text, "InternalReceive at storage should have received 'rofl'")

		c.Terminate()
		storage.Close()
	}()

	var c Connection
	c.IntlTLSConfig = tlsConfigD

	c.IncConn, err = InternalConnect(addrWorker, c.IntlTLSConfig, 0, false, "")
	assert.Nil(t, err, "InternalConnect (worker-only) should not return an error")

	err = c.InternalSend(true, "test", false, "")
	assert.Nil(t, err, "InternalSend (worker-only) should not return an error")

	time.Sleep(1 * time.Second)

	err = c.InternalSend(true, "rofl", true, addrStorage)
	assert.Nil(t, err, "InternalSend (failover to storage) should not return an error")

	time.Sleep(1 * time.Second)
}
