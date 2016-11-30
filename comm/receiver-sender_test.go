package comm_test

import (
	"fmt"
	"testing"
	"time"

	"crypto/tls"

	"github.com/numbleroot/pluto/comm"
	"github.com/numbleroot/pluto/crypto"
	"github.com/numbleroot/pluto/utils"
)

// Functions

// Execute a black-box integration test on implemented
// main functions of sender and receiver.
func TestSenderReceiver(t *testing.T) {

	// Names of nodes to test with.
	n1 := "worker-1"
	n2 := "storage"

	// Create needed test environment.
	config, _, err := utils.CreateTestEnv()
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected test environment creation not to fail but received: %s\n", err.Error())
	}

	// Load internal TLS config for worker-1.
	internalTLSConfigN1, err := crypto.NewInternalTLSConfig(config.Workers[n1].TLS.CertLoc, config.Workers[n1].TLS.KeyLoc, config.RootCertLoc)
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected no error while loading internal TLS config for worker-1 but received: %s\n", err.Error())
	}

	// Load internal TLS config for storage.
	internalTLSConfigN2, err := crypto.NewInternalTLSConfig(config.Storage.TLS.CertLoc, config.Storage.TLS.KeyLoc, config.RootCertLoc)
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected no error while loading internal TLS config for storage but received: %s\n", err.Error())
	}

	// Listen on defined worker-1 socket for TLS connections.
	socketN1, err := tls.Listen("tcp", fmt.Sprintf("%s:%s", config.Workers[n1].IP, config.Workers[n1].SyncPort), internalTLSConfigN1)
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected TLS listen for worker-1 not to fail but received: %s\n", err.Error())
	}

	// Listen on defined storage socket for TLS connections.
	socketN2, err := tls.Listen("tcp", fmt.Sprintf("%s:%s", config.Storage.IP, config.Storage.SyncPort), internalTLSConfigN2)
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected TLS listen for storage not to fail but received: %s\n", err.Error())
	}

	// Initialize receiver in background at worker-1.
	chanIncN1, chanUpdN1, err := comm.InitReceiver(n1, "../test-receiving-worker-1.log", socketN1, []string{n2})
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected InitReceiver() for worker-1 not to fail but received: %s\n", err.Error())
	}

	// Initialize receiver in foreground at storage.
	recv2, chanIncN2, chanUpdN2, err := comm.InitReceiverForeground(n2, "../test-receiving-storage.log", socketN2, []string{n1})
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected InitReceiverForeground() for storage not to fail but received: %s\n", err.Error())
	}

	go func() {

		// Apply received CRDT messages at storage.
		_ = recv2.AcceptIncMsgs()
	}()

	// Wait shortly for goroutines to have started.
	time.Sleep(200 * time.Millisecond)

	// Connect via TLS from worker-1 to storage.
	cToN2, err := tls.Dial("tcp", fmt.Sprintf("%s:%s", config.Storage.IP, config.Storage.SyncPort), internalTLSConfigN1)
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected to be able to connect from worker-1 to storage but received: %s\n", err.Error())
	}

	// Create map of connections for worker-1.
	connsN1 := make(map[string]*tls.Conn)
	connsN1[n2] = cToN2

	// Connect via TLS from storage to worker-1.
	cToN1, err := tls.Dial("tcp", fmt.Sprintf("%s:%s", config.Workers[n1].IP, config.Workers[n1].SyncPort), internalTLSConfigN2)
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected to be able to connect from storage to worker-1 but received: %s\n", err.Error())
	}

	// Create map of connections for storage.
	connsN2 := make(map[string]*tls.Conn)
	connsN2[n1] = cToN1

	// Initialize sending interface for worker-1.
	chan1, err := comm.InitSender(n1, "../test-sending-worker-1.log", chanIncN1, chanUpdN1, connsN1)
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected InitSender() for worker-1 not to fail but received: %s\n", err.Error())
	}

	// Initialize sending interface for storage.
	chan2, err := comm.InitSender(n2, "../test-sending-storage.log", chanIncN2, chanUpdN2, connsN2)
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected InitSender() for storage not to fail but received: %s\n", err.Error())
	}

	chan1 <- "rmv|brathering|y"
	chan2 <- "add|toast|z"

	// Let output finish.
	time.Sleep(1 * time.Second)
}
