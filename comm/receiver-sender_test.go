package comm_test

import (
	"fmt"
	"log"
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

	// Make needed channels.
	n1ApplyCRDTUpdChan := make(chan string)
	n1DoneCRDTUpdChan := make(chan struct{})
	n2ApplyCRDTUpdChan := make(chan string)
	n2DoneCRDTUpdChan := make(chan struct{})
	n1DownRecv := make(chan struct{})
	n1DownSender := make(chan struct{})
	n2DownRecv := make(chan struct{})
	n2DownSender := make(chan struct{})

	// Create needed test environment.
	testEnv, err := utils.CreateTestEnv()
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected test environment creation not to fail but received: %s\n", err.Error())
	}

	// Load internal TLS config for worker-1.
	internalTLSConfigN1, err := crypto.NewInternalTLSConfig(testEnv.Config.Workers[n1].TLS.CertLoc, testEnv.Config.Workers[n1].TLS.KeyLoc, testEnv.Config.RootCertLoc)
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected no error while loading internal TLS config for worker-1 but received: %s\n", err.Error())
	}

	// Load internal TLS config for storage.
	internalTLSConfigN2, err := crypto.NewInternalTLSConfig(testEnv.Config.Storage.TLS.CertLoc, testEnv.Config.Storage.TLS.KeyLoc, testEnv.Config.RootCertLoc)
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected no error while loading internal TLS config for storage but received: %s\n", err.Error())
	}

	// Listen on defined worker-1 socket for TLS connections.
	socketN1, err := tls.Listen("tcp", fmt.Sprintf("%s:%s", testEnv.Config.Workers[n1].IP, testEnv.Config.Workers[n1].SyncPort), internalTLSConfigN1)
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected TLS listen for worker-1 not to fail but received: %s\n", err.Error())
	}

	// Listen on defined storage socket for TLS connections.
	socketN2, err := tls.Listen("tcp", fmt.Sprintf("%s:%s", testEnv.Config.Storage.IP, testEnv.Config.Storage.SyncPort), internalTLSConfigN2)
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected TLS listen for storage not to fail but received: %s\n", err.Error())
	}

	// Initialize receiver in background at worker-1.
	chanIncN1, chanUpdN1, err := comm.InitReceiver(n1, "../test-comm-receiving-worker-1.log", socketN1, n1ApplyCRDTUpdChan, n1DoneCRDTUpdChan, n1DownRecv, []string{n2})
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected InitReceiver() for worker-1 not to fail but received: %s\n", err.Error())
	}

	go func() {

		// Dummy-apply messages in background.

		for {

			// Receive messages to update on this channel.
			updMsg := <-n1ApplyCRDTUpdChan

			// Log message.
			log.Printf("[comm_test.TestSenderReceiver] %s: Would apply update from message here: %s\n", n1, updMsg)

			// Signal success.
			n1DoneCRDTUpdChan <- struct{}{}
		}
	}()

	// Initialize receiver in foreground at storage.
	chanIncN2, chanUpdN2, err := comm.InitReceiver(n2, "../test-comm-receiving-storage.log", socketN2, n2ApplyCRDTUpdChan, n2DoneCRDTUpdChan, n2DownRecv, []string{n1})
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected InitReceiver() for storage not to fail but received: %s\n", err.Error())
	}

	go func() {

		// Dummy-apply messages in background.

		for {

			// Receive messages to update on this channel.
			updMsg := <-n2ApplyCRDTUpdChan

			// Log message.
			log.Printf("[comm_test.TestSenderReceiver] %s: Would apply update from message here: %s\n", n2, updMsg)

			// Signal success.
			n2DoneCRDTUpdChan <- struct{}{}
		}
	}()

	// Wait shortly for goroutines to have started.
	time.Sleep(200 * time.Millisecond)

	// Connect via TLS from worker-1 to storage.
	cToN2, err := tls.Dial("tcp", fmt.Sprintf("%s:%s", testEnv.Config.Storage.IP, testEnv.Config.Storage.SyncPort), internalTLSConfigN1)
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected to be able to connect from worker-1 to storage but received: %s\n", err.Error())
	}

	// Create map of connections for worker-1.
	connsN1 := make(map[string]*tls.Conn)
	connsN1[n2] = cToN2

	// Connect via TLS from storage to worker-1.
	cToN1, err := tls.Dial("tcp", fmt.Sprintf("%s:%s", testEnv.Config.Workers[n1].IP, testEnv.Config.Workers[n1].SyncPort), internalTLSConfigN2)
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected to be able to connect from storage to worker-1 but received: %s\n", err.Error())
	}

	// Create map of connections for storage.
	connsN2 := make(map[string]*tls.Conn)
	connsN2[n1] = cToN1

	// Initialize sending interface for worker-1.
	chan1, err := comm.InitSender(n1, "../test-comm-sending-worker-1.log", chanIncN1, chanUpdN1, n1DownSender, connsN1)
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected InitSender() for worker-1 not to fail but received: %s\n", err.Error())
	}

	// Initialize sending interface for storage.
	chan2, err := comm.InitSender(n2, "../test-comm-sending-storage.log", chanIncN2, chanUpdN2, n2DownSender, connsN2)
	if err != nil {
		t.Fatalf("[comm_test.TestSenderReceiver] Expected InitSender() for storage not to fail but received: %s\n", err.Error())
	}

	chan1 <- "test message"
	chan2 <- "yay, it works!"

	// Let output finish.
	socketN1.Close()
	socketN2.Close()
	time.Sleep(1 * time.Second)
}
