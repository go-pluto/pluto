package imap

import (
	"fmt"
	"testing"
	"time"

	"crypto/tls"

	"github.com/numbleroot/pluto/utils"
)

// Functions

// TestInitNode executes a white-box unit test on the
// implemented InitNode() function.
func TestInitNode(t *testing.T) {

	var err error
	var Node *Node

	// Create needed test environment.
	Config, _ := utils.CreateTestEnv()

	// All types indicated.
	_, err = InitNode(Config, true, "worker-1", true)
	if err.Error() != "[imap.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n" {
		t.Fatalf("[imap.TestInitNode] Expected '%s' but received '%s'\n", "[imap.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n", err.Error())
	}

	// Two types indicated.
	_, err = InitNode(Config, true, "abc", false)
	if err.Error() != "[imap.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n" {
		t.Fatalf("[imap.TestInitNode] Expected '%s' but received '%s'\n", "[imap.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n", err.Error())
	}

	// Two types indicated.
	_, err = InitNode(Config, true, "", true)
	if err.Error() != "[imap.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n" {
		t.Fatalf("[imap.TestInitNode] Expected '%s' but received '%s'\n", "[imap.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n", err.Error())
	}

	// Two types indicated.
	_, err = InitNode(Config, false, "worker-1", true)
	if err.Error() != "[imap.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n" {
		t.Fatalf("[imap.TestInitNode] Expected '%s' but received '%s'\n", "[imap.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n", err.Error())
	}

	// No type indicated.
	_, err = InitNode(Config, false, "", false)
	if err.Error() != "[imap.InitNode] Node must be of one type, either '-distributor' or '-worker WORKER-ID' or '-storage'.\n" {
		t.Fatalf("[imap.TestInitNode] Expected '%s' but received '%s'\n", "[imap.InitNode] Node must be of one type, either '-distributor' or '-worker WORKER-ID' or '-storage'.\n", err.Error())
	}

	// Non-existent worker ID.
	_, err = InitNode(Config, false, "abc", false)
	if (err.Error() != "[imap.InitNode] Specified worker ID does not exist in config file. Please provide a valid one, for example 'worker-1'.\n") && (err.Error() != "[imap.InitNode] Specified worker ID does not exist in config file. Please provide a valid one, for example 'worker-2'.\n") && (err.Error() != "[imap.InitNode] Specified worker ID does not exist in config file. Please provide a valid one, for example 'worker-3'.\n") {
		t.Fatalf("[imap.TestInitNode] Expected '%s' but received '%s'\n", "[imap.InitNode] Specified worker ID does not exist in config file. Please provide a valid one, for example 'worker-1'.\n", err.Error())
	}

	// Set certificate path in config to non-existent one.
	Config.Distributor.TLS.CertLoc = "../private/not-existing.cert"

	// Config error: non-existent certificate.
	_, err = InitNode(Config, true, "", false)
	if err.Error() != "[imap.InitNode] Failed to load DISTRIBUTOR TLS cert and key: open ../private/not-existing.cert: no such file or directory\n" {
		t.Fatalf("[imap.TestInitNode] Expected '%s' but received '%s'\n", "[imap.InitNode] Failed to load DISTRIBUTOR TLS cert and key: open ../private/not-existing.cert: no such file or directory\n", err.Error())
	}

	// Set wrong certificate path back to correct one.
	Config.Distributor.TLS.CertLoc = "../private/distributor-cert.pem"

	// Correct distributor initialization.
	Node, err = InitNode(Config, true, "", false)
	Node.Socket.Close()

	// Correct worker initialization.
	Node, err = InitNode(Config, false, "worker-1", false)
	Node.Socket.Close()

	// Correct storage initialization.
	Node, err = InitNode(Config, false, "", true)
	Node.Socket.Close()
}

// TestRunNode executes a white-box unit test on the
// implemented RunNode() and HandleRequest() functions.
func TestRunNode(t *testing.T) {

	// Create needed test environment.
	Config, TLSConfig := utils.CreateTestEnv()

	// Initialize a correct node.
	Node, err := InitNode(Config, true, "", false)
	if err != nil {
		t.Fatalf("[imap.TestRunNode] Error during initializing node with correct config: %s\n", err.Error())
	}
	defer Node.Socket.Close()

	// Start test distributor in background.
	go func() {

		err := Node.RunNode()

		// Define error message that will be sent when connection
		// is closed in a proper way so that we can allow that one.
		okError := fmt.Sprintf("[imap.RunNode] Accepting incoming request failed with: accept tcp %s:%s: use of closed network connection\n", Node.Config.Distributor.IP, Node.Config.Distributor.Port)

		if err.Error() != okError {
			t.Fatalf("[imap.RunNode] Expected '%s' but received '%s'\n", okError, err.Error())
		}
	}()

	// Wait shortly for backend before sending request.
	time.Sleep(time.Millisecond * 100)

	// Connect to IMAP distributor.
	conn, err := tls.Dial("tcp", (Config.Distributor.IP + ":" + Config.Distributor.Port), TLSConfig)
	if err != nil {
		t.Fatalf("[imap.TestRunNode] Error during connection attempt to IMAP distributor: %s\n", err.Error())
	}
	defer conn.Close()

	// Create new connection struct.
	c := NewConnection(conn)

	// Expect to receive IMAP greeting.
	imapGreeting, err := c.Receive()
	if err != nil {
		t.Errorf("[imap.TestRunNode] Error during receiving initial distributor greeting: %s\n", err.Error())
	}

	// Check if greeting is the one we expect.
	if imapGreeting != "* OK IMAP4rev1 Pluto ready." {
		t.Fatalf("[imap.TestRunNode] Expected '%s' but received '%s'\n", "* OK IMAP4rev1 Pluto ready.", imapGreeting)
	}
}
