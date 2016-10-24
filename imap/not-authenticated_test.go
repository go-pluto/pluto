package imap_test

import (
	"fmt"
	"log"
	"testing"

	"crypto/tls"

	"github.com/numbleroot/pluto/imap"
	"github.com/numbleroot/pluto/utils"
)

// Structs

var starttlsTests = []struct {
	in  string
	out string
}{
	{"yyy STARTTLS", "yyy BAD TLS is already active"},
	{"qwerty starttls", "qwerty BAD TLS is already active"},
	{"1 STARTTLS   ", "1 BAD Command STARTTLS was sent with extra parameters"},
	{"STARTTLS", "* BAD Received invalid IMAP command"},
}

// Functions

// TestStartTLS executes a black-box table test on the
// implemented StartTLS() function.
func TestStartTLS(t *testing.T) {

	// Create needed test environment.
	Config, TLSConfig := utils.CreateTestEnv()

	// Initialize a distributor node.
	Node, err := imap.InitNode(Config, true, "", false)
	if err != nil {
		log.Fatal(err)
	}
	defer Node.Socket.Close()

	// Start test distributor in background.
	go func() {

		err := Node.RunNode()

		// Define error message that will be sent when connection
		// is closed in a proper way so that we can allow that one.
		okError := fmt.Sprintf("[imap.RunNode] Accepting incoming request failed with: accept tcp %s:%s: use of closed network connection\n", Node.Config.Distributor.IP, Node.Config.Distributor.Port)

		if err.Error() != okError {
			t.Fatalf("[imap.TestStartTLS] Expected '%s' but received '%s'\n", okError, err.Error())
		}
	}()

	// Connect to IMAP server.
	conn, err := tls.Dial("tcp", (Config.Distributor.IP + ":" + Config.Distributor.Port), TLSConfig)
	if err != nil {
		t.Fatalf("[imap.TestStartTLS] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestStartTLS] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for _, tt := range starttlsTests {

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
		if err != nil {
			t.Fatalf("[imap.TestStartTLS] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive go ahead signal for TLS negotiation.
		answer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestStartTLS] Error during receiving table test LOGIN: %s\n", err.Error())
		}

		if answer != tt.out {
			t.Fatalf("[imap.TestStartTLS] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()
}
