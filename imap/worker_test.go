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

// TODO: Correctly implement the tests.
//       Includes behaviour further down.
var selectTests = []struct {
	in  string
	out string
}{}

// Functions

// TestSelect executes a black-box table test on the
// implemented Select() function.
func TestSelect(t *testing.T) {

	// Create needed test environment.
	Config, TLSConfig, err := utils.CreateTestEnv()
	if err != nil {
		log.Fatal(err)
	}

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
			t.Fatalf("[imap.TestSelect] Expected '%s' but received '%s'\n", okError, err.Error())
		}
	}()

	// Connect to IMAP server.
	conn, err := tls.Dial("tcp", (Config.Distributor.IP + ":" + Config.Distributor.Port), TLSConfig)
	if err != nil {
		t.Fatalf("[imap.TestSelect] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestSelect] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for _, tt := range selectTests {

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
		if err != nil {
			t.Fatalf("[imap.TestSelect] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive answer to SELECT request.
		answer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestSelect] Error during receiving table test LOGIN: %s\n", err.Error())
		}

		if answer != tt.out {
			t.Fatalf("[imap.TestSelect] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()
}
