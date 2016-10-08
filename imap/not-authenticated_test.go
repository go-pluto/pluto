package imap_test

import (
	"testing"

	"crypto/tls"

	"github.com/numbleroot/pluto/imap"
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

	// Connect to IMAP server.
	conn, err := tls.Dial("tcp", (Config.IP + ":" + Config.Port), TLSConfig)
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
