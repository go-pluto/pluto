package imap_test

import (
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/imap"
	"github.com/numbleroot/pluto/server"
)

// Structs

var capabilityTests = []struct {
	in  string
	out string
}{
	{"a001 CAPABILITY", "* CAPABILITY IMAP4rev1 STARTTLS LOGINDISABLED\na001 OK CAPABILITY completed"},
	{"a001 CAPABILITY   ", "* BAD Command CAPABILITY was sent with extra space"},
	{"CAPABILITY", "* BAD Received invalid IMAP command"},
}

// Variables

var Config *config.Config
var Server *server.Server

// Functions

func TestMain(m *testing.M) {

	// Read configuration from file.
	Config = config.LoadConfig("../config.toml")

	// Initialize a server instance.
	Server = server.InitServer(Config.IP, Config.Port)

	// Start test server in background.
	go Server.RunServer(Config.Server.Greeting)

	// Start main tests.
	os.Exit(m.Run())
}

// TestCapability executes a black-box table test on the
// implemented Capability() function.
func TestCapability(t *testing.T) {

	// Connect to IMAP server.
	conn, err := net.Dial("tcp", (Config.IP + ":" + Config.Port))
	if err != nil {
		t.Fatalf("[imap.TestCapability] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestCapability] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for i, tt := range capabilityTests {

		var answer string

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
		if err != nil {
			t.Fatalf("[imap.TestCapability] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive options listed in CAPABILITY command.
		capAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestCapability] Error during receiving table test CAPABILITY: %s\n", err.Error())
		}

		if i == 0 {

			// Receive command termination message from server.
			okAnswer, err := c.Receive()
			if err != nil {
				t.Errorf("[imap.TestCapability] Error during receiving table test CAPABILITY: %s\n", err.Error())
			}

			answer = fmt.Sprintf("%s\n%s", capAnswer, okAnswer)
		} else {
			answer = capAnswer
		}

		if answer != tt.out {
			t.Fatalf("[imap.TestCapability] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}
}

// TestLogout executes a black-box table test on the
// implemented Logout() function.
func TestLogout(t *testing.T) {

	// TODO: Implement this function.
}
