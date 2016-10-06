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
	{"a001 CAPABILITY   ", "a001 BAD Command CAPABILITY was sent with extra parameters"},
	{"CAPABILITY", "* BAD Received invalid IMAP command"},
}

var loginTests = []struct {
	in  string
	out string
}{
	{"xyz LOGIN smith sesame", "xyz NO Command LOGIN is disabled. Do not send plaintext login information."},
	{"a1b2c3   LOGIN    smith sesame", "a1b2c3 BAD Received invalid IMAP command"},
	{"LOGIN ernie bert", "* BAD Received invalid IMAP command"},
	{"12345 LOL ernie bert", "12345 BAD Received invalid IMAP command"},
}

var logoutTests = []struct {
	in  string
	out string
}{
	{"iiqqee LOGOUT", "* BYE Terminating connection\niiqqee OK LOGOUT completed"},
	{"5   LOGOUT    ", "5 BAD Received invalid IMAP command"},
	{"LOGOUT some more parameters", "* BAD Received invalid IMAP command"},
	{"b01 LOGOUT some more parameters", "b01 BAD Command LOGOUT was sent with extra parameters"},
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

// TestLogin executes a black-box table test on the
// implemented Login() function.
func TestLogin(t *testing.T) {

	// Connect to IMAP server.
	conn, err := net.Dial("tcp", (Config.IP + ":" + Config.Port))
	if err != nil {
		t.Fatalf("[imap.TestLogin] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestLogin] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for _, tt := range loginTests {

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
		if err != nil {
			t.Fatalf("[imap.TestLogin] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive LOGINDISABLED answer.
		answer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestLogin] Error during receiving table test LOGIN: %s\n", err.Error())
		}

		if answer != tt.out {
			t.Fatalf("[imap.TestLogin] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}
}

// TestLogout executes a black-box table test on the
// implemented Logout() function.
func TestLogout(t *testing.T) {

	// Connect to IMAP server.
	conn, err := net.Dial("tcp", (Config.IP + ":" + Config.Port))
	if err != nil {
		t.Fatalf("[imap.TestLogout] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestLogout] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for i, tt := range logoutTests {

		var answer string

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
		if err != nil {
			t.Fatalf("[imap.TestLogout] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive logout reponse.
		logoutAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestLogout] Error during receiving table test LOGOUT: %s\n", err.Error())
		}

		if i == 0 {

			// Receive command termination message from server.
			okAnswer, err := c.Receive()
			if err != nil {
				t.Errorf("[imap.TestLogout] Error during receiving table test LOGOUT: %s\n", err.Error())
			}

			answer = fmt.Sprintf("%s\n%s", logoutAnswer, okAnswer)
		} else {
			answer = logoutAnswer
		}

		if answer != tt.out {
			t.Fatalf("[imap.TestLogout] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}
}
