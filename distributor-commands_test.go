package main

/*
import (
	"bufio"
	"fmt"
	stdlog "log"
	"testing"

	"crypto/tls"

	"github.com/numbleroot/pluto/imap"
	"github.com/numbleroot/pluto/utils"
)

// Variables

var testEnv *utils.TestEnv

// Structs

var capabilityTests = []struct {
	in  string
	out string
}{
	{"a CAPABILITY", "* CAPABILITY IMAP4rev1 AUTH=PLAIN\r\na OK CAPABILITY completed"},
	{"b capability", "* CAPABILITY IMAP4rev1 AUTH=PLAIN\r\nb OK CAPABILITY completed"},
	{"c CAPABILITY   ", "c BAD Command CAPABILITY was sent with extra parameters"},
	{"CAPABILITY", "* BAD Received invalid IMAP command"},
}

var logoutTests = []struct {
	in  string
	out string
}{
	{"a LOGOUT", "* BYE Terminating connection\r\na OK LOGOUT completed"},
	{"b logout", "* BYE Terminating connection\r\nb OK LOGOUT completed"},
	{"c   LOGOUT    ", "c BAD Received invalid IMAP command"},
	{"LOGOUT some more parameters", "* BAD Received invalid IMAP command"},
	{"d LOGOUT some more parameters", "d BAD Command LOGOUT was sent with extra parameters"},
}

var starttlsTests = []struct {
	in  string
	out string
}{
	{"a STARTTLS", "a BAD TLS is already active"},
	{"b starttls", "b BAD TLS is already active"},
	{"c STARTTLS   ", "c BAD Command STARTTLS was sent with extra parameters"},
	{"STARTTLS", "* BAD Received invalid IMAP command"},
}

var loginTests = []struct {
	in  string
	out string
}{
	{"a LOGIN smith sesame", "a NO Name and / or password wrong"},
	{"b login smith sesame", "b NO Name and / or password wrong"},
	{"c   LOGIN    user3 password3", "c BAD Received invalid IMAP command"},
	{"LOGIN ernie bert", "* BAD Received invalid IMAP command"},
	{"d LOL ernie bert", "d BAD Received invalid IMAP command"},
	{"e LOGIN let me in please", "e BAD Command LOGIN was not sent with exactly two parameters"},
	{"f LOGIN user1 password1", "f OK LOGIN completed"},
	{"g LOGIN user1 password1", "g BAD Command LOGIN cannot be executed in this state"},
	{"h LOGIN user1 password1", "h BAD Command LOGIN cannot be executed in this state"},
}

// Functions

// TestCapability executes a black-box table test on the
// implemented Capability() function.
func TestCapability(t *testing.T) {

	// Connect to IMAP distributor.
	conn, err := tls.Dial("tcp", testEnv.Addr, testEnv.TLSConfig)
	if err != nil {
		t.Fatalf("[imap.TestCapability] Error during connection attempt to IMAP distributor: %s\n", err.Error())
	}

	// Create new connection struct.
	c := &imap.Connection{
		OutConn:   conn,
		OutReader: bufio.NewReader(conn),
	}

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestCapability] Error during receiving initial distributor greeting: %s\n", err.Error())
	}

	for i, tt := range capabilityTests {

		var answer string

		// Table test: send 'in' part of each line.
		err = c.Send(false, tt.in)
		if err != nil {
			t.Fatalf("[imap.TestCapability] Sending message to distributor failed with: %s\n", err.Error())
		}

		// Receive options listed in CAPABILITY command.
		capAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestCapability] Error during receiving table test CAPABILITY: %s\n", err.Error())
		}

		if i < 2 {

			// Receive command termination message from distributor.
			okAnswer, err := c.Receive()
			if err != nil {
				t.Errorf("[imap.TestCapability] Error during receiving table test CAPABILITY: %s\n", err.Error())
			}

			answer = fmt.Sprintf("%s\r\n%s", capAnswer, okAnswer)
		} else {
			answer = capAnswer
		}

		if answer != tt.out {
			t.Fatalf("[imap.TestCapability] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.IncConn.Close()
}

// TestLogout executes a black-box table test on the
// implemented Logout() function.
func TestLogout(t *testing.T) {

	for i, tt := range logoutTests {

		var answer string

		// Connect to IMAP distributor.
		conn, err := tls.Dial("tcp", testEnv.Addr, testEnv.TLSConfig)
		if err != nil {
			t.Fatalf("[imap.TestLogout] Error during connection attempt to IMAP distributor: %s\n", err.Error())
		}

		// Create new connection struct.
		c := &imap.Connection{
			OutConn:   conn,
			OutReader: bufio.NewReader(conn),
		}

		// Consume mandatory IMAP greeting.
		_, err = c.Receive()
		if err != nil {
			t.Errorf("[imap.TestLogout] Error during receiving initial distributor greeting: %s\n", err.Error())
		}

		// Table test: send 'in' part of each line.
		err = c.Send(false, tt.in)
		if err != nil {
			t.Fatalf("[imap.TestLogout] Sending message to distributor failed with: %s\n", err.Error())
		}

		// Receive logout response.
		logoutAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestLogout] Error during receiving table test LOGOUT: %s\n", err.Error())
		}

		if i < 2 {

			// Receive command termination message from distributor.
			okAnswer, err := c.Receive()
			if err != nil {
				t.Errorf("[imap.TestLogout] Error during receiving table test LOGOUT: %s\n", err.Error())
			}

			answer = fmt.Sprintf("%s\r\n%s", logoutAnswer, okAnswer)
		} else {
			answer = logoutAnswer
		}

		if answer != tt.out {
			t.Fatalf("[imap.TestLogout] Expected '%s' but received '%s'\n", tt.out, answer)
		}

		// At the end of each test, terminate connection.
		err = c.IncConn.Close()
		if err != nil {
			stdlog.Fatal(err)
		}
	}
}

// TestStartTLS executes a black-box table test on the
// implemented StartTLS() function.
func TestStartTLS(t *testing.T) {

	// Connect to IMAP server.
	conn, err := tls.Dial("tcp", testEnv.Addr, testEnv.TLSConfig)
	if err != nil {
		t.Fatalf("[imap.TestStartTLS] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := &imap.Connection{
		OutConn:   conn,
		OutReader: bufio.NewReader(conn),
	}

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestStartTLS] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for _, tt := range starttlsTests {

		// Table test: send 'in' part of each line.
		err = c.Send(false, tt.in)
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
	c.IncConn.Close()
}

// TestLogin executes a black-box table test on the
// implemented Login() function.
func TestLogin(t *testing.T) {

	// Connect to IMAP distributor.
	conn, err := tls.Dial("tcp", testEnv.Addr, testEnv.TLSConfig)
	if err != nil {
		t.Fatalf("[imap.TestLogin] Error during connection attempt to IMAP distributor: %s\n", err.Error())
	}

	// Create new connection struct.
	c := &imap.Connection{
		OutConn:   conn,
		OutReader: bufio.NewReader(conn),
	}

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestLogin] Error during receiving initial distributor greeting: %s\n", err.Error())
	}

	for _, tt := range loginTests {

		// Table test: send 'in' part of each line.
		err = c.Send(false, tt.in)
		if err != nil {
			t.Fatalf("[imap.TestLogin] Sending message to distributor failed with: %s\n", err.Error())
		}

		// Receive successful LOGIN message.
		answer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestLogin] Error during receiving table test LOGIN: %s\n", err.Error())
		}

		if answer != tt.out {
			t.Fatalf("[imap.TestLogin] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.IncConn.Close()
}
*/
