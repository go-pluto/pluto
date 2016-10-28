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

var capabilityTests = []struct {
	in  string
	out string
}{
	{"a001 CAPABILITY", "* CAPABILITY IMAP4rev1 AUTH=PLAIN\na001 OK CAPABILITY completed"},
	{"1337 capability", "* CAPABILITY IMAP4rev1 AUTH=PLAIN\n1337 OK CAPABILITY completed"},
	{"tag CAPABILITY   ", "tag BAD Command CAPABILITY was sent with extra parameters"},
	{"CAPABILITY", "* BAD Received invalid IMAP command"},
}

var loginTests = []struct {
	in  string
	out string
}{
	{"blub LOGIN TestUser TestPassword", "blub OK Logged in"},
	{"blargh login TestUser TestPassword", "blargh OK Logged in"},
	{"xyz LOGIN smith sesame", "xyz NO Name and / or password wrong"},
	{"zyx login smith sesame", "zyx NO Name and / or password wrong"},
	{"a1b2c3   LOGIN    smith sesame", "a1b2c3 BAD Received invalid IMAP command"},
	{"LOGIN ernie bert", "* BAD Received invalid IMAP command"},
	{"12345 LOL ernie bert", "12345 BAD Received invalid IMAP command"},
	{"uuu LOGIN let me in please", "uuu BAD Command LOGIN was not sent with exactly two parameters"},
}

var logoutTests = []struct {
	in  string
	out string
}{
	{"iiqqee LOGOUT", "* BYE Terminating connection\niiqqee OK LOGOUT completed"},
	{"lol logout", "* BYE Terminating connection\nlol OK LOGOUT completed"},
	{"5   LOGOUT    ", "5 BAD Received invalid IMAP command"},
	{"LOGOUT some more parameters", "* BAD Received invalid IMAP command"},
	{"b01 LOGOUT some more parameters", "b01 BAD Command LOGOUT was sent with extra parameters"},
}

// Functions

// TestCapability executes a black-box table test on the
// implemented Capability() function.
func TestCapability(t *testing.T) {

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

	// Start test distributor in background.
	go func() {

		err := Node.RunNode()

		// Define error message that will be sent when connection
		// is closed in a proper way so that we can allow that one.
		okError := fmt.Sprintf("[imap.RunNode] Accepting incoming request failed with: accept tcp %s:%s: use of closed network connection\n", Node.Config.Distributor.IP, Node.Config.Distributor.Port)

		if err.Error() != okError {
			t.Fatalf("[imap.TestCapability] Expected '%s' but received '%s'\n", okError, err.Error())
		}
	}()

	// Connect to IMAP distributor.
	conn, err := tls.Dial("tcp", (Config.Distributor.IP + ":" + Config.Distributor.Port), TLSConfig)
	if err != nil {
		t.Fatalf("[imap.TestCapability] Error during connection attempt to IMAP distributor: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestCapability] Error during receiving initial distributor greeting: %s\n", err.Error())
	}

	for i, tt := range capabilityTests {

		var answer string

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
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

			answer = fmt.Sprintf("%s\n%s", capAnswer, okAnswer)
		} else {
			answer = capAnswer
		}

		if answer != tt.out {
			t.Fatalf("[imap.TestCapability] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	Node.Terminate(c)

	Node.Socket.Close()
}

// TestLogin executes a black-box table test on the
// implemented Login() function.
func TestLogin(t *testing.T) {

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
			t.Fatalf("[imap.TestLogin] Expected '%s' but received '%s'\n", okError, err.Error())
		}
	}()

	// Connect to IMAP distributor.
	conn, err := tls.Dial("tcp", (Config.Distributor.IP + ":" + Config.Distributor.Port), TLSConfig)
	if err != nil {
		t.Fatalf("[imap.TestLogin] Error during connection attempt to IMAP distributor: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestLogin] Error during receiving initial distributor greeting: %s\n", err.Error())
	}

	for _, tt := range loginTests {

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
		if err != nil {
			t.Fatalf("[imap.TestLogin] Sending message to distributor failed with: %s\n", err.Error())
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

	// At the end of each test, terminate connection.
	Node.Terminate(c)
}

// TestLogout executes a black-box table test on the
// implemented Logout() function.
func TestLogout(t *testing.T) {

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
			t.Fatalf("[imap.TestLogout] Expected '%s' but received '%s'\n", okError, err.Error())
		}
	}()

	for i, tt := range logoutTests {

		var answer string

		// Connect to IMAP distributor.
		conn, err := tls.Dial("tcp", (Config.Distributor.IP + ":" + Config.Distributor.Port), TLSConfig)
		if err != nil {
			t.Fatalf("[imap.TestLogout] Error during connection attempt to IMAP distributor: %s\n", err.Error())
		}

		// Create new connection struct.
		c := imap.NewConnection(conn)

		// Consume mandatory IMAP greeting.
		_, err = c.Receive()
		if err != nil {
			t.Errorf("[imap.TestLogout] Error during receiving initial distributor greeting: %s\n", err.Error())
		}

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
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

			answer = fmt.Sprintf("%s\n%s", logoutAnswer, okAnswer)
		} else {
			answer = logoutAnswer
		}

		if answer != tt.out {
			t.Fatalf("[imap.TestLogout] Expected '%s' but received '%s'\n", tt.out, answer)
		}

		// At the end of each test, terminate connection.
		err = Node.Terminate(c)
		if err != nil {
			log.Fatal(err)
		}
	}
}
