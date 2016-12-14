package imap_test

import (
	"fmt"
	"log"
	"testing"
	"time"

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
	{"blub1 LOGIN user1 password1", "blub1 OK LOGIN completed"},
	{"blub2 LOGIN user1 password1", "blub2 OK LOGIN completed"},
	{"blub3 LOGIN user1 password1", "blub3 OK LOGIN completed"},
	{"blargh login user2 password2", "blargh OK LOGIN completed"},
	{"xyz LOGIN smith sesame", "xyz NO Name and / or password wrong"},
	{"zyx login smith sesame", "zyx NO Name and / or password wrong"},
	{"a1b2c3   LOGIN    user3 password3", "a1b2c3 BAD Received invalid IMAP command"},
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

// TestCapability executes a black-box table test on the
// implemented Capability() function.
func TestCapability(t *testing.T) {

	// Create needed test environment.
	config, tlsConfig, err := utils.CreateTestEnv()
	if err != nil {
		log.Fatal(err)
	}

	// Start a storage node in background.
	go utils.RunStorageWithTimeout(config, 2200)
	time.Sleep(400 * time.Millisecond)

	// Start a worker node in background.
	go utils.RunWorkerWithTimeout(config, "worker-1", 1800)
	time.Sleep(400 * time.Millisecond)

	// Start a distributor node in background.
	go utils.RunDistributorWithTimeout(config, 1400)
	time.Sleep(400 * time.Millisecond)

	// Connect to IMAP distributor.
	conn, err := tls.Dial("tcp", (config.Distributor.IP + ":" + config.Distributor.Port), tlsConfig)
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
	c.Terminate()

	time.Sleep(1200 * time.Millisecond)
}

// TestLogin executes a black-box table test on the
// implemented Login() function.
func TestLogin(t *testing.T) {

	time.Sleep(1 * time.Second)

	// Create needed test environment.
	config, tlsConfig, err := utils.CreateTestEnv()
	if err != nil {
		log.Fatal(err)
	}

	// Start a storage node in background.
	go utils.RunStorageWithTimeout(config, 2200)
	time.Sleep(400 * time.Millisecond)

	// Start a worker node in background.
	go utils.RunWorkerWithTimeout(config, "worker-1", 1800)
	time.Sleep(400 * time.Millisecond)

	// Start a distributor node in background.
	go utils.RunDistributorWithTimeout(config, 1400)
	time.Sleep(400 * time.Millisecond)

	// Connect to IMAP distributor.
	conn, err := tls.Dial("tcp", (config.Distributor.IP + ":" + config.Distributor.Port), tlsConfig)
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
	c.Terminate()

	time.Sleep(1200 * time.Millisecond)
}

// TestLogout executes a black-box table test on the
// implemented Logout() function.
func TestLogout(t *testing.T) {

	// Create needed test environment.
	config, tlsConfig, err := utils.CreateTestEnv()
	if err != nil {
		log.Fatal(err)
	}

	// Start a storage node in background.
	go utils.RunStorageWithTimeout(config, 2200)
	time.Sleep(400 * time.Millisecond)

	// Start a worker node in background.
	go utils.RunWorkerWithTimeout(config, "worker-1", 1800)
	time.Sleep(400 * time.Millisecond)

	// Start a distributor node in background.
	go utils.RunDistributorWithTimeout(config, 1400)
	time.Sleep(400 * time.Millisecond)

	for i, tt := range logoutTests {

		var answer string

		// Connect to IMAP distributor.
		conn, err := tls.Dial("tcp", (config.Distributor.IP + ":" + config.Distributor.Port), tlsConfig)
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
		err = c.Terminate()
		if err != nil {
			log.Fatal(err)
		}
	}

	time.Sleep(1200 * time.Millisecond)
}

// TestStartTLS executes a black-box table test on the
// implemented StartTLS() function.
func TestStartTLS(t *testing.T) {

	// Create needed test environment.
	config, tlsConfig, err := utils.CreateTestEnv()
	if err != nil {
		log.Fatal(err)
	}

	// Start a storage node in background.
	go utils.RunStorageWithTimeout(config, 2200)
	time.Sleep(400 * time.Millisecond)

	// Start a worker node in background.
	go utils.RunWorkerWithTimeout(config, "worker-1", 1800)
	time.Sleep(400 * time.Millisecond)

	// Start a distributor node in background.
	go utils.RunDistributorWithTimeout(config, 1400)
	time.Sleep(400 * time.Millisecond)

	// Connect to IMAP server.
	conn, err := tls.Dial("tcp", (config.Distributor.IP + ":" + config.Distributor.Port), tlsConfig)
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

	time.Sleep(1200 * time.Millisecond)
}
