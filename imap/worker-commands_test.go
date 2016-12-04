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

// TODO: Correctly implement the tests.
//       Includes behaviour further down.
var selectTests = []struct {
	in  string
	out string
}{
	{"a LOGIN user1 password1", "a OK Logged in"},
	{"b SELECT INBOX", "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\n* OK [PERMANENTFLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)]"},
	{"c SELECT", "c BAD Command SELECT was sent without a mailbox to select"},
	{"d SELECT lol rofl nope", "d BAD Command SELECT was sent with multiple mailbox names instead of only one"},
	{"e LOGOUT", "* BYE Terminating connection\ne OK LOGOUT completed"},
}

var createTests = []struct {
	in  string
	out string
}{
	{"a LOGIN user2 password2", "a OK Logged in"},
	{"b CREATE University", "b OK CREATE completed"},
	{"z LOGOUT", "* BYE Terminating connection\nz OK LOGOUT completed"},
}

// Functions

// TestSelect executes a black-box table test on the
// implemented Select() function.
func TestSelect(t *testing.T) {

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
		t.Fatalf("[imap.TestSelect] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestSelect] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for i, tt := range selectTests {

		var answer string

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
		if err != nil {
			t.Fatalf("[imap.TestSelect] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive answer to SELECT request.
		selectAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestSelect] Error during receiving table test SELECT: %s\n", err.Error())
		}

		if (i == 1) || (i == (len(selectTests) - 1)) {

			// Receive command termination message from distributor.
			okAnswer, err := c.Receive()
			if err != nil {
				t.Errorf("[imap.TestSelect] Error during receiving table test SELECT: %s\n", err.Error())
			}

			answer = fmt.Sprintf("%s\n%s", selectAnswer, okAnswer)
		} else {
			answer = selectAnswer
		}

		if answer != tt.out {
			t.Fatalf("[imap.TestSelect] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()

	time.Sleep(1200 * time.Millisecond)
}

// TestCreate executes a black-box table test on the
// implemented Create() function.
func TestCreate(t *testing.T) {

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
		t.Fatalf("[imap.TestCreate] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestCreate] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for i, tt := range createTests {

		var answer string

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
		if err != nil {
			t.Fatalf("[imap.TestCreate] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive answer to CREATE request.
		createAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestCreate] Error during receiving table test CREATE: %s\n", err.Error())
		}

		if i == (len(createTests) - 1) {

			// Receive command termination message from distributor.
			okAnswer, err := c.Receive()
			if err != nil {
				t.Errorf("[imap.TestCreate] Error during receiving table test CREATE: %s\n", err.Error())
			}

			answer = fmt.Sprintf("%s\n%s", createAnswer, okAnswer)
		} else {
			answer = createAnswer
		}

		if answer != tt.out {
			t.Fatalf("[imap.TestCreate] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()

	time.Sleep(1200 * time.Millisecond)
}
