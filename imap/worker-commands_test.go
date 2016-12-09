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

var selectTests = []struct {
	in  string
	out string
}{
	{"a LOGIN user0 password0", "a OK Logged in"},
	{"b SELECT INBOX", "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\n* OK [PERMANENTFLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)]"},
	{"c SELECT", "c BAD Command SELECT was sent without a mailbox to select"},
	{"d SELECT lol rofl nope", "d BAD Command SELECT was sent with multiple mailbox names instead of only one"},
	{"e LOGOUT", "* BYE Terminating connection\ne OK LOGOUT completed"},
}

var createTests = []struct {
	in  string
	out string
}{
	{"a LOGIN user1 password1", "a OK Logged in"},
	{"b CREATE mailbox1 mailbox2", "b BAD Command CREATE was not sent with exactly one parameter"},
	{"c CREATE INBOX.", "c NO New mailbox cannot be named INBOX"},
	{"d CREATE INBOX", "d NO New mailbox cannot be named INBOX"},
	{"e CREATE inbox", "e NO New mailbox cannot be named INBOX"},
	{"f CREATE inBOx", "f NO New mailbox cannot be named INBOX"},
	{"g CREATE University.", "g OK CREATE completed"},
	{"h CREATE University.", "h NO New mailbox cannot be named after already existing mailbox"},
	{"i CREATE University", "i NO New mailbox cannot be named after already existing mailbox"},
	{"j CREATE university", "j OK CREATE completed"},
	{"k CREATE University.Thesis", "k OK CREATE completed"},
	{"l CREATE University.Thesis.", "l NO New mailbox cannot be named after already existing mailbox"},
	{"m LOGOUT", "* BYE Terminating connection\nm OK LOGOUT completed"},
}

var deleteTests = []struct {
	in  string
	out string
}{
	{"a LOGIN user1 password1", "a OK Logged in"},
	{"b DELETE INBOX.", "b NO Forbidden to delete INBOX"},
	{"c DELETE INBOX", "c NO Forbidden to delete INBOX"},
	{"d DELETE inbox", "d NO Forbidden to delete INBOX"},
	{"e DELETE inBOx", "e NO Forbidden to delete INBOX"},
	{"f DELETE DoesNotExist.", "f NO Cannot delete folder that does not exist"},
	{"g DELETE DoesNotExist", "g NO Cannot delete folder that does not exist"},
	{"h DELETE University.Thesis.", "h OK DELETE completed"},
	{"i DELETE University.Thesis", "i NO Cannot delete folder that does not exist"},
	{"j DELETE University.", "j OK DELETE completed"},
	{"k DELETE University", "k NO Cannot delete folder that does not exist"},
	{"l LOGOUT", "* BYE Terminating connection\nl OK LOGOUT completed"},
}

/*
var renameTests = []struct {
	in  string
	out string
}{
	{"a LOGIN user1 password1", "a OK Logged in"},
	{"l LOGOUT", "* BYE Terminating connection\nl OK LOGOUT completed"},
}
*/

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

	time.Sleep(1400 * time.Millisecond)
}

// TestDelete executes a black-box table test on the
// implemented Delete() function.
func TestDelete(t *testing.T) {

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
		t.Fatalf("[imap.TestDelete] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestDelete] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for i, tt := range deleteTests {

		var answer string

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
		if err != nil {
			t.Fatalf("[imap.TestDelete] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive answer to DELETE request.
		deleteAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestDelete] Error during receiving table test DELETE: %s\n", err.Error())
		}

		if i == (len(deleteTests) - 1) {

			// Receive command termination message from distributor.
			okAnswer, err := c.Receive()
			if err != nil {
				t.Errorf("[imap.TestDelete] Error during receiving table test DELETE: %s\n", err.Error())
			}

			answer = fmt.Sprintf("%s\n%s", deleteAnswer, okAnswer)
		} else {
			answer = deleteAnswer
		}

		if answer != tt.out {
			t.Fatalf("[imap.TestDelete] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()

	time.Sleep(1400 * time.Millisecond)
}

/*
// TestRename executes a black-box table test on the
// implemented Rename() function.
func TestRename(t *testing.T) {

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
		t.Fatalf("[imap.TestRename] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestRename] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for i, tt := range renameTests {

		var answer string

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
		if err != nil {
			t.Fatalf("[imap.TestRename] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive answer to RENAME request.
		renameAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestRename] Error during receiving table test RENAME: %s\n", err.Error())
		}

		if i == (len(renameTests) - 1) {

			// Receive command termination message from distributor.
			okAnswer, err := c.Receive()
			if err != nil {
				t.Errorf("[imap.TestRename] Error during receiving table test RENAME: %s\n", err.Error())
			}

			answer = fmt.Sprintf("%s\n%s", renameAnswer, okAnswer)
		} else {
			answer = renameAnswer
		}

		if answer != tt.out {
			t.Fatalf("[imap.TestRename] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()

	time.Sleep(1400 * time.Millisecond)
}
*/
