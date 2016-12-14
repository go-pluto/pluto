package imap_test

import (
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"crypto/tls"

	"github.com/numbleroot/pluto/imap"
	"github.com/numbleroot/pluto/utils"
)

// Structs

var msg1 string = `Date: Mon, 7 Feb 1994 21:52:25 -0800 (PST)
From: Fred Foobar <foobar@Blurdybloop.COM>
Subject: afternoon meeting
To: mooch@owatagu.siam.edu
Message-Id: <B27397-0100000@Blurdybloop.COM>
MIME-Version: 1.0
Content-Type: TEXT/PLAIN; CHARSET=US-ASCII

Hello Joe, do you think we can meet at 3:30 tomorrow?
`

var selectTests = []struct {
	in  string
	out string
}{
	{"a LOGIN user0 password0", "a OK LOGIN completed"},
	{"b SELECT INBOX", "* 0 EXISTS\n* 0 RECENT\n* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\n* OK [PERMANENTFLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)]\nb OK [READ-WRITE] SELECT completed"},
	{"c SELECT", "c BAD Command SELECT was sent without a mailbox to select"},
	{"d SELECT lol rofl nope", "d BAD Command SELECT was sent with multiple mailbox names instead of only one"},
	{"e LOGOUT", "* BYE Terminating connection\ne OK LOGOUT completed"},
}

var createTests = []struct {
	in  string
	out string
}{
	{"a LOGIN user1 password1", "a OK LOGIN completed"},
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
	{"a LOGIN user1 password1", "a OK LOGIN completed"},
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

var listTests = []struct {
	in     string
	outOne string
	outTwo string
}{
	{"a LOGIN user1 password1", "a OK LOGIN completed", "a OK LOGIN completed"},
	{"b LIST \"\" *", "* LIST () \".\" INBOX\n* LIST () \".\" university\nb OK LIST completed", "* LIST () \".\" university\n* LIST () \".\" INBOX\nb OK LIST completed"},
	{"c CREATE university.Modul1", "c OK CREATE completed", "c OK CREATE completed"},
	{"d LIST \"\" %", "* LIST () \".\" INBOX\n* LIST () \".\" university\nd OK LIST completed", "* LIST () \".\" INBOX\n* LIST () \".\" university\nd OK LIST completed"},
	{"e DELETE university", "e OK DELETE completed", "e OK DELETE completed"},
	{"f LIST \"\" *", "* LIST () \".\" INBOX\n* LIST () \".\" university.Modul1\nf OK LIST completed", "* LIST () \".\" university.Modul1\n* LIST () \".\" INBOX\nf OK LIST completed"},
	{"g LOGOUT", "* BYE Terminating connection\ng OK LOGOUT completed", "* BYE Terminating connection\ng OK LOGOUT completed"},
}

var appendTests = []struct {
	in  string
	out string
}{
	{"a LOGIN user2 password2", "a OK LOGIN completed"},
	{"b APPEND DoesNotExist {301}", "b NO [TRYCREATE] Mailbox to append to does not exist"},
	{"c APPEND inbox {301}", "+ Ready for literal data"},
	{msg1, "c OK APPEND completed"},
	{"d APPEND INBOX {301}", "+ Ready for literal data"},
	{msg1, "d OK APPEND completed"},
	{"e CREATE University", "e OK CREATE completed"},
	{"f APPEND University {301}", "+ Ready for literal data"},
	{msg1, "f OK APPEND completed"},
	{"f APPEND university {301}", "f NO [TRYCREATE] Mailbox to append to does not exist"},
	{"z LOGOUT", "* BYE Terminating connection\nz OK LOGOUT completed"},
}

var storeTests = []struct {
	in  string
	out string
}{
	{"a LOGIN user3 password3", "a OK LOGIN completed"},
	{"b CREATE Sports", "b OK CREATE completed"},
	{"c APPEND Sports {301}", "+ Ready for literal data"},
	{msg1, "c OK APPEND completed"},
	{"d APPEND Sports {301}", "+ Ready for literal data"},
	{msg1, "d OK APPEND completed"},
	{"e APPEND Sports {301}", "+ Ready for literal data"},
	{msg1, "e OK APPEND completed"},
	{"f APPEND Sports {301}", "+ Ready for literal data"},
	{msg1, "f OK APPEND completed"},
	{"g APPEND Sports {301}", "+ Ready for literal data"},
	{msg1, "g OK APPEND completed"},
	{"h STORE anything", "h BAD No mailbox selected for store"},
	{"i SELECT Sports", "* 5 EXISTS\n* 5 RECENT\n* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\n* OK [PERMANENTFLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)]\ni OK [READ-WRITE] SELECT completed"},
	{"j STORE too few", "j BAD Command STORE was not sent with three parameters"},
	{"k STORE one,two FLAGS (\\Seen)", "k BAD Command STORE was sent with an invalid number parameter"},
	{"l STORE 2,4:* WHYNOTTHIS? (\\Seen)", "l BAD Unknown data item type specified"},
	{"m STORE 2,4:* -FLAGS.SILENT \\Seen", "m BAD Command STORE was sent with invalid parenthesized flags list"},
	{"n STORE 2,4:* +FLAGS (\\Seen \\Answered)", "* 2 FETCH (FLAGS (\\Answered \\Seen))\n* 4 FETCH (FLAGS (\\Answered \\Seen))\n* 5 FETCH (FLAGS (\\Answered \\Seen))\nn OK STORE completed"},
	{"o STORE 3,2,1 -FLAGS (\\Answered)", "* 1 FETCH (FLAGS ())\n* 2 FETCH (FLAGS (\\Seen))\n* 3 FETCH (FLAGS ())\no OK STORE completed"},
	{"p STORE 1,2,3:* FLAGS.SILENT (\\Draft \\Deleted)", "p OK STORE completed"},
	{"q STORE 5,3,4,1,2 +FLAGS (\\Answered)", "* 1 FETCH (FLAGS (\\Answered \\Draft \\Deleted))\n* 2 FETCH (FLAGS (\\Answered \\Draft \\Deleted))\n* 3 FETCH (FLAGS (\\Answered \\Draft \\Deleted))\n* 4 FETCH (FLAGS (\\Answered \\Draft \\Deleted))\n* 5 FETCH (FLAGS (\\Answered \\Draft \\Deleted))\nq OK STORE completed"},
	{"r LOGOUT", "* BYE Terminating connection\nr OK LOGOUT completed"},
}

var expungeTests = []struct {
	in  string
	out string
}{
	{"a LOGIN user4 password4", "a OK LOGIN completed"},
	{"b CREATE Monday", "b OK CREATE completed"},
	{"c APPEND Monday {301}", "+ Ready for literal data"},
	{msg1, "c OK APPEND completed"},
	{"d APPEND Monday {301}", "+ Ready for literal data"},
	{msg1, "d OK APPEND completed"},
	{"e APPEND Monday {301}", "+ Ready for literal data"},
	{msg1, "e OK APPEND completed"},
	{"f APPEND Monday {301}", "+ Ready for literal data"},
	{msg1, "f OK APPEND completed"},
	{"g APPEND Monday {301}", "+ Ready for literal data"},
	{msg1, "g OK APPEND completed"},
	{"h EXPUNGE", "h BAD No mailbox selected to expunge"},
	{"i SELECT Monday", "* 5 EXISTS\n* 5 RECENT\n* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\n* OK [PERMANENTFLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)]\ni OK [READ-WRITE] SELECT completed"},
	{"j EXPUNGE", "j OK EXPUNGE completed"},
	{"k STORE 1:* +FLAGS (\\Deleted)", "* 1 FETCH (FLAGS (\\Deleted))\n* 2 FETCH (FLAGS (\\Deleted))\n* 3 FETCH (FLAGS (\\Deleted))\n* 4 FETCH (FLAGS (\\Deleted))\n* 5 FETCH (FLAGS (\\Deleted))\nk OK STORE completed"},
	{"l EXPUNGE", "* 5 EXPUNGE\n* 4 EXPUNGE\n* 3 EXPUNGE\n* 2 EXPUNGE\n* 1 EXPUNGE\nl OK EXPUNGE completed"},
	{"m LOGOUT", "* BYE Terminating connection\nm OK LOGOUT completed"},
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

	for _, tt := range selectTests {

		var answer string

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
		if err != nil {
			t.Fatalf("[imap.TestSelect] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive answer to SELECT request.
		firstAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestSelect] Error during receiving table test SELECT: %s\n", err.Error())
		}

		// As long as the IMAP command termination indicator
		// was not yet received, continue append answers.
		for (strings.Contains(firstAnswer, "completed") != true) &&
			(strings.Contains(firstAnswer, "BAD") != true) &&
			(strings.Contains(firstAnswer, "NO") != true) &&
			(strings.Contains(firstAnswer, "+ Ready for literal data") != true) {

			// Receive next line from distributor.
			nextAnswer, err := c.Receive()
			if err != nil {
				t.Errorf("[imap.TestSelect] Error during receiving table test SELECT: %s\n", err.Error())
			}

			firstAnswer = fmt.Sprintf("%s\n%s", firstAnswer, nextAnswer)
		}

		answer = firstAnswer

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

	for _, tt := range createTests {

		var answer string

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
		if err != nil {
			t.Fatalf("[imap.TestCreate] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive answer to CREATE request.
		firstAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestCreate] Error during receiving table test CREATE: %s\n", err.Error())
		}

		// As long as the IMAP command termination indicator
		// was not yet received, continue append answers.
		for (strings.Contains(firstAnswer, "completed") != true) &&
			(strings.Contains(firstAnswer, "BAD") != true) &&
			(strings.Contains(firstAnswer, "NO") != true) &&
			(strings.Contains(firstAnswer, "+ Ready for literal data") != true) {

			// Receive next line from distributor.
			nextAnswer, err := c.Receive()
			if err != nil {
				t.Errorf("[imap.TestCreate] Error during receiving table test CREATE: %s\n", err.Error())
			}

			firstAnswer = fmt.Sprintf("%s\n%s", firstAnswer, nextAnswer)
		}

		answer = firstAnswer

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

	for _, tt := range deleteTests {

		var answer string

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
		if err != nil {
			t.Fatalf("[imap.TestDelete] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive answer to DELETE request.
		firstAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestDelete] Error during receiving table test DELETE: %s\n", err.Error())
		}

		// As long as the IMAP command termination indicator
		// was not yet received, continue append answers.
		for (strings.Contains(firstAnswer, "completed") != true) &&
			(strings.Contains(firstAnswer, "BAD") != true) &&
			(strings.Contains(firstAnswer, "NO") != true) &&
			(strings.Contains(firstAnswer, "+ Ready for literal data") != true) {

			// Receive next line from distributor.
			nextAnswer, err := c.Receive()
			if err != nil {
				t.Errorf("[imap.TestDelete] Error during receiving table test DELETE: %s\n", err.Error())
			}

			firstAnswer = fmt.Sprintf("%s\n%s", firstAnswer, nextAnswer)
		}

		answer = firstAnswer

		if answer != tt.out {
			t.Fatalf("[imap.TestDelete] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()

	time.Sleep(1400 * time.Millisecond)
}

// TestList executes a black-box table test on the
// implemented List() function.
func TestList(t *testing.T) {

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
		t.Fatalf("[imap.TestList] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestList] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for _, tt := range listTests {

		var answer string

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
		if err != nil {
			t.Fatalf("[imap.TestList] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive answer to LIST request.
		firstAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestList] Error during receiving table test LIST: %s\n", err.Error())
		}

		// As long as the IMAP command termination indicator
		// was not yet received, continue append answers.
		for (strings.Contains(firstAnswer, "completed") != true) &&
			(strings.Contains(firstAnswer, "BAD") != true) &&
			(strings.Contains(firstAnswer, "NO") != true) &&
			(strings.Contains(firstAnswer, "+ Ready for literal data") != true) {

			// Receive next line from distributor.
			nextAnswer, err := c.Receive()
			if err != nil {
				t.Errorf("[imap.TestList] Error during receiving table test LIST: %s\n", err.Error())
			}

			firstAnswer = fmt.Sprintf("%s\n%s", firstAnswer, nextAnswer)
		}

		answer = firstAnswer

		if (answer != tt.outOne) && (answer != tt.outTwo) {
			t.Fatalf("[imap.TestList] Expected '%s' or '%s' but received '%s'\n", tt.outOne, tt.outTwo, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()

	time.Sleep(1400 * time.Millisecond)
}

// TestAppend executes a black-box table test on the
// implemented Append() function.
func TestAppend(t *testing.T) {

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
		t.Fatalf("[imap.TestAppend] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestAppend] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for i, tt := range appendTests {

		var answer string

		if (i == 3) || (i == 5) || (i == 8) {

			// Send mail message without additional newline.
			_, err = fmt.Fprintf(c.Conn, "%s", tt.in)
			if err != nil {
				t.Fatalf("[imap.TestAppend] Sending mail message to server failed with: %s\n", err.Error())
			}
		} else {

			// Table test: send 'in' part of each line.
			err = c.Send(tt.in)
			if err != nil {
				t.Fatalf("[imap.TestAppend] Sending message to server failed with: %s\n", err.Error())
			}
		}

		// Receive answer to APPEND request.
		firstAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestAppend] Error during receiving table test APPEND: %s\n", err.Error())
		}

		// As long as the IMAP command termination indicator
		// was not yet received, continue append answers.
		for (strings.Contains(firstAnswer, "completed") != true) &&
			(strings.Contains(firstAnswer, "BAD") != true) &&
			(strings.Contains(firstAnswer, "NO") != true) &&
			(strings.Contains(firstAnswer, "+ Ready for literal data") != true) {

			// Receive next line from distributor.
			nextAnswer, err := c.Receive()
			if err != nil {
				t.Errorf("[imap.TestAppend] Error during receiving table test APPEND: %s\n", err.Error())
			}

			firstAnswer = fmt.Sprintf("%s\n%s", firstAnswer, nextAnswer)
		}

		answer = firstAnswer

		if answer != tt.out {
			t.Fatalf("[imap.TestAppend] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()

	time.Sleep(1400 * time.Millisecond)
}

// TestStore executes a black-box table test on the
// implemented Store() function.
func TestStore(t *testing.T) {

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
		t.Fatalf("[imap.TestStore] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestStore] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for i, tt := range storeTests {

		var answer string

		if (i == 3) || (i == 5) || (i == 7) || (i == 9) || (i == 11) {

			// Send mail message without additional newline.
			_, err = fmt.Fprintf(c.Conn, "%s", tt.in)
			if err != nil {
				t.Fatalf("[imap.TestStore] Sending mail message to server failed with: %s\n", err.Error())
			}
		} else {

			// Table test: send 'in' part of each line.
			err = c.Send(tt.in)
			if err != nil {
				t.Fatalf("[imap.TestStore] Sending message to server failed with: %s\n", err.Error())
			}
		}

		// Receive answer to STORE request.
		firstAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestStore] Error during receiving table test STORE: %s\n", err.Error())
		}

		// As long as the IMAP command termination indicator
		// was not yet received, continue append answers.
		for (strings.Contains(firstAnswer, "completed") != true) &&
			(strings.Contains(firstAnswer, "BAD") != true) &&
			(strings.Contains(firstAnswer, "NO") != true) &&
			(strings.Contains(firstAnswer, "+ Ready for literal data") != true) {

			// Receive next line from distributor.
			nextAnswer, err := c.Receive()
			if err != nil {
				t.Errorf("[imap.TestStore] Error during receiving table test STORE: %s\n", err.Error())
			}

			firstAnswer = fmt.Sprintf("%s\n%s", firstAnswer, nextAnswer)
		}

		answer = firstAnswer

		if answer != tt.out {
			t.Fatalf("[imap.TestStore] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()

	time.Sleep(1400 * time.Millisecond)
}

// TestExpunge executes a black-box table test on the
// implemented Expunge() function.
func TestExpunge(t *testing.T) {

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
		t.Fatalf("[imap.TestExpunge] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestExpunge] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for i, tt := range expungeTests {

		var answer string

		if (i == 3) || (i == 5) || (i == 7) || (i == 9) || (i == 11) {

			// Send mail message without additional newline.
			_, err = fmt.Fprintf(c.Conn, "%s", tt.in)
			if err != nil {
				t.Fatalf("[imap.TestExpunge] Sending mail message to server failed with: %s\n", err.Error())
			}
		} else {

			// Table test: send 'in' part of each line.
			err = c.Send(tt.in)
			if err != nil {
				t.Fatalf("[imap.TestExpunge] Sending message to server failed with: %s\n", err.Error())
			}
		}

		// Receive answer to EXPUNGE request.
		firstAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestExpunge] Error during receiving table test EXPUNGE: %s\n", err.Error())
		}

		// As long as the IMAP command termination indicator
		// was not yet received, continue append answers.
		for (strings.Contains(firstAnswer, "completed") != true) &&
			(strings.Contains(firstAnswer, "BAD") != true) &&
			(strings.Contains(firstAnswer, "NO") != true) &&
			(strings.Contains(firstAnswer, "+ Ready for literal data") != true) {

			// Receive next line from distributor.
			nextAnswer, err := c.Receive()
			if err != nil {
				t.Errorf("[imap.TestExpunge] Error during receiving table test EXPUNGE: %s\n", err.Error())
			}

			firstAnswer = fmt.Sprintf("%s\n%s", firstAnswer, nextAnswer)
		}

		answer = firstAnswer

		if answer != tt.out {
			t.Fatalf("[imap.TestExpunge] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()

	time.Sleep(1400 * time.Millisecond)
}
