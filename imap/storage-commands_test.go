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

var proxiedSelectTests = []struct {
	in  string
	out string
}{
	{"a LOGIN user5 password5", "a OK LOGIN completed"},
	{"b SELECT INBOX", "* 0 EXISTS\n* 0 RECENT\n* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\n* OK [PERMANENTFLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)]\nb OK [READ-WRITE] SELECT completed"},
	{"c SELECT", "c BAD Command SELECT was sent without a mailbox to select"},
	{"d SELECT lol rofl nope", "d BAD Command SELECT was sent with multiple mailbox names instead of only one"},
	{"e LOGOUT", "* BYE Terminating connection\ne OK LOGOUT completed"},
}

var proxiedCreateTests = []struct {
	in  string
	out string
}{
	{"a LOGIN user6 password6", "a OK LOGIN completed"},
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

var proxiedDeleteTests = []struct {
	in  string
	out string
}{
	{"a LOGIN user6 password6", "a OK LOGIN completed"},
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

var proxiedListTests = []struct {
	in     string
	outOne string
	outTwo string
}{
	{"a LOGIN user6 password6", "a OK LOGIN completed", "a OK LOGIN completed"},
	{"b LIST \"\" *", "* LIST () \".\" INBOX\n* LIST () \".\" university\nb OK LIST completed", "* LIST () \".\" university\n* LIST () \".\" INBOX\nb OK LIST completed"},
	{"c CREATE university.Modul1", "c OK CREATE completed", "c OK CREATE completed"},
	{"d LIST \"\" %", "* LIST () \".\" INBOX\n* LIST () \".\" university\nd OK LIST completed", "* LIST () \".\" university\n* LIST () \".\" INBOX\nd OK LIST completed"},
	{"e DELETE university", "e OK DELETE completed", "e OK DELETE completed"},
	{"f LIST \"\" *", "* LIST () \".\" INBOX\n* LIST () \".\" university.Modul1\nf OK LIST completed", "* LIST () \".\" university.Modul1\n* LIST () \".\" INBOX\nf OK LIST completed"},
	{"g LOGOUT", "* BYE Terminating connection\ng OK LOGOUT completed", "* BYE Terminating connection\ng OK LOGOUT completed"},
}

var proxiedAppendTests = []struct {
	in  string
	out string
}{
	{"a LOGIN user7 password7", "a OK LOGIN completed"},
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

var proxiedStoreTests = []struct {
	in  string
	out string
}{
	{"a LOGIN user8 password8", "a OK LOGIN completed"},
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

var proxiedExpungeTests = []struct {
	in  string
	out string
}{
	{"a LOGIN user9 password9", "a OK LOGIN completed"},
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

// TestProxiedSelect executes a black-box table
// test on the Select() function of storage.
func TestProxiedSelect(t *testing.T) {

	// Create needed test environment.
	config, tlsConfig, err := utils.CreateTestEnv()
	if err != nil {
		log.Fatal(err)
	}

	// Start a storage node in background.
	go utils.RunStorageWithTimeout(config, 2400)
	time.Sleep(100 * time.Millisecond)

	// Start a worker node in background.
	go utils.RunWorkerWithTimeout(config, "worker-1", 2300)
	time.Sleep(100 * time.Millisecond)

	// Start a distributor node in background.
	go utils.RunDistributorWithTimeout(config, 2200)
	time.Sleep(600 * time.Millisecond)

	// Connect to IMAP server.
	conn, err := tls.Dial("tcp", (config.Distributor.IP + ":" + config.Distributor.Port), tlsConfig)
	if err != nil {
		t.Fatalf("[imap.TestProxiedSelect] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestProxiedSelect] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for _, tt := range proxiedSelectTests {

		var answer string

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
		if err != nil {
			t.Fatalf("[imap.TestProxiedSelect] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive answer to SELECT request.
		firstAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestProxiedSelect] Error during receiving table test SELECT: %s\n", err.Error())
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
				t.Errorf("[imap.TestProxiedSelect] Error during receiving table test SELECT: %s\n", err.Error())
			}

			firstAnswer = fmt.Sprintf("%s\n%s", firstAnswer, nextAnswer)
		}

		answer = firstAnswer

		if answer != tt.out {
			t.Fatalf("[imap.TestProxiedSelect] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()

	time.Sleep(2000 * time.Millisecond)
}

// TestProxiedCreate executes a black-box table
// test on the Create() function of storage.
func TestProxiedCreate(t *testing.T) {

	// Create needed test environment.
	config, tlsConfig, err := utils.CreateTestEnv()
	if err != nil {
		log.Fatal(err)
	}

	// Start a storage node in background.
	go utils.RunStorageWithTimeout(config, 2400)
	time.Sleep(100 * time.Millisecond)

	// Start a worker node in background.
	go utils.RunWorkerWithTimeout(config, "worker-1", 2300)
	time.Sleep(100 * time.Millisecond)

	// Start a distributor node in background.
	go utils.RunDistributorWithTimeout(config, 2200)
	time.Sleep(600 * time.Millisecond)

	// Connect to IMAP server.
	conn, err := tls.Dial("tcp", (config.Distributor.IP + ":" + config.Distributor.Port), tlsConfig)
	if err != nil {
		t.Fatalf("[imap.TestProxiedCreate] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestProxiedCreate] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for _, tt := range proxiedCreateTests {

		var answer string

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
		if err != nil {
			t.Fatalf("[imap.TestProxiedCreate] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive answer to CREATE request.
		firstAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestProxiedCreate] Error during receiving table test CREATE: %s\n", err.Error())
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
				t.Errorf("[imap.TestProxiedCreate] Error during receiving table test CREATE: %s\n", err.Error())
			}

			firstAnswer = fmt.Sprintf("%s\n%s", firstAnswer, nextAnswer)
		}

		answer = firstAnswer

		if answer != tt.out {
			t.Fatalf("[imap.TestProxiedCreate] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()

	time.Sleep(2000 * time.Millisecond)
}

// TestProxiedDelete executes a black-box table
// test on the Delete() function of storage.
func TestProxiedDelete(t *testing.T) {

	// Create needed test environment.
	config, tlsConfig, err := utils.CreateTestEnv()
	if err != nil {
		log.Fatal(err)
	}

	// Start a storage node in background.
	go utils.RunStorageWithTimeout(config, 2400)
	time.Sleep(100 * time.Millisecond)

	// Start a worker node in background.
	go utils.RunWorkerWithTimeout(config, "worker-1", 2300)
	time.Sleep(100 * time.Millisecond)

	// Start a distributor node in background.
	go utils.RunDistributorWithTimeout(config, 2200)
	time.Sleep(600 * time.Millisecond)

	// Connect to IMAP server.
	conn, err := tls.Dial("tcp", (config.Distributor.IP + ":" + config.Distributor.Port), tlsConfig)
	if err != nil {
		t.Fatalf("[imap.TestProxiedDelete] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestProxiedDelete] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for _, tt := range proxiedDeleteTests {

		var answer string

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
		if err != nil {
			t.Fatalf("[imap.TestProxiedDelete] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive answer to DELETE request.
		firstAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestProxiedDelete] Error during receiving table test DELETE: %s\n", err.Error())
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
				t.Errorf("[imap.TestProxiedDelete] Error during receiving table test DELETE: %s\n", err.Error())
			}

			firstAnswer = fmt.Sprintf("%s\n%s", firstAnswer, nextAnswer)
		}

		answer = firstAnswer

		if answer != tt.out {
			t.Fatalf("[imap.TestProxiedDelete] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()

	time.Sleep(2000 * time.Millisecond)
}

// TestProxiedList executes a black-box table
// test on the List() function of storage.
func TestProxiedList(t *testing.T) {

	// Create needed test environment.
	config, tlsConfig, err := utils.CreateTestEnv()
	if err != nil {
		log.Fatal(err)
	}

	// Start a storage node in background.
	go utils.RunStorageWithTimeout(config, 2400)
	time.Sleep(100 * time.Millisecond)

	// Start a worker node in background.
	go utils.RunWorkerWithTimeout(config, "worker-1", 2300)
	time.Sleep(100 * time.Millisecond)

	// Start a distributor node in background.
	go utils.RunDistributorWithTimeout(config, 2200)
	time.Sleep(600 * time.Millisecond)

	// Connect to IMAP server.
	conn, err := tls.Dial("tcp", (config.Distributor.IP + ":" + config.Distributor.Port), tlsConfig)
	if err != nil {
		t.Fatalf("[imap.TestProxiedList] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestProxiedList] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for _, tt := range proxiedListTests {

		var answer string

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
		if err != nil {
			t.Fatalf("[imap.TestProxiedList] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive answer to LIST request.
		firstAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestProxiedList] Error during receiving table test LIST: %s\n", err.Error())
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
				t.Errorf("[imap.TestProxiedList] Error during receiving table test LIST: %s\n", err.Error())
			}

			firstAnswer = fmt.Sprintf("%s\n%s", firstAnswer, nextAnswer)
		}

		answer = firstAnswer

		if (answer != tt.outOne) && (answer != tt.outTwo) {
			t.Fatalf("[imap.TestProxiedList] Expected '%s' or '%s' but received '%s'\n", tt.outOne, tt.outTwo, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()

	time.Sleep(2000 * time.Millisecond)
}

// TestProxiedAppend executes a black-box table
// test on the Append() function of storage.
func TestProxiedAppend(t *testing.T) {

	// Create needed test environment.
	config, tlsConfig, err := utils.CreateTestEnv()
	if err != nil {
		log.Fatal(err)
	}

	// Start a storage node in background.
	go utils.RunStorageWithTimeout(config, 2400)
	time.Sleep(100 * time.Millisecond)

	// Start a worker node in background.
	go utils.RunWorkerWithTimeout(config, "worker-1", 2300)
	time.Sleep(100 * time.Millisecond)

	// Start a distributor node in background.
	go utils.RunDistributorWithTimeout(config, 2200)
	time.Sleep(600 * time.Millisecond)

	// Connect to IMAP server.
	conn, err := tls.Dial("tcp", (config.Distributor.IP + ":" + config.Distributor.Port), tlsConfig)
	if err != nil {
		t.Fatalf("[imap.TestProxiedAppend] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestProxiedAppend] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for i, tt := range proxiedAppendTests {

		var answer string

		if (i == 3) || (i == 5) || (i == 8) {

			// Send mail message without additional newline.
			_, err = fmt.Fprintf(c.Conn, "%s", tt.in)
			if err != nil {
				t.Fatalf("[imap.TestProxiedAppend] Sending mail message to server failed with: %s\n", err.Error())
			}
		} else {

			// Table test: send 'in' part of each line.
			err = c.Send(tt.in)
			if err != nil {
				t.Fatalf("[imap.TestProxiedAppend] Sending message to server failed with: %s\n", err.Error())
			}
		}

		// Receive answer to APPEND request.
		firstAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestProxiedAppend] Error during receiving table test APPEND: %s\n", err.Error())
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
				t.Errorf("[imap.TestProxiedAppend] Error during receiving table test APPEND: %s\n", err.Error())
			}

			firstAnswer = fmt.Sprintf("%s\n%s", firstAnswer, nextAnswer)
		}

		answer = firstAnswer

		if answer != tt.out {
			t.Fatalf("[imap.TestProxiedAppend] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()

	time.Sleep(2000 * time.Millisecond)
}

// TestProxiedStore executes a black-box table
// test on the Store() function of storage.
func TestProxiedStore(t *testing.T) {

	// Create needed test environment.
	config, tlsConfig, err := utils.CreateTestEnv()
	if err != nil {
		log.Fatal(err)
	}

	// Start a storage node in background.
	go utils.RunStorageWithTimeout(config, 2400)
	time.Sleep(100 * time.Millisecond)

	// Start a worker node in background.
	go utils.RunWorkerWithTimeout(config, "worker-1", 2300)
	time.Sleep(100 * time.Millisecond)

	// Start a distributor node in background.
	go utils.RunDistributorWithTimeout(config, 2200)
	time.Sleep(600 * time.Millisecond)

	// Connect to IMAP server.
	conn, err := tls.Dial("tcp", (config.Distributor.IP + ":" + config.Distributor.Port), tlsConfig)
	if err != nil {
		t.Fatalf("[imap.TestProxiedStore] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestProxiedStore] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for i, tt := range proxiedStoreTests {

		var answer string

		if (i == 3) || (i == 5) || (i == 7) || (i == 9) || (i == 11) {

			// Send mail message without additional newline.
			_, err = fmt.Fprintf(c.Conn, "%s", tt.in)
			if err != nil {
				t.Fatalf("[imap.TestProxiedStore] Sending mail message to server failed with: %s\n", err.Error())
			}
		} else {

			// Table test: send 'in' part of each line.
			err = c.Send(tt.in)
			if err != nil {
				t.Fatalf("[imap.TestProxiedStore] Sending message to server failed with: %s\n", err.Error())
			}
		}

		// Receive answer to STORE request.
		firstAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestProxiedStore] Error during receiving table test STORE: %s\n", err.Error())
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
				t.Errorf("[imap.TestProxiedStore] Error during receiving table test STORE: %s\n", err.Error())
			}

			firstAnswer = fmt.Sprintf("%s\n%s", firstAnswer, nextAnswer)
		}

		answer = firstAnswer

		if answer != tt.out {
			t.Fatalf("[imap.TestProxiedStore] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()

	time.Sleep(2000 * time.Millisecond)
}

// TestProxiedExpunge executes a black-box table
// test on the Expunge() function of storage.
func TestProxiedExpunge(t *testing.T) {

	// Create needed test environment.
	config, tlsConfig, err := utils.CreateTestEnv()
	if err != nil {
		log.Fatal(err)
	}

	// Start a storage node in background.
	go utils.RunStorageWithTimeout(config, 2400)
	time.Sleep(100 * time.Millisecond)

	// Start a worker node in background.
	go utils.RunWorkerWithTimeout(config, "worker-1", 2300)
	time.Sleep(100 * time.Millisecond)

	// Start a distributor node in background.
	go utils.RunDistributorWithTimeout(config, 2200)
	time.Sleep(600 * time.Millisecond)

	// Connect to IMAP server.
	conn, err := tls.Dial("tcp", (config.Distributor.IP + ":" + config.Distributor.Port), tlsConfig)
	if err != nil {
		t.Fatalf("[imap.TestProxiedExpunge] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestProxiedExpunge] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for i, tt := range proxiedExpungeTests {

		var answer string

		if (i == 3) || (i == 5) || (i == 7) || (i == 9) || (i == 11) {

			// Send mail message without additional newline.
			_, err = fmt.Fprintf(c.Conn, "%s", tt.in)
			if err != nil {
				t.Fatalf("[imap.TestProxiedExpunge] Sending mail message to server failed with: %s\n", err.Error())
			}
		} else {

			// Table test: send 'in' part of each line.
			err = c.Send(tt.in)
			if err != nil {
				t.Fatalf("[imap.TestProxiedExpunge] Sending message to server failed with: %s\n", err.Error())
			}
		}

		// Receive answer to EXPUNGE request.
		firstAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestProxiedExpunge] Error during receiving table test EXPUNGE: %s\n", err.Error())
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
				t.Errorf("[imap.TestProxiedExpunge] Error during receiving table test EXPUNGE: %s\n", err.Error())
			}

			firstAnswer = fmt.Sprintf("%s\n%s", firstAnswer, nextAnswer)
		}

		answer = firstAnswer

		if answer != tt.out {
			t.Fatalf("[imap.TestProxiedExpunge] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()

	time.Sleep(2000 * time.Millisecond)
}
