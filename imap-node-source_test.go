package main

import (
	"bufio"
	"fmt"
	"strings"
	"testing"

	"crypto/tls"

	"github.com/numbleroot/pluto/imap"
)

// Variables

var msg1 = `Date: Mon, 7 Feb 1994 21:52:25 -0800 (PST)
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
	{"a LOGIN user10 password10", "a OK LOGIN completed"},
	{"b SELECT INBOX", "* 0 EXISTS\r\n* 0 RECENT\r\n* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n* OK [PERMANENTFLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)]\r\nb OK [READ-WRITE] SELECT completed"},
	{"c SELECT", "c BAD Command SELECT was sent without a mailbox to select"},
	{"d SELECT lol rofl nope", "d BAD Command SELECT was sent with multiple mailbox names instead of only one"},
	{"e LOGOUT", "* BYE Terminating connection\r\ne OK LOGOUT completed"},
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
	{"m LOGOUT", "* BYE Terminating connection\r\nm OK LOGOUT completed"},
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
	{"l LOGOUT", "* BYE Terminating connection\r\nl OK LOGOUT completed"},
}

var listTests = []struct {
	in     string
	outOne string
	outTwo string
}{
	{"a LOGIN user1 password1", "a OK LOGIN completed", "a OK LOGIN completed"},
	{"b LIST \"\" *", "* LIST () \".\" INBOX\r\n* LIST () \".\" university\r\nb OK LIST completed", "* LIST () \".\" university\r\n* LIST () \".\" INBOX\r\nb OK LIST completed"},
	{"c CREATE university.Modul1", "c OK CREATE completed", "c OK CREATE completed"},
	{"d LIST \"\" %", "* LIST () \".\" INBOX\r\n* LIST () \".\" university\r\nd OK LIST completed", "* LIST () \".\" university\r\n* LIST () \".\" INBOX\r\nd OK LIST completed"},
	{"e DELETE university", "e OK DELETE completed", "e OK DELETE completed"},
	{"f LIST \"\" *", "* LIST () \".\" INBOX\r\n* LIST () \".\" university.Modul1\r\nf OK LIST completed", "* LIST () \".\" university.Modul1\r\n* LIST () \".\" INBOX\r\nf OK LIST completed"},
	{"g LOGOUT", "* BYE Terminating connection\r\ng OK LOGOUT completed", "* BYE Terminating connection\r\ng OK LOGOUT completed"},
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
	{"z LOGOUT", "* BYE Terminating connection\r\nz OK LOGOUT completed"},
}

var storeTests = []struct {
	in  string
	out string
}{
	{"a LOGIN user3 password3", "a OK LOGIN completed"},
	{"b APPEND INBOX {301}", "+ Ready for literal data"},
	{msg1, "b OK APPEND completed"},
	{"c SELECT INBOX", "* 1 EXISTS\r\n* 1 RECENT\r\n* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n* OK [PERMANENTFLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)]\r\nc OK [READ-WRITE] SELECT completed"},
	{"d STORE 1 FLAGS.SILENT (\\Answered \\Draft \\Deleted)", "d OK STORE completed"},
	{"e LOGOUT", "* BYE Terminating connection\r\ne OK LOGOUT completed"},
	{"f LOGIN user3 password3", "f OK LOGIN completed"},
	{"g CREATE Sports", "g OK CREATE completed"},
	{"h APPEND Sports {301}", "+ Ready for literal data"},
	{msg1, "h OK APPEND completed"},
	{"i APPEND Sports {301}", "+ Ready for literal data"},
	{msg1, "i OK APPEND completed"},
	{"j APPEND Sports {301}", "+ Ready for literal data"},
	{msg1, "j OK APPEND completed"},
	{"k APPEND Sports {301}", "+ Ready for literal data"},
	{msg1, "k OK APPEND completed"},
	{"l APPEND Sports {301}", "+ Ready for literal data"},
	{msg1, "l OK APPEND completed"},
	{"m STORE anything", "m BAD No mailbox selected for store"},
	{"n SELECT Sports", "* 5 EXISTS\r\n* 5 RECENT\r\n* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n* OK [PERMANENTFLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)]\r\nn OK [READ-WRITE] SELECT completed"},
	{"o STORE too few", "o BAD Command STORE was not sent with three parameters"},
	{"p STORE one,two FLAGS (\\Seen)", "p BAD Command STORE was sent with an invalid number parameter"},
	{"q STORE 2,4:* WHYNOTTHIS? (\\Seen)", "q BAD Unknown data item type specified"},
	{"r STORE 2,4:* -FLAGS.SILENT \\Seen", "r BAD Command STORE was sent with invalid parenthesized flags list"},
	{"s STORE 2,4:* +FLAGS (\\Seen \\Answered)", "* 2 FETCH (FLAGS (\\Answered \\Seen))\r\n* 4 FETCH (FLAGS (\\Answered \\Seen))\r\n* 5 FETCH (FLAGS (\\Answered \\Seen))\r\ns OK STORE completed"},
	{"t STORE 3,2,1 -FLAGS (\\Answered)", "* 1 FETCH (FLAGS ())\r\n* 2 FETCH (FLAGS (\\Seen))\r\n* 3 FETCH (FLAGS ())\r\nt OK STORE completed"},
	{"u STORE 1,2,3:* FLAGS.SILENT (\\Draft \\Deleted)", "u OK STORE completed"},
	{"v STORE 5,3,4,1,2 +FLAGS (\\Answered)", "* 1 FETCH (FLAGS (\\Answered \\Draft \\Deleted))\r\n* 2 FETCH (FLAGS (\\Answered \\Draft \\Deleted))\r\n* 3 FETCH (FLAGS (\\Answered \\Draft \\Deleted))\r\n* 4 FETCH (FLAGS (\\Answered \\Draft \\Deleted))\r\n* 5 FETCH (FLAGS (\\Answered \\Draft \\Deleted))\r\nv OK STORE completed"},
	{"w LOGOUT", "* BYE Terminating connection\r\nw OK LOGOUT completed"},
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
	{"i SELECT Monday", "* 5 EXISTS\r\n* 5 RECENT\r\n* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n* OK [PERMANENTFLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)]\r\ni OK [READ-WRITE] SELECT completed"},
	{"j EXPUNGE", "j OK EXPUNGE completed"},
	{"k STORE 1:* +FLAGS (\\Deleted)", "* 1 FETCH (FLAGS (\\Deleted))\r\n* 2 FETCH (FLAGS (\\Deleted))\r\n* 3 FETCH (FLAGS (\\Deleted))\r\n* 4 FETCH (FLAGS (\\Deleted))\r\n* 5 FETCH (FLAGS (\\Deleted))\r\nk OK STORE completed"},
	{"l EXPUNGE", "* 5 EXPUNGE\r\n* 4 EXPUNGE\r\n* 3 EXPUNGE\r\n* 2 EXPUNGE\r\n* 1 EXPUNGE\r\nl OK EXPUNGE completed"},
	{"m LOGOUT", "* BYE Terminating connection\r\nm OK LOGOUT completed"},
}

// Functions

// TestSelect executes a black-box table test on the
// implemented Select() function.
func TestSelect(t *testing.T) {

	// Connect to IMAP server.
	conn, err := tls.Dial("tcp", testEnv.Addr, testEnv.TLSConfig)
	if err != nil {
		t.Fatalf("[imap.TestSelect] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := &imap.Connection{
		OutConn:   conn,
		OutReader: bufio.NewReader(conn),
	}

	// Consume mandatory IMAP greeting.
	_, err = c.Receive(false)
	if err != nil {
		t.Fatalf("[imap.TestSelect] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for _, tt := range selectTests {

		var answer string

		// Table test: send 'in' part of each line.
		err = c.Send(false, tt.in)
		if err != nil {
			t.Fatalf("[imap.TestSelect] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive answer to SELECT request.
		firstAnswer, err := c.Receive(false)
		if err != nil {
			t.Fatalf("[imap.TestSelect] Error during receiving table test SELECT: %s\n", err.Error())
		}

		// As long as the IMAP command termination indicator
		// was not yet received, continue append answers.
		for (strings.Contains(firstAnswer, "completed") != true) &&
			(strings.Contains(firstAnswer, "BAD") != true) &&
			(strings.Contains(firstAnswer, "NO") != true) &&
			(strings.Contains(firstAnswer, "+ Ready for literal data") != true) {

			// Receive next line from distributor.
			nextAnswer, err := c.Receive(false)
			if err != nil {
				t.Fatalf("[imap.TestSelect] Error during receiving table test SELECT: %s\n", err.Error())
			}

			firstAnswer = fmt.Sprintf("%s\r\n%s", firstAnswer, nextAnswer)
		}

		answer = firstAnswer

		if answer != tt.out {
			t.Fatalf("[imap.TestSelect] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()
}

// TestCreate executes a black-box table test on the
// implemented Create() function.
func TestCreate(t *testing.T) {

	// Connect to IMAP server.
	conn, err := tls.Dial("tcp", testEnv.Addr, testEnv.TLSConfig)
	if err != nil {
		t.Fatalf("[imap.TestCreate] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := &imap.Connection{
		OutConn:   conn,
		OutReader: bufio.NewReader(conn),
	}

	// Consume mandatory IMAP greeting.
	_, err = c.Receive(false)
	if err != nil {
		t.Fatalf("[imap.TestCreate] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for _, tt := range createTests {

		var answer string

		// Table test: send 'in' part of each line.
		err = c.Send(false, tt.in)
		if err != nil {
			t.Fatalf("[imap.TestCreate] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive answer to CREATE request.
		firstAnswer, err := c.Receive(false)
		if err != nil {
			t.Fatalf("[imap.TestCreate] Error during receiving table test CREATE: %s\n", err.Error())
		}

		// As long as the IMAP command termination indicator
		// was not yet received, continue append answers.
		for (strings.Contains(firstAnswer, "completed") != true) &&
			(strings.Contains(firstAnswer, "BAD") != true) &&
			(strings.Contains(firstAnswer, "NO") != true) &&
			(strings.Contains(firstAnswer, "+ Ready for literal data") != true) {

			// Receive next line from distributor.
			nextAnswer, err := c.Receive(false)
			if err != nil {
				t.Fatalf("[imap.TestCreate] Error during receiving table test CREATE: %s\n", err.Error())
			}

			firstAnswer = fmt.Sprintf("%s\r\n%s", firstAnswer, nextAnswer)
		}

		answer = firstAnswer

		if answer != tt.out {
			t.Fatalf("[imap.TestCreate] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()
}

// TestDelete executes a black-box table test on the
// implemented Delete() function.
func TestDelete(t *testing.T) {

	// Connect to IMAP server.
	conn, err := tls.Dial("tcp", testEnv.Addr, testEnv.TLSConfig)
	if err != nil {
		t.Fatalf("[imap.TestDelete] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := &imap.Connection{
		OutConn:   conn,
		OutReader: bufio.NewReader(conn),
	}

	// Consume mandatory IMAP greeting.
	_, err = c.Receive(false)
	if err != nil {
		t.Fatalf("[imap.TestDelete] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for _, tt := range deleteTests {

		var answer string

		// Table test: send 'in' part of each line.
		err = c.Send(false, tt.in)
		if err != nil {
			t.Fatalf("[imap.TestDelete] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive answer to DELETE request.
		firstAnswer, err := c.Receive(false)
		if err != nil {
			t.Fatalf("[imap.TestDelete] Error during receiving table test DELETE: %s\n", err.Error())
		}

		// As long as the IMAP command termination indicator
		// was not yet received, continue append answers.
		for (strings.Contains(firstAnswer, "completed") != true) &&
			(strings.Contains(firstAnswer, "BAD") != true) &&
			(strings.Contains(firstAnswer, "NO") != true) &&
			(strings.Contains(firstAnswer, "+ Ready for literal data") != true) {

			// Receive next line from distributor.
			nextAnswer, err := c.Receive(false)
			if err != nil {
				t.Fatalf("[imap.TestDelete] Error during receiving table test DELETE: %s\n", err.Error())
			}

			firstAnswer = fmt.Sprintf("%s\r\n%s", firstAnswer, nextAnswer)
		}

		answer = firstAnswer

		if answer != tt.out {
			t.Fatalf("[imap.TestDelete] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()
}

// TestList executes a black-box table test on the
// implemented List() function.
func TestList(t *testing.T) {

	// Connect to IMAP server.
	conn, err := tls.Dial("tcp", testEnv.Addr, testEnv.TLSConfig)
	if err != nil {
		t.Fatalf("[imap.TestList] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := &imap.Connection{
		OutConn:   conn,
		OutReader: bufio.NewReader(conn),
	}

	// Consume mandatory IMAP greeting.
	_, err = c.Receive(false)
	if err != nil {
		t.Fatalf("[imap.TestList] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for _, tt := range listTests {

		var answer string

		// Table test: send 'in' part of each line.
		err = c.Send(false, tt.in)
		if err != nil {
			t.Fatalf("[imap.TestList] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive answer to LIST request.
		firstAnswer, err := c.Receive(false)
		if err != nil {
			t.Fatalf("[imap.TestList] Error during receiving table test LIST: %s\n", err.Error())
		}

		// As long as the IMAP command termination indicator
		// was not yet received, continue append answers.
		for (strings.Contains(firstAnswer, "completed") != true) &&
			(strings.Contains(firstAnswer, "BAD") != true) &&
			(strings.Contains(firstAnswer, "NO") != true) &&
			(strings.Contains(firstAnswer, "+ Ready for literal data") != true) {

			// Receive next line from distributor.
			nextAnswer, err := c.Receive(false)
			if err != nil {
				t.Fatalf("[imap.TestList] Error during receiving table test LIST: %s\n", err.Error())
			}

			firstAnswer = fmt.Sprintf("%s\r\n%s", firstAnswer, nextAnswer)
		}

		answer = firstAnswer

		if (answer != tt.outOne) && (answer != tt.outTwo) {
			t.Fatalf("[imap.TestList] Expected '%s' or '%s' but received '%s'\n", tt.outOne, tt.outTwo, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()
}

// TestAppend executes a black-box table test on the
// implemented Append() function.
func TestAppend(t *testing.T) {

	// Connect to IMAP server.
	conn, err := tls.Dial("tcp", testEnv.Addr, testEnv.TLSConfig)
	if err != nil {
		t.Fatalf("[imap.TestAppend] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := &imap.Connection{
		OutConn:   conn,
		OutReader: bufio.NewReader(conn),
	}

	// Consume mandatory IMAP greeting.
	_, err = c.Receive(false)
	if err != nil {
		t.Fatalf("[imap.TestAppend] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for i, tt := range appendTests {

		var answer string

		if (i == 3) || (i == 5) || (i == 8) {

			// Send mail message without additional newline.
			_, err = fmt.Fprintf(c.OutConn, "%s", tt.in)
			if err != nil {
				t.Fatalf("[imap.TestAppend] Sending mail message to server failed with: %s\n", err.Error())
			}
		} else {

			// Table test: send 'in' part of each line.
			err = c.Send(false, tt.in)
			if err != nil {
				t.Fatalf("[imap.TestAppend] Sending message to server failed with: %s\n", err.Error())
			}
		}

		// Receive answer to APPEND request.
		firstAnswer, err := c.Receive(false)
		if err != nil {
			t.Fatalf("[imap.TestAppend] Error during receiving table test APPEND: %s\n", err.Error())
		}

		// As long as the IMAP command termination indicator
		// was not yet received, continue append answers.
		for (strings.Contains(firstAnswer, "completed") != true) &&
			(strings.Contains(firstAnswer, "BAD") != true) &&
			(strings.Contains(firstAnswer, "NO") != true) &&
			(strings.Contains(firstAnswer, "+ Ready for literal data") != true) {

			// Receive next line from distributor.
			nextAnswer, err := c.Receive(false)
			if err != nil {
				t.Fatalf("[imap.TestAppend] Error during receiving table test APPEND: %s\n", err.Error())
			}

			firstAnswer = fmt.Sprintf("%s\r\n%s", firstAnswer, nextAnswer)
		}

		answer = firstAnswer

		if answer != tt.out {
			t.Fatalf("[imap.TestAppend] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()
}

// TestStore executes a black-box table test on the
// implemented Store() function.
func TestStore(t *testing.T) {

	// Connect to IMAP server.
	conn, err := tls.Dial("tcp", testEnv.Addr, testEnv.TLSConfig)
	if err != nil {
		t.Fatalf("[imap.TestStore] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := &imap.Connection{
		OutConn:   conn,
		OutReader: bufio.NewReader(conn),
	}

	// Consume mandatory IMAP greeting.
	_, err = c.Receive(false)
	if err != nil {
		t.Fatalf("[imap.TestStore] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for i, tt := range storeTests {

		var answer string

		if i == 6 {

			// As the command prior to this command logged out
			// the user, we have to terminate and re-establish
			// the used connection.
			c.Terminate()

			conn, err = tls.Dial("tcp", testEnv.Addr, testEnv.TLSConfig)
			if err != nil {
				t.Fatalf("[imap.TestStore] Error during connection attempt to IMAP server: %s\n", err.Error())
			}

			// Create new connection struct.
			c = &imap.Connection{
				OutConn:   conn,
				OutReader: bufio.NewReader(conn),
			}

			// Consume mandatory IMAP greeting.
			_, err = c.Receive(false)
			if err != nil {
				t.Fatalf("[imap.TestStore] Error during receiving initial server greeting: %s\n", err.Error())
			}

			// Now, send login command.
			err = c.Send(false, tt.in)
			if err != nil {
				t.Fatalf("[imap.TestStore] Sending message to server failed with: %s\n", err.Error())
			}

		} else if (i == 2) || (i == 9) || (i == 11) || (i == 13) || (i == 15) || (i == 17) {

			// Send mail message without additional newline.
			_, err = fmt.Fprintf(c.OutConn, "%s", tt.in)
			if err != nil {
				t.Fatalf("[imap.TestStore] Sending mail message to server failed with: %s\n", err.Error())
			}

		} else {

			// Table test: send 'in' part of each line.
			err = c.Send(false, tt.in)
			if err != nil {
				t.Fatalf("[imap.TestStore] Sending message to server failed with: %s\n", err.Error())
			}
		}

		// Receive answer to STORE request.
		firstAnswer, err := c.Receive(false)
		if err != nil {
			t.Fatalf("[imap.TestStore] Error during receiving table test STORE: %s\n", err.Error())
		}

		// As long as the IMAP command termination indicator
		// was not yet received, continue append answers.
		for (strings.Contains(firstAnswer, "completed") != true) &&
			(strings.Contains(firstAnswer, "BAD") != true) &&
			(strings.Contains(firstAnswer, "NO") != true) &&
			(strings.Contains(firstAnswer, "+ Ready for literal data") != true) {

			// Receive next line from distributor.
			nextAnswer, err := c.Receive(false)
			if err != nil {
				t.Fatalf("[imap.TestStore] Error during receiving table test STORE: %s\n", err.Error())
			}

			firstAnswer = fmt.Sprintf("%s\r\n%s", firstAnswer, nextAnswer)
		}

		answer = firstAnswer

		if answer != tt.out {
			t.Fatalf("[imap.TestStore] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()
}

// TestExpunge executes a black-box table test on the
// implemented Expunge() function.
func TestExpunge(t *testing.T) {

	// Connect to IMAP server.
	conn, err := tls.Dial("tcp", testEnv.Addr, testEnv.TLSConfig)
	if err != nil {
		t.Fatalf("[imap.TestExpunge] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := &imap.Connection{
		OutConn:   conn,
		OutReader: bufio.NewReader(conn),
	}

	// Consume mandatory IMAP greeting.
	_, err = c.Receive(false)
	if err != nil {
		t.Fatalf("[imap.TestExpunge] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for i, tt := range expungeTests {

		var answer string

		if (i == 3) || (i == 5) || (i == 7) || (i == 9) || (i == 11) {

			// Send mail message without additional newline.
			_, err = fmt.Fprintf(c.OutConn, "%s", tt.in)
			if err != nil {
				t.Fatalf("[imap.TestExpunge] Sending mail message to server failed with: %s\n", err.Error())
			}
		} else {

			// Table test: send 'in' part of each line.
			err = c.Send(false, tt.in)
			if err != nil {
				t.Fatalf("[imap.TestExpunge] Sending message to server failed with: %s\n", err.Error())
			}
		}

		// Receive answer to EXPUNGE request.
		firstAnswer, err := c.Receive(false)
		if err != nil {
			t.Fatalf("[imap.TestExpunge] Error during receiving table test EXPUNGE: %s\n", err.Error())
		}

		// As long as the IMAP command termination indicator
		// was not yet received, continue append answers.
		for (strings.Contains(firstAnswer, "completed") != true) &&
			(strings.Contains(firstAnswer, "BAD") != true) &&
			(strings.Contains(firstAnswer, "NO") != true) &&
			(strings.Contains(firstAnswer, "+ Ready for literal data") != true) {

			// Receive next line from distributor.
			nextAnswer, err := c.Receive(false)
			if err != nil {
				t.Fatalf("[imap.TestExpunge] Error during receiving table test EXPUNGE: %s\n", err.Error())
			}

			firstAnswer = fmt.Sprintf("%s\r\n%s", firstAnswer, nextAnswer)
		}

		answer = firstAnswer

		if answer != tt.out {
			t.Fatalf("[imap.TestExpunge] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()
}
