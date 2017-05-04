package comm_test

import (
	"testing"

	"github.com/numbleroot/pluto/comm"
)

// Functions

// TODO: Add lots of missing tests for functions.

// TestString executes a black-box unit test
// on implemented String() function of messages.
func TestString(t *testing.T) {

	// Create a new message struct.
	msg := comm.InitMessage()

	// Check marshalling.
	marshalled := msg.String()
	if marshalled != "||" {
		t.Fatalf("[comm.TestString] Expected '||' as marshalled initial message, but got '%s'\n", marshalled)
	}

	// Set sender name.
	msg.Sender = "worker-1"

	// Check marshalling.
	marshalled = msg.String()
	if marshalled != "worker-1||" {
		t.Fatalf("[comm.TestString] Expected 'worker-1||' as marshalled message, but got '%s'\n", marshalled)
	}

	// Set one vector clock entry.
	msg.Sender = ""
	msg.VClock["A"] = 5

	// Check marshalling.
	marshalled = msg.String()
	if marshalled != "|A:5|" {
		t.Fatalf("[comm.TestString] Expected '|A:5|' as marshalled message, but got '%s'\n", marshalled)
	}

	// Set payload once.
	msg.VClock = make(map[string]int)
	msg.Payload = "lorem ipsum DOLOR sit"

	// Check marshalling.
	marshalled = msg.String()
	if marshalled != "||lorem ipsum DOLOR sit" {
		t.Fatalf("[comm.TestString] Expected '||lorem ipsum DOLOR sit' as marshalled message, but got '%s'\n", marshalled)
	}

	// Set multiple values.
	msg.Sender = "storage"
	msg.VClock["worker-1"] = 3
	msg.VClock["worker-2"] = 10
	msg.VClock["worker-3"] = 0
	msg.Payload = "works"

	// Check marshalling.
	marshalled = msg.String()
	if (marshalled != "storage|worker-1:3;worker-2:10;worker-3:0|works") &&
		(marshalled != "storage|worker-1:3;worker-3:0;worker-2:10|works") &&
		(marshalled != "storage|worker-2:10;worker-1:3;worker-3:0|works") &&
		(marshalled != "storage|worker-2:10;worker-3:0;worker-1:3|works") &&
		(marshalled != "storage|worker-3:0;worker-1:3;worker-2:10|works") &&
		(marshalled != "storage|worker-3:0;worker-2:10;worker-1:3|works") {
		t.Fatalf("[comm.TestString] Expected 'storage|worker-1:3;worker-2:10;worker-3:0|works' as marshalled message, but got '%s'\n", marshalled)
	}
}

// TestString executes a black-box unit test
// on implemented Parse() function of messages.
func TestParse(t *testing.T) {

	// Test strings.
	marshalled1 := "abc"
	marshalled2 := "||"
	marshalled3 := "sender|A|abc"
	marshalled4 := "sender|A:string|abc"
	marshalled5 := "sender|A:5|abc"
	marshalled6 := "worker-1|A:5;B:3;C:10;D:7|this is a long payload"

	// Check parsing.
	_, err := comm.Parse(marshalled1)
	if err.Error() != "invalid sync message" {
		t.Fatalf("[comm.TestParse] marshalled1: Expected 'invalid sync message' but received: '%s'\n", err.Error())
	}

	// Check parsing.
	_, err = comm.Parse(marshalled2)
	if err.Error() != "invalid sync message because sender node name is missing" {
		t.Fatalf("[comm.TestParse] marshalled2: Expected 'invalid sync message because sender node name is missing' but received: '%s'\n", err.Error())
	}

	// Check parsing.
	_, err = comm.Parse(marshalled3)
	if err.Error() != "invalid vector clock element" {
		t.Fatalf("[comm.TestParse] marshalled3: Expected 'invalid vector clock element' but received: '%s'\n", err.Error())
	}

	// Check parsing.
	_, err = comm.Parse(marshalled4)
	if err.Error() != "invalid number as element in vector clock" {
		t.Fatalf("[comm.TestParse] marshalled4: Expected 'invalid number as element in vector clock' but received: '%s'\n", err.Error())
	}

	// Check parsing.
	msg5, err := comm.Parse(marshalled5)
	if err != nil {
		t.Fatalf("[comm.TestParse] marshalled5: Expected nil error but received: '%s'\n", err.Error())
	}

	if msg5.Sender != "sender" {
		t.Fatalf("[comm.TestParse] marshalled5: Expected 'sender' as sending node but found: '%v'\n", msg5.Sender)
	}

	if msg5.VClock["A"] != 5 {
		t.Fatalf("[comm.TestParse] marshalled5: Expected value '5' at key 'A' but found: '%v'\n", msg5.VClock["A"])
	}

	if msg5.Payload != "abc" {
		t.Fatalf("[comm.TestParse] marshalled5: Expected value 'abc' as payload but found: '%v'\n", msg5.Payload)
	}

	// Check parsing.
	msg6, err := comm.Parse(marshalled6)
	if err != nil {
		t.Fatalf("[comm.TestParse] marshalled6: Expected nil error but received: '%s'\n", err.Error())
	}

	if msg6.Sender != "worker-1" {
		t.Fatalf("[comm.TestParse] marshalled6: Expected 'worker-1' as sending node but found: '%v'\n", msg6.Sender)
	}

	for i, e := range msg6.VClock {

		switch i {

		case "A":
			if e != 5 {
				t.Fatalf("[comm.TestParse] marshalled6: Expected value '5' at key 'A' but found: '%v'\n", msg6.VClock["A"])
			}

		case "B":
			if e != 3 {
				t.Fatalf("[comm.TestParse] marshalled6: Expected value '3' at key 'B' but found: '%v'\n", msg6.VClock["B"])
			}

		case "C":
			if e != 10 {
				t.Fatalf("[comm.TestParse] marshalled6: Expected value '10' at key 'C' but found: '%v'\n", msg6.VClock["C"])
			}

		case "D":
			if e != 7 {
				t.Fatalf("[comm.TestParse] marshalled6: Expected value '7' at key 'D' but found: '%v'\n", msg6.VClock["D"])
			}
		}
	}

	if msg6.Payload != "this is a long payload" {
		t.Fatalf("[comm.TestParse] marshalled6: Expected value 'this is a long payload' as payload but found: '%v'\n", msg6.Payload)
	}
}

// TestParseOp executes a black-box unit test
// on implemented ParseOp() function of messages.
func TestParseOp(t *testing.T) {

	// Test message strings.
	msg1 := "|"
	msg2 := "operation|payload"
	msg3 := "123op!|p$$$load|more|payload|v4lu3s"

	op1, payload1, err := comm.ParseOp(msg1)
	if err != nil {
		t.Fatalf("[comm.TestParseOp] msg1: Expected nil error but received: '%v'", err)
	}

	if op1 != "" {
		t.Fatalf("[comm.TestParseOp] msg1: Expected operation to be '' but found: '%s'", op1)
	}

	if payload1 != "" {
		t.Fatalf("[comm.TestParseOp] msg1: Expected payload to be '' but found: '%s'", payload1)
	}

	op2, payload2, err := comm.ParseOp(msg2)
	if err != nil {
		t.Fatalf("[comm.TestParseOp] msg2: Expected nil error but received: '%v'", err)
	}

	if op2 != "operation" {
		t.Fatalf("[comm.TestParseOp] msg2: Expected operation to be 'operation' but found: '%s'", op2)
	}

	if payload2 != "payload" {
		t.Fatalf("[comm.TestParseOp] msg2: Expected payload to be 'payload' but found: '%s'", payload2)
	}

	op3, payload3, err := comm.ParseOp(msg3)
	if err != nil {
		t.Fatalf("[comm.TestParseOp] msg3: Expected nil error but received: '%v'", err)
	}

	if op3 != "123op!" {
		t.Fatalf("[comm.TestParseOp] msg3: Expected operation to be '123op!' but found: '%s'", op3)
	}

	if payload3 != "p$$$load|more|payload|v4lu3s" {
		t.Fatalf("[comm.TestParseOp] msg3: Expected payload to be 'p$$$load|more|payload|v4lu3s' but found: '%s'", payload3)
	}
}

// TestParseCreate executes a black-box unit test
// on implemented ParseCreate() function of messages.
func TestParseCreate(t *testing.T) {

	// user1, University.Thesis, University.Thesis, ae3daa63-4d50-4ea5-baa4-d5f780e05302, ""
	create1 := "user1|VW5pdmVyc2l0eS5UaGVzaXM=|VW5pdmVyc2l0eS5UaGVzaXM=;ae3daa63-4d50-4ea5-baa4-d5f780e05302"

	createParsed1, err := comm.ParseCreate(create1)
	if err != nil {
		t.Fatalf("[comm.TestParseCreate] create1: Expected nil error but received: '%v'", err)
	}

	if createParsed1.User != "user1" {
		t.Fatalf("[comm.TestParseCreate] create1: Expected User to be 'user1' but found: '%v'", createParsed1.User)
	}

	if createParsed1.Mailbox != "University.Thesis" {
		t.Fatalf("[comm.TestParseCreate] create1: Expected Mailbox to be 'University.Thesis' but found: '%v'", createParsed1.Mailbox)
	}

	if createParsed1.AddMailbox.Value != "University.Thesis" {
		t.Fatalf("[comm.TestParseCreate] create1: Expected AddMailbox.Value to be 'University.Thesis' but found: '%v'", createParsed1.AddMailbox.Value)
	}

	if createParsed1.AddMailbox.Tag != "ae3daa63-4d50-4ea5-baa4-d5f780e05302" {
		t.Fatalf("[comm.TestParseCreate] create1: Expected AddMailbox.Tag to be 'ae3daa63-4d50-4ea5-baa4-d5f780e05302' but found: '%v'", createParsed1.AddMailbox.Tag)
	}

	if createParsed1.AddMailbox.Contents != "" {
		t.Fatalf("[comm.TestParseCreate] create1: Expected AddMailbox.Contents to be '' but found: '%v'", createParsed1.AddMailbox.Contents)
	}
}

// TestParseDelete executes a black-box unit test
// on implemented ParseDelete() function of messages.
func TestParseDelete(t *testing.T) {

	// user1, University.Thesis, University.Thesis, 76e18257-3687-41f3-8c94-9a7b4d1a9799, ""
	delete1 := "user1|VW5pdmVyc2l0eS5UaGVzaXM=|VW5pdmVyc2l0eS5UaGVzaXM=;76e18257-3687-41f3-8c94-9a7b4d1a9799"

	deleteParsed1, err := comm.ParseDelete(delete1)
	if err != nil {
		t.Fatalf("[comm.TestParseDelete] delete1: Expected nil error but received: '%v'", err)
	}

	if deleteParsed1.User != "user1" {
		t.Fatalf("[comm.TestParseDelete] delete1: Expected User to be 'user1' but found: '%v'", deleteParsed1.User)
	}

	if deleteParsed1.Mailbox != "University.Thesis" {
		t.Fatalf("[comm.TestParseDelete] delete1: Expected Mailbox to be 'University.Thesis' but found: '%v'", deleteParsed1.Mailbox)
	}

	for _, rmvMailbox := range deleteParsed1.RmvMailbox {

		if rmvMailbox.Value != "University.Thesis" {
			t.Fatalf("[comm.TestParseDelete] delete1: Expected RmvMailbox.Value to be 'University.Thesis' but found: '%v'", rmvMailbox.Value)
		}

		if rmvMailbox.Tag != "76e18257-3687-41f3-8c94-9a7b4d1a9799" {
			t.Fatalf("[comm.TestParseDelete] delete1: Expected RmvMailbox.Tag to be '76e18257-3687-41f3-8c94-9a7b4d1a9799' but found: '%v'", rmvMailbox.Tag)
		}

		if rmvMailbox.Contents != "" {
			t.Fatalf("[comm.TestParseDelete] delete1: Expected RmvMailbox.Contents to be '' but found: '%v'", rmvMailbox.Contents)
		}
	}
}
