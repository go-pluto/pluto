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
