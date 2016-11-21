package comm

import (
	"testing"
)

// Functions

// TestString executes a white-box unit test
// on implemented String() function of messages.
func TestString(t *testing.T) {

	// Create a new message struct.
	msg := Message{
		vclock: make(map[string]int),
	}

	// Check marshalling.
	marshalled := msg.String()
	if marshalled != "|" {
		t.Fatalf("[comm.TestString] Expected '|' as marshalled initial message, but got '%s'\n", marshalled)
	}

	// Set one vector clock entry.
	msg.vclock["A"] = 5

	// Check marshalling.
	marshalled = msg.String()
	if marshalled != "A:5|" {
		t.Fatalf("[comm.TestString] Expected 'A:5|' as marshalled initial message, but got '%s'\n", marshalled)
	}

	// Set payload once.
	msg.vclock = make(map[string]int)
	msg.payload = "lorem ipsum DOLOR sit"

	// Check marshalling.
	marshalled = msg.String()
	if marshalled != "|lorem ipsum DOLOR sit" {
		t.Fatalf("[comm.TestString] Expected '|lorem ipsum DOLOR sit' as marshalled initial message, but got '%s'\n", marshalled)
	}

	// Set multiple values.
	msg.vclock["worker-1"] = 3
	msg.vclock["worker-2"] = 10
	msg.vclock["worker-3"] = 0
	msg.payload = "works"

	// Check marshalling.
	marshalled = msg.String()
	if (marshalled != "worker-1:3;worker-2:10;worker-3:0|works") &&
		(marshalled != "worker-1:3;worker-3:0;worker-2:10|works") &&
		(marshalled != "worker-2:10;worker-1:3;worker-3:0|works") &&
		(marshalled != "worker-2:10;worker-3:0;worker-1:3|works") &&
		(marshalled != "worker-3:0;worker-1:3;worker-2:10|works") &&
		(marshalled != "worker-3:0;worker-2:10;worker-1:3|works") {
		t.Fatalf("[comm.TestString] Expected 'worker-1:3;worker-2:10;worker-3:0|works' as marshalled initial message, but got '%s'\n", marshalled)
	}
}

// TestString executes a white-box unit test
// on implemented Parse() function of messages.
func TestParse(t *testing.T) {

	// Test strings.
	marshalled1 := "abc"
	marshalled2 := "A|abc"
	marshalled3 := "A:string|abc"
	marshalled4 := "A:5|abc"
	marshalled5 := "A:5;B:3;C:10;D:7|this is a long payload"

	// Check parsing.
	_, err := Parse(marshalled1)
	if err.Error() != "Invalid sync message" {
		t.Fatalf("[comm.TestParse] Expected 'Invalid sync message' but received: '%s'\n", err.Error())
	}

	// Check parsing.
	_, err = Parse(marshalled2)
	if err.Error() != "Invalid vector clock element" {
		t.Fatalf("[comm.TestParse] Expected 'Invalid vector clock element' but received: '%s'\n", err.Error())
	}

	// Check parsing.
	_, err = Parse(marshalled3)
	if err.Error() != "Invalid number as element in vector clock" {
		t.Fatalf("[comm.TestParse] Expected 'Invalid number as element in vector clock' but received: '%s'\n", err.Error())
	}

	// Check parsing.
	msg4, err := Parse(marshalled4)
	if err != nil {
		t.Fatalf("[comm.TestParse] Expected nil error but received: '%s'\n", err.Error())
	}

	if msg4.vclock["A"] != 5 {
		t.Fatalf("[comm.TestParse] Expected value '5' at key 'A' but found: '%v'\n", msg4.vclock["A"])
	}

	if msg4.payload != "abc" {
		t.Fatalf("[comm.TestParse] Expected value 'abc' as payload but found: '%v'\n", msg4.payload)
	}

	// Check parsing.
	msg5, err := Parse(marshalled5)
	if err != nil {
		t.Fatalf("[comm.TestParse] Expected nil error but received: '%s'\n", err.Error())
	}

	for i, e := range msg5.vclock {

		switch i {

		case "A":
			if e != 5 {
				t.Fatalf("[comm.TestParse] Expected value '5' at key 'A' but found: '%v'\n", msg5.vclock["A"])
			}

		case "B":
			if e != 3 {
				t.Fatalf("[comm.TestParse] Expected value '3' at key 'B' but found: '%v'\n", msg5.vclock["B"])
			}

		case "C":
			if e != 10 {
				t.Fatalf("[comm.TestParse] Expected value '10' at key 'C' but found: '%v'\n", msg5.vclock["C"])
			}

		case "D":
			if e != 7 {
				t.Fatalf("[comm.TestParse] Expected value '7' at key 'D' but found: '%v'\n", msg5.vclock["D"])
			}
		}
	}

	if msg5.payload != "this is a long payload" {
		t.Fatalf("[comm.TestParse] Expected value 'this is a long payload' as payload but found: '%v'\n", msg5.payload)
	}
}
