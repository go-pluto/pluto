package comm

import (
	"testing"
)

// Functions

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
