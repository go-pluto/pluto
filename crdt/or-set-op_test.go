package crdt_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/numbleroot/pluto/crdt"
)

// Functions

// TestString executes a black-box unit test
// on implemented String() functionality.
func TestString(t *testing.T) {

	// Create a new CRDT update operation message.
	msg := crdt.InitORSetOp()

	// Check empty message marshalling.
	marshalled := msg.String()
	if marshalled != "" {
		t.Fatalf("[crdt.TestString] Expected '' as marshalled representation of empty CRDT message but got: '%s'\n", marshalled)
	}

	// Set operation part of update message.
	msg.Operation = "add"

	// Check marshalling.
	marshalled = msg.String()
	if marshalled != "add" {
		t.Fatalf("[crdt.TestString] Expected 'add' as marshalled representation of CRDT message but got: '%s'\n", marshalled)
	}

	// Set empty argument.
	msg.Arguments[""] = ""

	// Check marshalling.
	marshalled = msg.String()
	if marshalled != "add||" {
		t.Fatalf("[crdt.TestString] Expected 'add||' as marshalled representation of CRDT message but got: '%s'\n", marshalled)
	}

	// Reset msg and set a correct argument pair.
	msg.Arguments = make(map[string]interface{})
	msg.Arguments["1"] = "test string"

	// Check marshalling.
	marshalled = msg.String()
	if marshalled != "add|test string|1" {
		t.Fatalf("[crdt.TestString] Expected 'add|test string|1' as marshalled representation of CRDT message but got: '%s'\n", marshalled)
	}

	// Reset msg and set operation to remove.
	msg = crdt.InitORSetOp()
	msg.Operation = "rmv"

	// Set multiple arguments.
	msg.Arguments["1"] = true
	msg.Arguments["2"] = "test remove message payload"

	// Check marshalling.
	marshalled = msg.String()
	expected1 := fmt.Sprintf("rmv|true|1|test remove message payload|2")
	expected2 := fmt.Sprintf("rmv|test remove message payload|2|true|1")
	if (marshalled != expected1) && (marshalled != expected2) {
		t.Fatalf("[crdt.TestString] Expected '%s' or '%s' as marshalled representation of CRDT message but got: '%s'\n", expected1, expected2, marshalled)
	}

	// Reset arguments and test more types.
	msg.Arguments = make(map[string]interface{})
	msg.Arguments["3"] = 99
	msg.Arguments["4"] = 0.25

	// Check marshalling.
	marshalled = msg.String()
	expected1 = fmt.Sprintf("rmv|99|3|0.25|4")
	expected2 = fmt.Sprintf("rmv|0.25|4|99|3")
	if (marshalled != expected1) && (marshalled != expected2) {
		t.Fatalf("[crdt.TestString] Expected '%s' or '%s' as marshalled representation of CRDT message but got: '%s'\n", expected1, expected2, marshalled)
	}

	// Reset arguments and test more types.
	msg.Arguments = make(map[string]interface{})
	msg.Arguments["5"] = math.MaxFloat64
	msg.Arguments["6"] = 10 * 10i

	// Check marshalling.
	marshalled = msg.String()
	expected1 = fmt.Sprintf("rmv|%v|5|%v|6", math.MaxFloat64, (10 * 10i))
	expected2 = fmt.Sprintf("rmv|%v|6|%v|5", (10 * 10i), math.MaxFloat64)
	if (marshalled != expected1) && (marshalled != expected2) {
		t.Fatalf("[crdt.TestString] Expected '%s' or '%s' as marshalled representation of CRDT message but got: '%s'\n", expected1, expected2, marshalled)
	}

	// Reset arguments and test remaining type.
	msg.Arguments = make(map[string]interface{})
	msg.Arguments["7"] = (math.MaxFloat32 + 1i)

	// Check marshalling.
	marshalled = msg.String()
	expected1 = fmt.Sprintf("rmv|%v|7", (math.MaxFloat32 + 1i))
	if marshalled != expected1 {
		t.Fatalf("[crdt.TestString] Expected '%s' as marshalled representation of CRDT message but got: '%s'\n", expected1, marshalled)
	}
}
