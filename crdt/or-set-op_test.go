package crdt_test

import (
	"fmt"
	"log"
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

// TestParse executes a black-box unit test
// on implemented Parse() functionality.
func TestParse(t *testing.T) {

	// Marshalled representations of update messages.
	marshalled1 := ""
	marshalled2 := "||"
	marshalled3 := "remove||"
	marshalled4 := "rmv|||"
	marshalled5 := "rmv||"
	marshalled6 := "add|value|tag"
	marshalled7 := "rmv|12345|@|com"
	marshalled8 := "rmv|50.50|a|60.60|b|70.70|c"

	// Check parsing of incorrect marshalled messages.
	_, err := crdt.Parse(marshalled1)
	if err.Error() != "invalid CRDT update message found during parsing" {
		t.Fatalf("[crdt.TestParse] marshalled1: Expected 'invalid CRDT update message found during parsing' but received: '%s'\n", err.Error())
	}

	_, err = crdt.Parse(marshalled2)
	if err.Error() != "unsupported update operation specified in CRDT message" {
		t.Fatalf("[crdt.TestParse] marshalled2: Expected 'unsupported update operation specified in CRDT message' but received: '%s'\n", err.Error())
	}

	_, err = crdt.Parse(marshalled3)
	if err.Error() != "unsupported update operation specified in CRDT message" {
		t.Fatalf("[crdt.TestParse] marshalled3: Expected 'unsupported update operation specified in CRDT message' but received: '%s'\n", err.Error())
	}

	_, err = crdt.Parse(marshalled4)
	if err.Error() != "odd number of arguments, needs even one" {
		t.Fatalf("[crdt.TestParse] marshalled4: Expected 'odd number of arguments, needs even one' but received: '%s'\n", err.Error())
	}

	// Check parsing of correct but empty remove message.
	msg5, err := crdt.Parse(marshalled5)
	if err != nil {
		t.Fatalf("[crdt.TestParse] marshalled5: Expected nil error but received: '%s'\n", err.Error())
	}

	log.Printf("msg5: %v, len(args): %d\n", msg5, len(msg5.Arguments))

	if msg5.Operation != "rmv" {
		t.Fatalf("[crdt.TestParse] marshalled5: Expected 'rmv' as operation but found: '%s'\n", msg5.Operation)
	}

	if len(msg5.Arguments) != 1 {
		t.Fatalf("[crdt.TestParse] marshalled5: Expected exactly one tag-value-pair but found: '%d'\n", len(msg5.Arguments))
	}

	if _, found := msg5.Arguments[""]; found != true {
		t.Fatalf("[crdt.TestParse] marshalled5: Expected '' to be an existing key in arguments set but could not find it.\n")
	}

	if msg5.Arguments[""] != "" {
		t.Fatalf("[crdt.TestParse] marshalled5: Expected ''-value behind ''-tag but found: '%v'\n", msg5.Arguments[""])
	}

	// Check parsing of correct and complete add message.
	msg6, err := crdt.Parse(marshalled6)
	if err != nil {
		t.Fatalf("[crdt.TestParse] marshalled6: Expected nil error but received: '%s'\n", err.Error())
	}

	log.Printf("msg6: %v, len(args): %d, args: %v\n", msg6, len(msg6.Arguments), msg6.Arguments)

	if msg6.Operation != "add" {
		t.Fatalf("[crdt.TestParse] marshalled6: Expected 'add' as operation but found: '%s'\n", msg6.Operation)
	}

	if len(msg6.Arguments) != 1 {
		t.Fatalf("[crdt.TestParse] marshalled6: Expected exactly one tag-value-pair but found: '%d'\n", len(msg6.Arguments))
	}

	if _, found := msg6.Arguments["tag"]; found != true {
		t.Fatalf("[crdt.TestParse] marshalled6: Expected 'tag' to be an existing key in arguments set but could not find it.\n")
	}

	if msg6.Arguments["tag"] != "value" {
		t.Fatalf("[crdt.TestParse] marshalled6: Expected 'value' behind 'tag' but found: '%v'\n", msg6.Arguments["tag"])
	}

	// Check parsing of message with odd number of arguments.
	_, err = crdt.Parse(marshalled7)
	if err.Error() != "odd number of arguments, needs even one" {
		t.Fatalf("[crdt.TestParse] marshalled7: Expected 'odd number of arguments, needs even one' but received: '%s'\n", err.Error())
	}

	// Check parsing of correct and complete remove message.
	msg8, err := crdt.Parse(marshalled8)
	if err != nil {
		t.Fatalf("[crdt.TestParse] marshalled8: Expected nil error but received: '%s'\n", err.Error())
	}

	log.Printf("msg8: %v, len(args): %d, args: %v\n", msg8, len(msg8.Arguments), msg8.Arguments)

	if msg8.Operation != "rmv" {
		t.Fatalf("[crdt.TestParse] marshalled8: Expected 'rmv' as operation but found: '%s'\n", msg8.Operation)
	}

	if len(msg8.Arguments) != 3 {
		t.Fatalf("[crdt.TestParse] marshalled8: Expected exactly three tag-value-pairs but found: '%d'\n", len(msg8.Arguments))
	}

	if _, found := msg8.Arguments["a"]; found != true {
		t.Fatalf("[crdt.TestParse] marshalled8: Expected 'a' to be an existing key in arguments set but could not find it.\n")
	}

	if _, found := msg8.Arguments["b"]; found != true {
		t.Fatalf("[crdt.TestParse] marshalled8: Expected 'b' to be an existing key in arguments set but could not find it.\n")
	}

	if _, found := msg8.Arguments["c"]; found != true {
		t.Fatalf("[crdt.TestParse] marshalled8: Expected 'c' to be an existing key in arguments set but could not find it.\n")
	}

	if msg8.Arguments["a"] != "50.50" {
		t.Fatalf("[crdt.TestParse] marshalled8: Expected '50.50' behind 'a' but found: '%v'\n", msg8.Arguments["a"])
	}

	if msg8.Arguments["b"] != "60.60" {
		t.Fatalf("[crdt.TestParse] marshalled8: Expected '60.60' behind 'b' but found: '%v'\n", msg8.Arguments["b"])
	}

	if msg8.Arguments["c"] != "70.70" {
		t.Fatalf("[crdt.TestParse] marshalled8: Expected '70.70' behind 'c' but found: '%v'\n", msg8.Arguments["c"])
	}
}
