package crdt

import (
	"math"
	"strings"
	"testing"
)

// Variables

var k1 string
var k2 string
var k3 string
var k4 string
var k5 string
var k6 string

var v1 bool
var v2 string
var v3 string
var v4 int
var v5 float32
var v6 float64
var v7 complex64
var v8 complex128

// Functions

func init() {

	// Keys to use in tests below.
	k1 = "1"
	k2 = "🕤🕤🕤🙉🙉🚀🚀🚀🚶🚶🆒™"
	k3 = "☕"
	k4 = "4"
	k5 = "5"
	k6 = "6"

	// Values to use in tests below.
	// TODO: Add test value with semicolons.
	v1 = true
	v2 = "Hey there, I am a test."
	v3 = "Sending ✉ around the 🌐: ✔"
	v4 = 666
	v5 = 12.34
	v6 = math.MaxFloat64
	v7 = 123456 + 200i
	v8 = (math.MaxFloat32 * 2i)
}

// TestLookup executes a white-box unit test
// on implemented Lookup() function.
func TestLookup(t *testing.T) {

	// Create new ORSet.
	s := InitORSet()

	// Make sure, set is initially empty.
	if len(s.elements) != 0 {
		t.Fatalf("[crdt.TestLookup] Expected set list to be empty initially, but len(s.elements) returned %d\n", len(s.elements))
	}

	// Set values in internal map and check
	// that they are reachable via Lookup().

	// v1
	if s.Lookup(v1, true) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' not to be in set but Lookup() returns true.\n", v1)
	}

	s.elements["10000000-a071-4227-9e63-a4b0ee84688f"] = v1

	if s.Lookup(v1, true) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' to be in set but Lookup() returns false.\n", v1)
	}

	// v2
	if s.Lookup(v2, true) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' not to be in set but Lookup() returns true.\n", v2)
	}

	s.elements["20000000-a071-4227-9e63-a4b0ee84688f"] = v2

	if s.Lookup(v2, true) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' to be in set but Lookup() returns false.\n", v2)
	}

	// v3
	if s.Lookup(v3, true) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' not to be in set but Lookup() returns true.\n", v3)
	}

	s.elements["30000000-a071-4227-9e63-a4b0ee84688f"] = v3

	if s.Lookup(v3, true) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' to be in set but Lookup() returns false.\n", v3)
	}

	// v4
	if s.Lookup(v4, true) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' not to be in set but Lookup() returns true.\n", v4)
	}

	s.elements["40000000-a071-4227-9e63-a4b0ee84688f"] = v4

	if s.Lookup(v4, true) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' to be in set but Lookup() returns false.\n", v4)
	}

	// v5
	if s.Lookup(v5, true) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' not to be in set but Lookup() returns true.\n", v5)
	}

	s.elements["50000000-a071-4227-9e63-a4b0ee84688f"] = v5

	if s.Lookup(v5, true) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' to be in set but Lookup() returns false.\n", v5)
	}

	// v6
	if s.Lookup(v6, true) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' not to be in set but Lookup() returns true.\n", v6)
	}

	s.elements["60000000-a071-4227-9e63-a4b0ee84688f"] = v6

	if s.Lookup(v6, true) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' to be in set but Lookup() returns false.\n", v6)
	}

	// v7
	if s.Lookup(v7, true) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' not to be in set but Lookup() returns true.\n", v7)
	}

	s.elements["70000000-a071-4227-9e63-a4b0ee84688f"] = v7

	if s.Lookup(v7, true) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' to be in set but Lookup() returns false.\n", v7)
	}

	// v8
	if s.Lookup(v8, true) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' not to be in set but Lookup() returns true.\n", v8)
	}

	s.elements["80000000-a071-4227-9e63-a4b0ee84688f"] = v8

	if s.Lookup(v8, true) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' to be in set but Lookup() returns false.\n", v8)
	}
}

// TestAddEffect executes a white-box unit test
// on implemented AddEffect() function.
func TestAddEffect(t *testing.T) {

	// Create new ORSet.
	s := InitORSet()

	// Set and test keys.

	// k1
	if value, found := s.elements[k1]; found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' not to be an active map key but found '%v' at that place.\n", k1, value)
	}

	s.AddEffect(v1, k1, true)

	if value, found := s.elements[k1]; !found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' to be an active map key and contain '%v' as value but found '%v' at that place.\n", k1, v1, value)
	}

	// k2
	if value, found := s.elements[k2]; found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' not to be an active map key but found '%v' at that place.\n", k2, value)
	}

	s.AddEffect(v3, k2, true)

	if value, found := s.elements[k2]; !found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' to be an active map key and contain '%v' as value but found '%v' at that place.\n", k2, v3, value)
	}

	// k3
	if value, found := s.elements[k3]; found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' not to be an active map key but found '%v' at that place.\n", k3, value)
	}

	s.AddEffect(v5, k3, true)

	if value, found := s.elements[k3]; !found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' to be an active map key and contain '%v' as value but found '%v' at that place.\n", k3, v5, value)
	}
}

// TestAdd executes a white-box unit test
// on implemented Add() function.
func TestAdd(t *testing.T) {

	// Use these variables to compare sent values.
	var msg1, msg2, msg3, msg4 string

	// Create new ORSet.
	s := InitORSet()

	// Add defined values to set.

	// v2
	if s.Lookup(v2, true) == true {
		t.Fatalf("[crdt.TestAdd] Expected '%v' not to be in set but Lookup() returns true.\n", v2)
	}

	s.Add(v2, func(payload string) { msg1 = payload })

	if s.Lookup(v2, true) != true {
		t.Fatalf("[crdt.TestAdd] Expected '%v' to be in set but Lookup() returns false.\n", v2)
	}

	// v4
	if s.Lookup(v4, true) == true {
		t.Fatalf("[crdt.TestAdd] Expected '%v' not to be in set but Lookup() returns true.\n", v4)
	}

	s.Add(v4, func(payload string) { msg2 = payload })

	if s.Lookup(v4, true) != true {
		t.Fatalf("[crdt.TestAdd] Expected '%v' to be in set but Lookup() returns false.\n", v4)
	}

	// v6
	if s.Lookup(v6, true) == true {
		t.Fatalf("[crdt.TestAdd] Expected '%v' not to be in set but Lookup() returns true.\n", v6)
	}

	s.Add(v6, func(payload string) { msg3 = payload })

	if s.Lookup(v6, true) != true {
		t.Fatalf("[crdt.TestAdd] Expected '%v' to be in set but Lookup() returns false.\n", v6)
	}

	// Check sent messages for length.
	// Minimal length = 'add' + '|' + '|' + 36 UUID chars = 41 chars.

	if len(msg1) < 41 {
		t.Fatalf("[crdt.TestAdd] Expected '%s' to be at least 41 characters long but only got %d many.\n", msg1, len(msg1))
	}

	if len(msg2) < 41 {
		t.Fatalf("[crdt.TestAdd] Expected '%s' to be at least 41 characters long but only got %d many.\n", msg2, len(msg2))
	}

	if len(msg3) < 41 {
		t.Fatalf("[crdt.TestAdd] Expected '%s' to be at least 41 characters long but only got %d many.\n", msg3, len(msg3))
	}

	// Check that sent messages only contain two semicolons.
	// This discovers possible non-escaped characters in payload.

	parts1 := strings.Split(msg1, "|")
	if len(parts1) != 3 {
		t.Fatalf("[crdt.TestAdd] Expected '%s' to contain exactly two semicolons but found %d instead.\n", msg1, len(parts1))
	}

	parts2 := strings.Split(msg2, "|")
	if len(parts2) != 3 {
		t.Fatalf("[crdt.TestAdd] Expected '%s' to contain exactly two semicolons but found %d instead.\n", msg2, len(parts2))
	}

	parts3 := strings.Split(msg3, "|")
	if len(parts3) != 3 {
		t.Fatalf("[crdt.TestAdd] Expected '%s' to contain exactly two semicolons but found %d instead.\n", msg3, len(parts3))
	}

	// Test second add of an element that is
	// already contained in set.

	s.Add(v2, func(payload string) { msg4 = payload })

	if len(s.elements) != 4 {
		t.Fatalf("[crdt.TestAdd] Expected set to contain exactly 4 elements but found %d instead.\n", len(s.elements))
	}

	if msg1 == msg4 {
		t.Fatalf("[crdt.TestAdd] Expected '%s' and '%s' not to be equal but comparison returned true.\n", msg1, msg4)
	}
}

// TestRemoveEffect executes a white-box unit test
// on implemented RemoveEffect() function.
func TestRemoveEffect(t *testing.T) {

	// Create new ORSet.
	s := InitORSet()

	// Create an empty remove set.
	testRSet := make(map[string]interface{})

	// In order to delete keys, we need to add some first.
	s.AddEffect(v2, k1, true)
	s.AddEffect(v3, k2, true)
	s.AddEffect(v4, k3, true)
	s.AddEffect(v2, k4, true)
	s.AddEffect(v2, k5, true)
	s.AddEffect(v2, k6, true)

	// Attempt to delete non-existing keys.
	s.RemoveEffect(testRSet, true)

	if len(s.elements) != 6 {
		t.Fatalf("[crdt.TestRemoveEffect] Expected 6 elements in set but only found %d.\n", len(s.elements))
	}

	if s.Lookup(v2, true) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.\n", v2)
	}

	if s.Lookup(v3, true) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.\n", v3)
	}

	if s.Lookup(v4, true) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.\n", v4)
	}

	// Now set one key which is not present in set.
	testRSet["0"] = v2

	// And try to remove that tag from the set.
	s.RemoveEffect(testRSet, true)

	if len(s.elements) != 6 {
		t.Fatalf("[crdt.TestRemoveEffect] Expected 6 elements in set but only found %d.\n", len(s.elements))
	}

	if s.Lookup(v2, true) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.\n", v2)
	}

	if s.Lookup(v3, true) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.\n", v3)
	}

	if s.Lookup(v4, true) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.\n", v4)
	}

	// Reset map and include an existing tag.
	testRSet = make(map[string]interface{})
	testRSet["1"] = v2

	// Remove all tags from set.
	s.RemoveEffect(testRSet, true)

	if len(s.elements) != 5 {
		t.Fatalf("[crdt.TestRemoveEffect] Expected 5 elements in set but only found %d.\n", len(s.elements))
	}

	if s.Lookup(v2, true) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.\n", v2)
	}

	if s.Lookup(v3, true) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.\n", v3)
	}

	if s.Lookup(v4, true) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.\n", v4)
	}

	// Now mark all tags for value v2 as to-be-removed.
	testRSet = make(map[string]interface{})
	testRSet["1"] = v2
	testRSet["4"] = v2
	testRSet["5"] = v2
	testRSet["6"] = v2

	// Remove all tags from set.
	s.RemoveEffect(testRSet, true)

	if len(s.elements) != 2 {
		t.Fatalf("[crdt.TestRemoveEffect] Expected 2 elements in set but only found %d.\n", len(s.elements))
	}

	if s.Lookup(v2, true) == true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' not to be in set but Lookup() returns true.\n", v2)
	}

	if s.Lookup(v3, true) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.\n", v3)
	}

	if s.Lookup(v4, true) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.\n", v4)
	}

	// Add one again.
	s.AddEffect(v2, k6, true)

	// And remove all again.
	s.RemoveEffect(testRSet, true)

	if len(s.elements) != 2 {
		t.Fatalf("[crdt.TestRemoveEffect] Expected 2 elements in set but only found %d.\n", len(s.elements))
	}

	if s.Lookup(v2, true) == true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' not to be in set but Lookup() returns true.\n", v2)
	}

	if s.Lookup(v3, true) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.\n", v3)
	}

	if s.Lookup(v4, true) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.\n", v4)
	}
}

// TestRemove executes a white-box unit test
// on implemented Remove() function.
func TestRemove(t *testing.T) {

	// Use these variables to compare sent values.
	var msg1, msg2 string //, msg3, msg4, msg5, msg6 string

	// Create new ORSet.
	s := InitORSet()

	// Attempt to delete non-existing value.
	err := s.Remove(v1, func(payload string) {})
	if err.Error() != "element to be removed not found in set" {
		t.Fatalf("[crdt.TestRemove] Expected Remove() to return error 'element to be removed not found in set' but received '%s'.\n", err.Error())
	}

	// In order to delete keys, we need to add some first.
	s.Add(v2, func(payload string) {})
	s.Add(v3, func(payload string) {})
	s.Add(v4, func(payload string) {})
	s.Add(v2, func(payload string) {})
	s.Add(v2, func(payload string) {})
	s.Add(v2, func(payload string) {})

	// Delete value that is only present once in set.
	err = s.Remove(v3, func(payload string) { msg1 = payload })
	if err != nil {
		t.Fatalf("[crdt.TestRemove] Expected Remove() to return nil error but received '%s'.\n", err.Error())
	}

	if len(s.elements) != 5 {
		t.Fatalf("[crdt.TestRemove] Expected 5 elements in set but only found %d.\n", len(s.elements))
	}

	if s.Lookup(v2, true) != true {
		t.Fatalf("[crdt.TestRemove] Expected '%v' to be in set but Lookup() returns false.\n", v2)
	}

	if s.Lookup(v3, true) == true {
		t.Fatalf("[crdt.TestRemove] Expected '%v' not to be in set but Lookup() returns true.\n", v3)
	}

	if s.Lookup(v4, true) != true {
		t.Fatalf("[crdt.TestRemove] Expected '%v' to be in set but Lookup() returns false.\n", v4)
	}

	// Split message at delimiter symbols and check for correct length.
	// This should discover unescaped delimiters in the payload.
	parts1 := strings.Split(msg1, "|")
	if len(parts1) != 3 {
		t.Fatalf("[crdt.TestRemove] Expected '%s' to contain exactly three pipe symbols but found %d instead.\n", msg1, len(parts1))
	}

	// Delete all tags corresponding to value v2.
	s.Remove(v2, func(payload string) { msg2 = payload })
	if err != nil {
		t.Fatalf("[crdt.TestRemove] Expected Remove() to return nil error but received '%s'.\n", err.Error())
	}

	if len(s.elements) != 1 {
		t.Fatalf("[crdt.TestRemove] Expected 1 elements in set but only found %d.\n", len(s.elements))
	}

	if s.Lookup(v2, true) == true {
		t.Fatalf("[crdt.TestRemove] Expected '%v' not to be in set but Lookup() returns true.\n", v2)
	}

	if s.Lookup(v3, true) == true {
		t.Fatalf("[crdt.TestRemove] Expected '%v' not to be in set but Lookup() returns true.\n", v3)
	}

	if s.Lookup(v4, true) != true {
		t.Fatalf("[crdt.TestRemove] Expected '%v' to be in set but Lookup() returns false.\n", v4)
	}

	// Split message at delimiter symbols and check for correct length.
	// This should discover unescaped delimiters in the payload.
	parts2 := strings.Split(msg2, "|")
	if len(parts2) != 9 {
		t.Fatalf("[crdt.TestRemove] Expected '%s' to contain exactly nine pipe symbols but found %d instead.\n", msg2, len(parts2))
	}
}
