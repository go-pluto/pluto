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

	if s.Lookup(v1) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%s' not to be in set but Lookup() returns true.\n", v1)
	}
	s.elements["10000000-a071-4227-9e63-a4b0ee84688f"] = v1
	if s.Lookup(v1) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%s' to be in set but Lookup() returns false.\n", v1)
	}

	if s.Lookup(v2) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%s' not to be in set but Lookup() returns true.\n", v2)
	}
	s.elements["20000000-a071-4227-9e63-a4b0ee84688f"] = v2
	if s.Lookup(v2) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%s' to be in set but Lookup() returns false.\n", v2)
	}

	if s.Lookup(v3) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%s' not to be in set but Lookup() returns true.\n", v3)
	}
	s.elements["30000000-a071-4227-9e63-a4b0ee84688f"] = v3
	if s.Lookup(v3) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%s' to be in set but Lookup() returns false.\n", v3)
	}

	if s.Lookup(v4) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%s' not to be in set but Lookup() returns true.\n", v4)
	}
	s.elements["40000000-a071-4227-9e63-a4b0ee84688f"] = v4
	if s.Lookup(v4) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%s' to be in set but Lookup() returns false.\n", v4)
	}

	if s.Lookup(v5) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%s' not to be in set but Lookup() returns true.\n", v5)
	}
	s.elements["50000000-a071-4227-9e63-a4b0ee84688f"] = v5
	if s.Lookup(v5) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%s' to be in set but Lookup() returns false.\n", v5)
	}

	if s.Lookup(v6) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%s' not to be in set but Lookup() returns true.\n", v6)
	}
	s.elements["60000000-a071-4227-9e63-a4b0ee84688f"] = v6
	if s.Lookup(v6) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%s' to be in set but Lookup() returns false.\n", v6)
	}

	if s.Lookup(v7) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%s' not to be in set but Lookup() returns true.\n", v7)
	}
	s.elements["70000000-a071-4227-9e63-a4b0ee84688f"] = v7
	if s.Lookup(v7) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%s' to be in set but Lookup() returns false.\n", v7)
	}

	if s.Lookup(v8) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%s' not to be in set but Lookup() returns true.\n", v8)
	}
	s.elements["80000000-a071-4227-9e63-a4b0ee84688f"] = v8
	if s.Lookup(v8) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%s' to be in set but Lookup() returns false.\n", v8)
	}
}

// TestAddEffect executes a white-box unit test
// on implemented AddEffect() function.
func TestAddEffect(t *testing.T) {

	// Create new ORSet.
	s := InitORSet()

	// Set and test keys.

	if value, found := s.elements[k1]; found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' not to be an active map key but found '%v' at that place.\n", k1, value)
	}
	s.AddEffect(v1, k1)
	if value, found := s.elements[k1]; !found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' to be an active map key and contain '%v' as value but found '%v' at that place.\n", k1, v1, value)
	}

	if value, found := s.elements[k2]; found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' not to be an active map key but found '%v' at that place.\n", k2, value)
	}
	s.AddEffect(v3, k2)
	if value, found := s.elements[k2]; !found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' to be an active map key and contain '%v' as value but found '%v' at that place.\n", k2, v3, value)
	}

	if value, found := s.elements[k3]; found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' not to be an active map key but found '%v' at that place.\n", k3, value)
	}
	s.AddEffect(v5, k3)
	if value, found := s.elements[k3]; !found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' to be an active map key and contain '%v' as value but found '%v' at that place.\n", k3, v5, value)
	}
}

// TestAdd executes a white-box unit test
// on implemented Add() function.
func TestAdd(t *testing.T) {

	// Use these variables to compare sent values.
	var msg1, msg2, msg3 string

	// Create new ORSet.
	s := InitORSet()

	// Add defined values to set.

	if s.Lookup(v2) == true {
		t.Fatalf("[crdt.TestAdd] Expected '%s' not to be in set but Lookup() returns true.\n", v2)
	}
	s.Add(v2, func(payload string) {
		msg1 = payload
	})
	if s.Lookup(v2) != true {
		t.Fatalf("[crdt.TestAdd] Expected '%s' to be in set but Lookup() returns false.\n", v2)
	}

	if s.Lookup(v4) == true {
		t.Fatalf("[crdt.TestAdd] Expected '%s' not to be in set but Lookup() returns true.\n", v4)
	}
	s.Add(v4, func(payload string) {
		msg2 = payload
	})
	if s.Lookup(v4) != true {
		t.Fatalf("[crdt.TestAdd] Expected '%s' to be in set but Lookup() returns false.\n", v4)
	}

	if s.Lookup(v6) == true {
		t.Fatalf("[crdt.TestAdd] Expected '%s' not to be in set but Lookup() returns true.\n", v6)
	}
	s.Add(v6, func(payload string) {
		msg3 = payload
	})
	if s.Lookup(v6) != true {
		t.Fatalf("[crdt.TestAdd] Expected '%s' to be in set but Lookup() returns false.\n", v6)
	}

	// Check sent messages for length.
	// Minimal length = 'add' + ';' + ';' + 36 UUID chars = 41 chars.

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

	parts1 := strings.Split(msg1, ";")
	if len(parts1) != 3 {
		t.Fatalf("[crdt.TestAdd] Expected '%s' to contain exactly two semicolons but found %d instead.\n", msg1, len(parts1))
	}

	parts2 := strings.Split(msg2, ";")
	if len(parts2) != 3 {
		t.Fatalf("[crdt.TestAdd] Expected '%s' to contain exactly two semicolons but found %d instead.\n", msg2, len(parts2))
	}

	parts3 := strings.Split(msg3, ";")
	if len(parts3) != 3 {
		t.Fatalf("[crdt.TestAdd] Expected '%s' to contain exactly two semicolons but found %d instead.\n", msg3, len(parts3))
	}
}
