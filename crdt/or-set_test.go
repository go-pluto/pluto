package crdt

import (
	"fmt"
	"math"
	"os"
	"strings"
	"testing"

	"io/ioutil"
)

// Variables

var (

	// Keys to use in tests below.
	k1 string = "1"
	k2 string = "🕤🕤🕤🙉🙉🚀🚀🚀🚶🚶🆒™"
	k3 string = "☕"
	k4 string = "4"
	k5 string = "5"
	k6 string = "6"

	// Values to use in tests below.
	v1 string = "true"
	v2 string = "Hey there, I am a test."
	v3 string = "Sending ✉ around the 🌐: ✔"
	v4 string = "666"
	v5 string = "12.34"
	v6 string = fmt.Sprintf("%g", math.MaxFloat64)
	v7 string = fmt.Sprintf("%g", (123456 + 200i))
	v8 string = fmt.Sprintf("%g", (math.MaxFloat32 * 2i))
)

// Functions

// TODO: Add missing tests for functions.

// TestInitORSetFromFile executes a white-box unit
// test on implemented InitORSetFromFile() function.
func TestInitORSetFromFile(t *testing.T) {

	// Delete temporary test file on function exit.
	defer os.Remove("test-crdt.log")

	// Test representations of file contents.
	marshalled1 := []byte("")
	marshalled2 := []byte(";\n")
	marshalled3 := []byte("A;B;C\n")
	marshalled4 := []byte("abc;1;def;2;ghi;3\n")
	marshalled5 := []byte("YWJj;1;ZGVm;2;Z2hp;3\n")

	// Write to temporary test file.
	err := ioutil.WriteFile("test-crdt.log", marshalled1, 0600)
	if err != nil {
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled1: Failed to write to temporary test file: %s\n", err.Error())
	}

	// Attempt to init ORSet from created file.
	_, err = InitORSetFromFile("test-crdt.log")
	if err != nil {
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled1: Expected InitORSetFromFile() not to fail for empty set but got: %s\n", err.Error())
	}

	// Write to temporary test file.
	err = ioutil.WriteFile("test-crdt.log", marshalled2, 0600)
	if err != nil {
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled2: Failed to write to temporary test file: %s\n", err.Error())
	}

	// Attempt to init ORSet from created file.
	_, err = InitORSetFromFile("test-crdt.log")
	if err != nil {
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled2: Expected InitORSetFromFile() not to fail but got: %s\n", err.Error())
	}

	// Write to temporary test file.
	err = ioutil.WriteFile("test-crdt.log", marshalled3, 0600)
	if err != nil {
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled3: Failed to write to temporary test file: %s\n", err.Error())
	}

	// Attempt to init ORSet from created file.
	_, err = InitORSetFromFile("test-crdt.log")
	if err.Error() != "odd number of elements in CRDT file 'test-crdt.log'\n" {
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled3: Expected 'odd number of elements in CRDT file 'test-crdt.log'' as error but received: %s", err.Error())
	}

	// Write to temporary test file.
	err = ioutil.WriteFile("test-crdt.log", marshalled4, 0600)
	if err != nil {
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled4: Failed to write to temporary test file: %s\n", err.Error())
	}

	// Attempt to init ORSet from created file.
	_, err = InitORSetFromFile("test-crdt.log")
	if err.Error() != "decoding base64 string in CRDT file 'test-crdt.log' failed: illegal base64 data at input byte 0\n" {
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled4: Expected 'decoding base64 string in CRDT file 'test-crdt.log' failed: illegal base64 data at input byte 0\n' as error but received: '%s'\n", err.Error())
	}

	// Write to temporary test file.
	err = ioutil.WriteFile("test-crdt.log", marshalled5, 0600)
	if err != nil {
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled5: Failed to write to temporary test file: %s\n", err.Error())
	}

	// Attempt to init ORSet from created file.
	s, err := InitORSetFromFile("test-crdt.log")

	// Check success.
	if err != nil {
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled5: Expected InitORSetFromFile() not to fail but got: %s\n", err.Error())
	}

	// Check correct unmarshalling.
	if len(s.elements) != 3 {
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled5: Expected exactly three elements in set but found: %d\n", len(s.elements))
	}

	if s.Lookup("abc", true) != true {
		t.Fatalf("[crdt.TestInitORSetFromFile] Expected 'abc' to be in set but Lookup() returns false.\n")
	}

	if s.Lookup("def", true) != true {
		t.Fatalf("[crdt.TestInitORSetFromFile] Expected 'def' to be in set but Lookup() returns false.\n")
	}

	if s.Lookup("ghi", true) != true {
		t.Fatalf("[crdt.TestInitORSetFromFile] Expected 'ghi' to be in set but Lookup() returns false.\n")
	}
}

// TestWriteORSetToFile executes a white-box unit test
// on implemented WriteORSetToFile() function.
func TestWriteORSetToFile(t *testing.T) {

	// Create a new ORSet.
	s := InitORSet()

	// Assign a corresponding file.
	f, err := os.OpenFile("test-crdt.log", (os.O_CREATE | os.O_RDWR), 0600)
	if err != nil {
		t.Fatalf("[crdt.TestWriteORSetToFile] Failed to create CRDT file 'test-crdt.log': %s\n", err.Error())
	}

	// Assign to ORSet and make sure to close
	// and remove when function exits.
	s.file = f
	defer s.file.Close()
	defer os.Remove("test-crdt.log")

	// Write current ORSet to file.
	err = s.WriteORSetToFile(true)
	if err != nil {
		t.Fatalf("[crdt.TestWriteORSetToFile] Expected WriteORSetToFile() not to fail but got: %s\n", err.Error())
	}

	// Verfiy correct file representation.
	contentsRaw, err := ioutil.ReadFile("test-crdt.log")
	if err != nil {
		t.Fatalf("[crdt.TestWriteORSetToFile] Could not read from just written CRDT log file 'test-crdt.log': %s\n", err.Error())
	}
	contents1 := string(contentsRaw)

	if contents1 != "" {
		t.Fatalf("[crdt.TestWriteORSetToFile] contents1: Expected '' but found: %s\n", contents1)
	}

	// Set a value in the set.
	err = s.AddEffect("abc", "1", true, true)
	if err != nil {
		t.Fatalf("[crdt.TestWriteORSetToFile] Expected AddEffect() not to fail but got: %s\n", err.Error())
	}

	// Verfiy correct file representation.
	contentsRaw, err = ioutil.ReadFile("test-crdt.log")
	if err != nil {
		t.Fatalf("[crdt.TestWriteORSetToFile] Could not read from just written CRDT log file 'test-crdt.log': %s\n", err.Error())
	}
	contents2 := string(contentsRaw)

	if contents2 != "YWJj;1" {
		t.Fatalf("[crdt.TestWriteORSetToFile] contents2: Expected 'YWJj;1' but found: %s\n", contents2)
	}

	// Set one more.
	err = s.AddEffect("def", "2", true, true)
	if err != nil {
		t.Fatalf("[crdt.TestWriteORSetToFile] Expected AddEffect() not to fail but got: %s\n", err.Error())
	}

	// Verfiy correct file representation.
	contentsRaw, err = ioutil.ReadFile("test-crdt.log")
	if err != nil {
		t.Fatalf("[crdt.TestWriteORSetToFile] Could not read from just written CRDT log file 'test-crdt.log': %s\n", err.Error())
	}
	contents3 := string(contentsRaw)

	if (contents3 != "YWJj;1;ZGVm;2") && (contents3 != "ZGVm;2;YWJj;1") {
		t.Fatalf("[crdt.TestWriteORSetToFile] contents3: Expected 'YWJj;1;ZGVm;2' or 'ZGVm;2;YWJj;1' but found: %s\n", contents3)
	}

	// And one last.
	err = s.AddEffect("ghi", "3", true, true)
	if err != nil {
		t.Fatalf("[crdt.TestWriteORSetToFile] Expected AddEffect() not to fail but got: %s\n", err.Error())
	}

	// Verfiy correct file representation.
	contentsRaw, err = ioutil.ReadFile("test-crdt.log")
	if err != nil {
		t.Fatalf("[crdt.TestWriteORSetToFile] Could not read from just written CRDT log file 'test-crdt.log': %s\n", err.Error())
	}
	contents4 := string(contentsRaw)

	if (contents4 != "YWJj;1;ZGVm;2;Z2hp;3") && (contents4 != "YWJj;1;Z2hp;3;ZGVm;2") &&
		(contents4 != "ZGVm;2;YWJj;1;Z2hp;3") && (contents4 != "ZGVm;2;Z2hp;3;YWJj;1") &&
		(contents4 != "Z2hp;3;YWJj;1;ZGVm;2") && (contents4 != "Z2hp;3;ZGVm;2;YWJj;1") {
		t.Fatalf("[crdt.TestWriteORSetToFile] contents4: Expected 'YWJj;1', 'ZGVm;2' and 'Z2hp;3' to be present but found: %s\n", contents4)
	}
}

// TestLookup executes a white-box unit test
// on implemented Lookup() function.
func TestLookup(t *testing.T) {

	// Delete temporary test file on function exit.
	defer os.Remove("test-crdt.log")

	// Create new ORSet with associated file.
	s, err := InitORSetWithFile("test-crdt.log")
	if err != nil {
		t.Fatalf("[crdt.TestLookup] Expected InitORSetWithFile() not to fail but got: %s\n", err.Error())
	}

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

	err := s.AddEffect(v1, k1, true, false)
	if err != nil {
		t.Fatalf("[crdt.TestAddEffect] Expected AddEffect() not to fail but got: %s\n", err.Error())
	}

	if value, found := s.elements[k1]; !found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' to be an active map key and contain '%v' as value but found '%v' at that place.\n", k1, v1, value)
	}

	// k2
	if value, found := s.elements[k2]; found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' not to be an active map key but found '%v' at that place.\n", k2, value)
	}

	err = s.AddEffect(v3, k2, true, false)
	if err != nil {
		t.Fatalf("[crdt.TestAddEffect] Expected AddEffect() not to fail but got: %s\n", err.Error())
	}

	if value, found := s.elements[k2]; !found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' to be an active map key and contain '%v' as value but found '%v' at that place.\n", k2, v3, value)
	}

	// k3
	if value, found := s.elements[k3]; found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' not to be an active map key but found '%v' at that place.\n", k3, value)
	}

	err = s.AddEffect(v5, k3, true, false)
	if err != nil {
		t.Fatalf("[crdt.TestAddEffect] Expected AddEffect() not to fail but got: %s\n", err.Error())
	}

	if value, found := s.elements[k3]; !found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' to be an active map key and contain '%v' as value but found '%v' at that place.\n", k3, v5, value)
	}
}

// TestAdd executes a white-box unit test
// on implemented Add() function.
func TestAdd(t *testing.T) {

	// Use these variables to compare sent values.
	var msg1, msg2, msg3, msg4 string

	// Delete temporary test file on function exit.
	defer os.Remove("test-crdt.log")

	// Create new ORSet with associated file.
	s, err := InitORSetWithFile("test-crdt.log")
	if err != nil {
		t.Fatalf("[crdt.TestAdd] Expected InitORSetWithFile() not to fail but got: %s\n", err.Error())
	}

	// Add defined values to set.

	// v2
	if s.Lookup(v2, true) == true {
		t.Fatalf("[crdt.TestAdd] Expected '%v' not to be in set but Lookup() returns true.\n", v2)
	}

	err = s.Add(v2, func(payload string) { msg1 = payload })
	if err != nil {
		t.Fatalf("[crdt.TestAdd] Expected Add() to return nil error but received '%s'.\n", err.Error())
	}

	if s.Lookup(v2, true) != true {
		t.Fatalf("[crdt.TestAdd] Expected '%v' to be in set but Lookup() returns false.\n", v2)
	}

	// v4
	if s.Lookup(v4, true) == true {
		t.Fatalf("[crdt.TestAdd] Expected '%v' not to be in set but Lookup() returns true.\n", v4)
	}

	err = s.Add(v4, func(payload string) { msg2 = payload })
	if err != nil {
		t.Fatalf("[crdt.TestAdd] Expected Add() to return nil error but received '%s'.\n", err.Error())
	}

	if s.Lookup(v4, true) != true {
		t.Fatalf("[crdt.TestAdd] Expected '%v' to be in set but Lookup() returns false.\n", v4)
	}

	// v6
	if s.Lookup(v6, true) == true {
		t.Fatalf("[crdt.TestAdd] Expected '%v' not to be in set but Lookup() returns true.\n", v6)
	}

	err = s.Add(v6, func(payload string) { msg3 = payload })
	if err != nil {
		t.Fatalf("[crdt.TestAdd] Expected Add() to return nil error but received '%s'.\n", err.Error())
	}

	if s.Lookup(v6, true) != true {
		t.Fatalf("[crdt.TestAdd] Expected '%v' to be in set but Lookup() returns false.\n", v6)
	}

	// Check sent messages for length.
	// Minimal length = ';' + 36 UUID chars = 37 chars.

	if len(msg1) < 37 {
		t.Fatalf("[crdt.TestAdd] Expected '%s' to be at least 37 characters long but only got %d many.\n", msg1, len(msg1))
	}

	if len(msg2) < 37 {
		t.Fatalf("[crdt.TestAdd] Expected '%s' to be at least 37 characters long but only got %d many.\n", msg2, len(msg2))
	}

	if len(msg3) < 37 {
		t.Fatalf("[crdt.TestAdd] Expected '%s' to be at least 37 characters long but only got %d many.\n", msg3, len(msg3))
	}

	// Check that sent messages only contain one semicolon.
	// This discovers possible non-escaped characters in payload.

	parts1 := strings.Split(msg1, ";")
	if len(parts1) != 2 {
		t.Fatalf("[crdt.TestAdd] Expected '%s' to contain exactly 1 semicolon but found %d instead.\n", msg1, (len(parts1) - 1))
	}

	parts2 := strings.Split(msg2, ";")
	if len(parts2) != 2 {
		t.Fatalf("[crdt.TestAdd] Expected '%s' to contain exactly 1 semicolon but found %d instead.\n", msg2, (len(parts2) - 1))
	}

	parts3 := strings.Split(msg3, ";")
	if len(parts3) != 2 {
		t.Fatalf("[crdt.TestAdd] Expected '%s' to contain exactly 1 semicolon but found %d instead.\n", msg3, (len(parts3) - 1))
	}

	// Test second add of an element that is
	// already contained in set.

	err = s.Add(v2, func(payload string) { msg4 = payload })
	if err != nil {
		t.Fatalf("[crdt.TestAdd] Expected Add() to return nil error but received '%s'.\n", err.Error())
	}

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
	testRSet := make(map[string]string)

	// In order to delete keys, we need to add some first.

	err := s.AddEffect(v2, k1, true, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected AddEffect() not to fail but got: %s\n", err.Error())
	}

	err = s.AddEffect(v3, k2, true, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected AddEffect() not to fail but got: %s\n", err.Error())
	}

	err = s.AddEffect(v4, k3, true, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected AddEffect() not to fail but got: %s\n", err.Error())
	}

	err = s.AddEffect(v2, k4, true, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected AddEffect() not to fail but got: %s\n", err.Error())
	}

	err = s.AddEffect(v2, k5, true, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected AddEffect() not to fail but got: %s\n", err.Error())
	}

	err = s.AddEffect(v2, k6, true, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected AddEffect() not to fail but got: %s\n", err.Error())
	}

	// Attempt to delete non-existing keys.
	err = s.RemoveEffect(testRSet, true, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected RemoveEffect() not to fail but got: %s\n", err.Error())
	}

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
	err = s.RemoveEffect(testRSet, true, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected RemoveEffect() not to fail but got: %s\n", err.Error())
	}

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
	testRSet = make(map[string]string)
	testRSet["1"] = v2

	// Remove all tags from set.
	err = s.RemoveEffect(testRSet, true, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected RemoveEffect() not to fail but got: %s\n", err.Error())
	}

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
	testRSet = make(map[string]string)
	testRSet["1"] = v2
	testRSet["4"] = v2
	testRSet["5"] = v2
	testRSet["6"] = v2

	// Remove all tags from set.
	err = s.RemoveEffect(testRSet, true, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected RemoveEffect() not to fail but got: %s\n", err.Error())
	}

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
	err = s.AddEffect(v2, k6, true, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected AddEffect() not to fail but got: %s\n", err.Error())
	}

	// And remove all again.
	err = s.RemoveEffect(testRSet, true, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected RemoveEffect() not to fail but got: %s\n", err.Error())
	}

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
	var msg1, msg2 string

	// Delete temporary test file on function exit.
	defer os.Remove("test-crdt.log")

	// Create new ORSet with associated file.
	s, err := InitORSetWithFile("test-crdt.log")
	if err != nil {
		t.Fatalf("[crdt.TestRemove] Expected InitORSetWithFile() not to fail but got: %s\n", err.Error())
	}

	// Attempt to delete non-existing value.
	err = s.Remove(v1, func(payload string) {})
	if err.Error() != "element to be removed not found in set" {
		t.Fatalf("[crdt.TestRemove] Expected Remove() to return error 'element to be removed not found in set' but received '%s'.\n", err.Error())
	}

	// In order to delete keys, we need to add some first.

	err = s.Add(v2, func(payload string) {})
	if err != nil {
		t.Fatalf("[crdt.TestRemove] Expected Add() to return nil error but received '%s'.\n", err.Error())
	}

	err = s.Add(v3, func(payload string) {})
	if err != nil {
		t.Fatalf("[crdt.TestRemove] Expected Add() to return nil error but received '%s'.\n", err.Error())
	}

	err = s.Add(v4, func(payload string) {})
	if err != nil {
		t.Fatalf("[crdt.TestRemove] Expected Add() to return nil error but received '%s'.\n", err.Error())
	}

	err = s.Add(v2, func(payload string) {})
	if err != nil {
		t.Fatalf("[crdt.TestRemove] Expected Add() to return nil error but received '%s'.\n", err.Error())
	}

	err = s.Add(v2, func(payload string) {})
	if err != nil {
		t.Fatalf("[crdt.TestRemove] Expected Add() to return nil error but received '%s'.\n", err.Error())
	}

	err = s.Add(v2, func(payload string) {})
	if err != nil {
		t.Fatalf("[crdt.TestRemove] Expected Add() to return nil error but received '%s'.\n", err.Error())
	}

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
	parts1 := strings.Split(msg1, ";")
	if len(parts1) != 2 {
		t.Fatalf("[crdt.TestRemove] Expected '%s' to contain exactly 2 semicola but found %d instead.\n", msg1, len(parts1))
	}

	// Delete all tags corresponding to value v2.
	err = s.Remove(v2, func(payload string) { msg2 = payload })
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
	parts2 := strings.Split(msg2, ";")
	if len(parts2) != 8 {
		t.Fatalf("[crdt.TestRemove] Expected '%s' to contain exactly 7 semicola but found %d instead.\n", msg2, (len(parts2) - 1))
	}
}
