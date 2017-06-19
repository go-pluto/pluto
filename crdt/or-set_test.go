package crdt

import (
	"fmt"
	"math"
	"os"
	"testing"

	"io/ioutil"

	"github.com/stretchr/testify/assert"
)

// Variables

var (

	// Keys to use in tests below.
	k1 = "1"
	k2 = "🕤🕤🕤🙉🙉🚀🚀🚀🚶🚶🆒™"
	k3 = "☕"
	k4 = "4"
	k5 = "5"
	k6 = "6"

	// Values to use in tests below.
	v1 = "true"
	v2 = "Hey there, I am a test."
	v3 = "Sending ✉ around the 🌐: ✔"
	v4 = "666"
	v5 = "12.34"
	v6 = fmt.Sprintf("%g", math.MaxFloat64)
	v7 = fmt.Sprintf("%g", (123456 + 200i))
	v8 = fmt.Sprintf("%g", (math.MaxFloat32 * 2i))
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
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled1: Failed to write to temporary test file: %v", err)
	}

	// Attempt to init ORSet from created file.
	_, err = InitORSetFromFile("test-crdt.log")
	if err != nil {
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled1: Expected InitORSetFromFile() not to fail for empty set but got: %v", err)
	}

	// Write to temporary test file.
	err = ioutil.WriteFile("test-crdt.log", marshalled2, 0600)
	if err != nil {
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled2: Failed to write to temporary test file: %v", err)
	}

	// Attempt to init ORSet from created file.
	_, err = InitORSetFromFile("test-crdt.log")
	if err != nil {
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled2: Expected InitORSetFromFile() not to fail but got: %v", err)
	}

	// Write to temporary test file.
	err = ioutil.WriteFile("test-crdt.log", marshalled3, 0600)
	if err != nil {
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled3: Failed to write to temporary test file: %v", err)
	}

	// Attempt to init ORSet from created file.
	_, err = InitORSetFromFile("test-crdt.log")
	if err.Error() != "odd number of elements in CRDT file 'test-crdt.log'" {
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled3: Expected 'odd number of elements in CRDT file 'test-crdt.log'' as error but received: '%v'", err)
	}

	// Write to temporary test file.
	err = ioutil.WriteFile("test-crdt.log", marshalled4, 0600)
	if err != nil {
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled4: Failed to write to temporary test file: %v", err)
	}

	// Attempt to init ORSet from created file.
	_, err = InitORSetFromFile("test-crdt.log")
	if err.Error() != "decoding base64 string in CRDT file 'test-crdt.log' failed: illegal base64 data at input byte 0" {
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled4: Expected 'decoding base64 string in CRDT file 'test-crdt.log' failed: illegal base64 data at input byte 0' as error but received: '%v'", err)
	}

	// Write to temporary test file.
	err = ioutil.WriteFile("test-crdt.log", marshalled5, 0600)
	if err != nil {
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled5: Failed to write to temporary test file: %v", err)
	}

	// Attempt to init ORSet from created file.
	s, err := InitORSetFromFile("test-crdt.log")

	// Check success.
	if err != nil {
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled5: Expected InitORSetFromFile() not to fail but got: %v", err)
	}

	// Check correct unmarshalling.
	if len(s.elements) != 3 {
		t.Fatalf("[crdt.TestInitORSetFromFile] marshalled5: Expected exactly three elements in set but found: %d", len(s.elements))
	}

	if s.Lookup("abc") != true {
		t.Fatalf("[crdt.TestInitORSetFromFile] Expected 'abc' to be in set but Lookup() returns false.")
	}

	if s.Lookup("def") != true {
		t.Fatalf("[crdt.TestInitORSetFromFile] Expected 'def' to be in set but Lookup() returns false.")
	}

	if s.Lookup("ghi") != true {
		t.Fatalf("[crdt.TestInitORSetFromFile] Expected 'ghi' to be in set but Lookup() returns false.")
	}
}

// TestWriteORSetToFile executes a white-box unit test
// on implemented WriteORSetToFile() function.
func TestWriteORSetToFile(t *testing.T) {

	s := InitORSet()

	// Assign a corresponding file.
	f, err := os.OpenFile("test-crdt.log", (os.O_CREATE | os.O_RDWR), 0600)
	if err != nil {
		t.Fatalf("[crdt.TestWriteORSetToFile] Failed to create CRDT file 'test-crdt.log': %v", err)
	}

	// Assign to ORSet and make sure to close
	// and remove when function exits.
	s.file = f
	defer s.file.Close()
	defer os.Remove("test-crdt.log")

	// Write current ORSet to file.
	err = s.WriteORSetToFile()
	if err != nil {
		t.Fatalf("[crdt.TestWriteORSetToFile] Expected WriteORSetToFile() not to fail but got: %v", err)
	}

	// Verfiy correct file representation.
	contentsRaw, err := ioutil.ReadFile("test-crdt.log")
	if err != nil {
		t.Fatalf("[crdt.TestWriteORSetToFile] Could not read from just written CRDT log file 'test-crdt.log': %v", err)
	}
	contents1 := string(contentsRaw)

	if contents1 != "" {
		t.Fatalf("[crdt.TestWriteORSetToFile] contents1: Expected '' but found: %s", contents1)
	}

	// Set a value in the set.
	err = s.AddEffect("abc", "1", true)
	if err != nil {
		t.Fatalf("[crdt.TestWriteORSetToFile] Expected AddEffect() not to fail but got: %v", err)
	}

	// Verfiy correct file representation.
	contentsRaw, err = ioutil.ReadFile("test-crdt.log")
	if err != nil {
		t.Fatalf("[crdt.TestWriteORSetToFile] Could not read from just written CRDT log file 'test-crdt.log': %v", err)
	}
	contents2 := string(contentsRaw)

	if contents2 != "YWJj;1" {
		t.Fatalf("[crdt.TestWriteORSetToFile] contents2: Expected 'YWJj;1' but found: %s", contents2)
	}

	// Set one more.
	err = s.AddEffect("def", "2", true)
	if err != nil {
		t.Fatalf("[crdt.TestWriteORSetToFile] Expected AddEffect() not to fail but got: %v", err)
	}

	// Verfiy correct file representation.
	contentsRaw, err = ioutil.ReadFile("test-crdt.log")
	if err != nil {
		t.Fatalf("[crdt.TestWriteORSetToFile] Could not read from just written CRDT log file 'test-crdt.log': %v", err)
	}
	contents3 := string(contentsRaw)

	if (contents3 != "YWJj;1;ZGVm;2") && (contents3 != "ZGVm;2;YWJj;1") {
		t.Fatalf("[crdt.TestWriteORSetToFile] contents3: Expected 'YWJj;1;ZGVm;2' or 'ZGVm;2;YWJj;1' but found: %s", contents3)
	}

	// And one last.
	err = s.AddEffect("ghi", "3", true)
	if err != nil {
		t.Fatalf("[crdt.TestWriteORSetToFile] Expected AddEffect() not to fail but got: %v", err)
	}

	// Verfiy correct file representation.
	contentsRaw, err = ioutil.ReadFile("test-crdt.log")
	if err != nil {
		t.Fatalf("[crdt.TestWriteORSetToFile] Could not read from just written CRDT log file 'test-crdt.log': %v", err)
	}
	contents4 := string(contentsRaw)

	if (contents4 != "YWJj;1;ZGVm;2;Z2hp;3") && (contents4 != "YWJj;1;Z2hp;3;ZGVm;2") &&
		(contents4 != "ZGVm;2;YWJj;1;Z2hp;3") && (contents4 != "ZGVm;2;Z2hp;3;YWJj;1") &&
		(contents4 != "Z2hp;3;YWJj;1;ZGVm;2") && (contents4 != "Z2hp;3;ZGVm;2;YWJj;1") {
		t.Fatalf("[crdt.TestWriteORSetToFile] contents4: Expected 'YWJj;1', 'ZGVm;2' and 'Z2hp;3' to be present but found: %s", contents4)
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
		t.Fatalf("[crdt.TestLookup] Expected InitORSetWithFile() not to fail but got: %v", err)
	}

	// Make sure, set is initially empty.
	if len(s.elements) != 0 {
		t.Fatalf("[crdt.TestLookup] Expected set list to be empty initially, but len(s.elements) returned %d", len(s.elements))
	}

	// Set values in internal map and check
	// that they are reachable via Lookup().

	// v1
	if s.Lookup(v1) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' not to be in set but Lookup() returns true.", v1)
	}

	s.elements["10000000-a071-4227-9e63-a4b0ee84688f"] = v1

	if s.Lookup(v1) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' to be in set but Lookup() returns false.", v1)
	}

	// v2
	if s.Lookup(v2) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' not to be in set but Lookup() returns true.", v2)
	}

	s.elements["20000000-a071-4227-9e63-a4b0ee84688f"] = v2

	if s.Lookup(v2) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' to be in set but Lookup() returns false.", v2)
	}

	// v3
	if s.Lookup(v3) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' not to be in set but Lookup() returns true.", v3)
	}

	s.elements["30000000-a071-4227-9e63-a4b0ee84688f"] = v3

	if s.Lookup(v3) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' to be in set but Lookup() returns false.", v3)
	}

	// v4
	if s.Lookup(v4) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' not to be in set but Lookup() returns true.", v4)
	}

	s.elements["40000000-a071-4227-9e63-a4b0ee84688f"] = v4

	if s.Lookup(v4) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' to be in set but Lookup() returns false.", v4)
	}

	// v5
	if s.Lookup(v5) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' not to be in set but Lookup() returns true.", v5)
	}

	s.elements["50000000-a071-4227-9e63-a4b0ee84688f"] = v5

	if s.Lookup(v5) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' to be in set but Lookup() returns false.", v5)
	}

	// v6
	if s.Lookup(v6) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' not to be in set but Lookup() returns true.", v6)
	}

	s.elements["60000000-a071-4227-9e63-a4b0ee84688f"] = v6

	if s.Lookup(v6) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' to be in set but Lookup() returns false.", v6)
	}

	// v7
	if s.Lookup(v7) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' not to be in set but Lookup() returns true.", v7)
	}

	s.elements["70000000-a071-4227-9e63-a4b0ee84688f"] = v7

	if s.Lookup(v7) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' to be in set but Lookup() returns false.", v7)
	}

	// v8
	if s.Lookup(v8) == true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' not to be in set but Lookup() returns true.", v8)
	}

	s.elements["80000000-a071-4227-9e63-a4b0ee84688f"] = v8

	if s.Lookup(v8) != true {
		t.Fatalf("[crdt.TestLookup] Expected '%v' to be in set but Lookup() returns false.", v8)
	}
}

func benchmarkLookup(b *testing.B, e string) {

	s := InitORSet()

	s.elements["10000000-a071-4227-9e63-a4b0ee84688f"] = v1
	s.elements["20000000-a071-4227-9e63-a4b0ee84688f"] = v2
	s.elements["30000000-a071-4227-9e63-a4b0ee84688f"] = v3
	s.elements["40000000-a071-4227-9e63-a4b0ee84688f"] = v4
	s.elements["50000000-a071-4227-9e63-a4b0ee84688f"] = v5
	s.elements["60000000-a071-4227-9e63-a4b0ee84688f"] = v6
	s.elements["70000000-a071-4227-9e63-a4b0ee84688f"] = v7
	s.elements["80000000-a071-4227-9e63-a4b0ee84688f"] = v8

	for i := 0; i < b.N; i++ {
		s.Lookup(e)
	}
}

// Benchmark lookup() with value 1.
func BenchmarkLookup1(b *testing.B) { benchmarkLookup(b, v1) }

// Benchmark lookup() with value 2.
func BenchmarkLookup2(b *testing.B) { benchmarkLookup(b, v2) }

// Benchmark lookup() with value 3.
func BenchmarkLookup3(b *testing.B) { benchmarkLookup(b, v3) }

// Benchmark lookup() with value 4.
func BenchmarkLookup4(b *testing.B) { benchmarkLookup(b, v4) }

// Benchmark lookup() with value 5.
func BenchmarkLookup5(b *testing.B) { benchmarkLookup(b, v5) }

// Benchmark lookup() with value 6.
func BenchmarkLookup6(b *testing.B) { benchmarkLookup(b, v6) }

// Benchmark lookup() with value 7.
func BenchmarkLookup7(b *testing.B) { benchmarkLookup(b, v7) }

// Benchmark lookup() with value 8.
func BenchmarkLookup8(b *testing.B) { benchmarkLookup(b, v8) }

// TestAddEffect executes a white-box unit test
// on implemented AddEffect() function.
func TestAddEffect(t *testing.T) {

	s := InitORSet()

	// Set and test keys.

	// k1
	if value, found := s.elements[k1]; found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' not to be an active map key but found '%v' at that place.", k1, value)
	}

	err := s.AddEffect(v1, k1, false)
	if err != nil {
		t.Fatalf("[crdt.TestAddEffect] Expected AddEffect() not to fail but got: %v", err)
	}

	if value, found := s.elements[k1]; !found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' to be an active map key and contain '%v' as value but found '%v' at that place.", k1, v1, value)
	}

	// k2
	if value, found := s.elements[k2]; found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' not to be an active map key but found '%v' at that place.", k2, value)
	}

	err = s.AddEffect(v3, k2, false)
	if err != nil {
		t.Fatalf("[crdt.TestAddEffect] Expected AddEffect() not to fail but got: %v", err)
	}

	if value, found := s.elements[k2]; !found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' to be an active map key and contain '%v' as value but found '%v' at that place.", k2, v3, value)
	}

	// k3
	if value, found := s.elements[k3]; found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' not to be an active map key but found '%v' at that place.", k3, value)
	}

	err = s.AddEffect(v5, k3, false)
	if err != nil {
		t.Fatalf("[crdt.TestAddEffect] Expected AddEffect() not to fail but got: %v", err)
	}

	if value, found := s.elements[k3]; !found {
		t.Fatalf("[crdt.TestAddEffect] Expected '%s' to be an active map key and contain '%v' as value but found '%v' at that place.", k3, v5, value)
	}
}

// TestAdd executes a white-box unit test
// on implemented Add() function.
func TestAdd(t *testing.T) {

	// Use these variables to compare sent values.
	var msg1, msg2, msg3, msg4 []string

	// Delete temporary test file on function exit.
	defer os.Remove("test-crdt.log")

	// Create new ORSet with associated file.
	s, err := InitORSetWithFile("test-crdt.log")
	if err != nil {
		t.Fatalf("[crdt.TestAdd] Expected InitORSetWithFile() not to fail but got: %v", err)
	}

	// Add defined values to set.

	// v2.
	assert.Equalf(t, false, s.Lookup(v2), "[crdt.TestAdd] Expected '%v' not to be in set but Lookup() returns true", v2)

	err = s.Add(v2, func(args ...string) { msg1 = args })
	assert.Nilf(t, err, "[crdt.TestAdd] Expected Add() to return nil error but received: %v", err)

	assert.Equalf(t, true, s.Lookup(v2), "[crdt.TestAdd] Expected '%v' to be in set but Lookup() returns false", v2)

	// v4.
	assert.Equalf(t, false, s.Lookup(v4), "[crdt.TestAdd] Expected '%v' not to be in set but Lookup() returns true", v4)

	err = s.Add(v4, func(args ...string) { msg2 = args })
	assert.Nilf(t, err, "[crdt.TestAdd] Expected Add() to return nil error but received: %v", err)

	assert.Equalf(t, true, s.Lookup(v4), "[crdt.TestAdd] Expected '%v' to be in set but Lookup() returns false", v4)

	// v6.
	assert.Equalf(t, false, s.Lookup(v6), "[crdt.TestAdd] Expected '%v' not to be in set but Lookup() returns true", v6)

	err = s.Add(v6, func(args ...string) { msg3 = args })
	assert.Nilf(t, err, "[crdt.TestAdd] Expected Add() to return nil error but received: %v", err)

	assert.Equalf(t, true, s.Lookup(v6), "[crdt.TestAdd] Expected '%v' to be in set but Lookup() returns false", v6)

	// Check received arguments.

	assert.Equal(t, 2, len(msg1), "[crdt.TestAdd] msg1 should be of length 2")
	assert.Equal(t, 2, len(msg2), "[crdt.TestAdd] msg2 should be of length 2")
	assert.Equal(t, 2, len(msg3), "[crdt.TestAdd] msg3 should be of length 2")

	assert.Equalf(t, v2, msg1[0], "[crdt.TestAdd] msg1[0] = '%v' should be equal to v2 = '%v'", msg1[0], v2)
	assert.Equalf(t, v4, msg2[0], "[crdt.TestAdd] msg2[0] = '%v' should be equal to v4 = '%v'", msg2[0], v4)
	assert.Equalf(t, v6, msg3[0], "[crdt.TestAdd] msg3[0] = '%v' should be equal to v6 = '%v'", msg3[0], v6)

	assert.Equalf(t, 36, len(msg1[1]), "[crdt.TestAdd] Expected tag of msg1 = '%s' to be of length 36 but was %d", msg1[1], len(msg1[1]))
	assert.Equalf(t, 36, len(msg2[1]), "[crdt.TestAdd] Expected tag of msg2 = '%s' to be of length 36 but was %d", msg2[1], len(msg2[1]))
	assert.Equalf(t, 36, len(msg3[1]), "[crdt.TestAdd] Expected tag of msg3 = '%s' to be of length 36 but was %d", msg3[1], len(msg3[1]))

	// Test second add of an element that is
	// already contained in set.

	err = s.Add(v2, func(args ...string) { msg4 = args })
	assert.Nilf(t, err, "[crdt.TestAdd] Expected Add() to return nil error but received: %v", err)

	assert.Equalf(t, 4, len(s.elements), "[crdt.TestAdd] Expected set to contain exactly 4 elements but found %d instead", len(s.elements))
	assert.Equalf(t, msg1[0], msg4[0], "[crdt.TestAdd] Expected values of msg1 and msg4 to be equal but '%s' != '%s'", msg1[0], msg4[0])
	assert.NotEqualf(t, msg1[1], msg4[1], "[crdt.TestAdd] Expected tags of msg1 and msg4 not to be equal but '%s' == '%s'", msg1[1], msg4[1])
}

// TestRemoveEffect executes a white-box unit test
// on implemented RemoveEffect() function.
func TestRemoveEffect(t *testing.T) {

	s := InitORSet()

	// Create an empty remove set.
	testRSet := make(map[string]string)

	// In order to delete keys, we need to add some first.

	err := s.AddEffect(v2, k1, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected AddEffect() not to fail but got: %v", err)
	}

	err = s.AddEffect(v3, k2, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected AddEffect() not to fail but got: %v", err)
	}

	err = s.AddEffect(v4, k3, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected AddEffect() not to fail but got: %v", err)
	}

	err = s.AddEffect(v2, k4, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected AddEffect() not to fail but got: %v", err)
	}

	err = s.AddEffect(v2, k5, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected AddEffect() not to fail but got: %v", err)
	}

	err = s.AddEffect(v2, k6, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected AddEffect() not to fail but got: %v", err)
	}

	// Attempt to delete non-existing keys.
	err = s.RemoveEffect(testRSet, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected RemoveEffect() not to fail but got: %v", err)
	}

	if len(s.elements) != 6 {
		t.Fatalf("[crdt.TestRemoveEffect] Expected 6 elements in set but only found %d.", len(s.elements))
	}

	if s.Lookup(v2) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.", v2)
	}

	if s.Lookup(v3) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.", v3)
	}

	if s.Lookup(v4) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.", v4)
	}

	// Now set one key which is not present in set.
	testRSet["0"] = v2

	// And try to remove that tag from the set.
	err = s.RemoveEffect(testRSet, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected RemoveEffect() not to fail but got: %v", err)
	}

	if len(s.elements) != 6 {
		t.Fatalf("[crdt.TestRemoveEffect] Expected 6 elements in set but only found %d.", len(s.elements))
	}

	if s.Lookup(v2) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.", v2)
	}

	if s.Lookup(v3) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.", v3)
	}

	if s.Lookup(v4) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.", v4)
	}

	// Reset map and include an existing tag.
	testRSet = make(map[string]string)
	testRSet["1"] = v2

	// Remove all tags from set.
	err = s.RemoveEffect(testRSet, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected RemoveEffect() not to fail but got: %v", err)
	}

	if len(s.elements) != 5 {
		t.Fatalf("[crdt.TestRemoveEffect] Expected 5 elements in set but only found %d.", len(s.elements))
	}

	if s.Lookup(v2) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.", v2)
	}

	if s.Lookup(v3) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.", v3)
	}

	if s.Lookup(v4) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.", v4)
	}

	// Now mark all tags for value v2 as to-be-removed.
	testRSet = make(map[string]string)
	testRSet["1"] = v2
	testRSet["4"] = v2
	testRSet["5"] = v2
	testRSet["6"] = v2

	// Remove all tags from set.
	err = s.RemoveEffect(testRSet, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected RemoveEffect() not to fail but got: %v", err)
	}

	if len(s.elements) != 2 {
		t.Fatalf("[crdt.TestRemoveEffect] Expected 2 elements in set but only found %d.", len(s.elements))
	}

	if s.Lookup(v2) == true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' not to be in set but Lookup() returns true.", v2)
	}

	if s.Lookup(v3) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.", v3)
	}

	if s.Lookup(v4) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.", v4)
	}

	// Add one again.
	err = s.AddEffect(v2, k6, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected AddEffect() not to fail but got: %v", err)
	}

	// And remove all again.
	err = s.RemoveEffect(testRSet, false)
	if err != nil {
		t.Fatalf("[crdt.TestRemoveEffect] Expected RemoveEffect() not to fail but got: %v", err)
	}

	if len(s.elements) != 2 {
		t.Fatalf("[crdt.TestRemoveEffect] Expected 2 elements in set but only found %d.", len(s.elements))
	}

	if s.Lookup(v2) == true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' not to be in set but Lookup() returns true.", v2)
	}

	if s.Lookup(v3) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.", v3)
	}

	if s.Lookup(v4) != true {
		t.Fatalf("[crdt.TestRemoveEffect] Expected '%v' to be in set but Lookup() returns false.", v4)
	}
}

// TestRemove executes a white-box unit test
// on implemented Remove() function.
func TestRemove(t *testing.T) {

	// Use these variables to compare sent values.
	var msg1, msg2 []string

	// Delete temporary test file on function exit.
	defer os.Remove("test-crdt.log")

	// Create new ORSet with associated file.
	s, err := InitORSetWithFile("test-crdt.log")
	assert.Nilf(t, err, "[crdt.TestRemove] Expected InitORSetWithFile() not to fail but got: %v", err)

	// Attempt to delete non-existing value.
	err = s.Remove(v1, func(args ...string) {})
	assert.Equal(t, "element to be removed not found in set", err.Error(), "[crdt.TestRemove] Expected Remove() to return error 'element to be removed not found in set' but received '%v'", err)

	// In order to delete keys, we need to add some first.

	err = s.Add(v2, func(args ...string) {})
	assert.Nilf(t, err, "[crdt.TestRemove] Expected Add() to return nil error but received: %v", err)

	err = s.Add(v3, func(args ...string) {})
	assert.Nilf(t, err, "[crdt.TestRemove] Expected Add() to return nil error but received: %v", err)

	err = s.Add(v4, func(args ...string) {})
	assert.Nilf(t, err, "[crdt.TestRemove] Expected Add() to return nil error but received: %v", err)

	err = s.Add(v2, func(args ...string) {})
	assert.Nilf(t, err, "[crdt.TestRemove] Expected Add() to return nil error but received: %v", err)

	err = s.Add(v2, func(args ...string) {})
	assert.Nilf(t, err, "[crdt.TestRemove] Expected Add() to return nil error but received: %v", err)

	err = s.Add(v2, func(args ...string) {})
	assert.Nilf(t, err, "[crdt.TestRemove] Expected Add() to return nil error but received: %v", err)

	// Delete value that is only present once in set.
	err = s.Remove(v3, func(args ...string) { msg1 = args })
	assert.Nilf(t, err, "[crdt.TestRemove] Expected Remove() to return nil error but received: %v", err)

	assert.Equalf(t, 5, len(s.elements), "[crdt.TestRemove] Expected 5 elements in set but only found %d", len(s.elements))

	assert.Equalf(t, true, s.Lookup(v2), "[crdt.TestRemove] Expected '%v' to be in set but Lookup() returns false", v2)
	assert.Equalf(t, false, s.Lookup(v3), "[crdt.TestRemove] Expected '%v' not to be in set but Lookup() returns true", v3)
	assert.Equalf(t, true, s.Lookup(v4), "[crdt.TestRemove] Expected '%v' to be in set but Lookup() returns false", v4)

	assert.Equalf(t, 2, len(msg1), "[crdt.TestRemove] Expected msg1 to contain exactly 2 elements but found %d", len(msg1))

	// Delete all tags corresponding to value v2.
	err = s.Remove(v2, func(args ...string) { msg2 = args })
	assert.Nilf(t, err, "[crdt.TestRemove] Expected Remove() to return nil error but received: %v", err)

	assert.Equalf(t, 1, len(s.elements), "[crdt.TestRemove] Expected 1 element in set but found %d", len(s.elements))
	assert.Equalf(t, false, s.Lookup(v2), "[crdt.TestRemove] Expected '%v' not to be in set but Lookup() returns true", v2)
	assert.Equalf(t, false, s.Lookup(v3), "[crdt.TestRemove] Expected '%v' not to be in set but Lookup() returns true", v3)
	assert.Equalf(t, true, s.Lookup(v4), "[crdt.TestRemove] Expected '%v' to be in set but Lookup() returns false", v4)

	assert.Equalf(t, 8, len(msg2), "[crdt.TestRemove] Expected msg2 to contain exactly 8 elements but found %d", len(msg2))
}
