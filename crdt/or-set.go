package crdt

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"encoding/base64"
	"io/ioutil"

	"github.com/satori/go.uuid"
)

// Structs

// ORSet conforms to the specification of an observed-
// removed set defined by Shapiro, Preguiça, Baquero,
// and Zawirski. It consists of unique IDs and data items.
type ORSet struct {
	File     *os.File
	Elements map[string]string
}

// sendFunc is used as a parameter to below defined
// AddSendDownstream function that broadcasts an update
// payload to all other replicas.
type sendFunc func(...string)

// Functions

// InitORSetWithFile takes in a file name and initializes
// a new ORSet with opened file handler to that name as
// designated log file.
func InitORSetWithFile(fileName string) (*ORSet, error) {

	// Attempt to create a new CRDT file.
	f, err := os.Create(fileName)
	if err != nil {
		return nil, fmt.Errorf("opening CRDT file '%s' failed with: %v", fileName, err)
	}

	// Change permissions.
	err = f.Chmod(0600)
	if err != nil {
		return nil, fmt.Errorf("changing permissions of CRDT file '%s' failed with: %v", fileName, err)
	}

	// Init an empty ORSet.
	s := &ORSet{
		Elements: make(map[string]string),
	}
	s.File = f

	// Write newly created CRDT file to stable storage.
	err = s.WriteORSetToFile()
	if err != nil {
		return nil, fmt.Errorf("error during CRDT file write-back: %v", err)
	}

	return s, nil
}

// InitORSetFromFile parses an ORSet found in the
// supplied file and returns it, initialized with
// elements saved in file.
func InitORSetFromFile(fileName string) (*ORSet, error) {

	// Attempt to open CRDT file and assign to set afterwards.
	f, err := os.OpenFile(fileName, os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("opening CRDT file '%s' failed with: %v", fileName, err)
	}

	// Init an empty ORSet.
	s := &ORSet{
		Elements: make(map[string]string),
	}
	s.File = f

	// Parse contained CRDT state from file.
	contentsRaw, err := ioutil.ReadAll(s.File)
	if err != nil {
		return nil, fmt.Errorf("reading all contents from CRDT file '%s' failed with: %v", fileName, err)
	}
	contents := string(contentsRaw)

	// Account for an empty CRDT set which is valid.
	if contents == "" {
		return s, nil
	}

	// Split content at each ';' (semicolon).
	parts := strings.Split(contents, ";")

	// Check even number of elements.
	if (len(parts) % 2) != 0 {
		return nil, fmt.Errorf("odd number of elements in CRDT file '%s'", fileName)
	}

	// Range over all value-tag-pairs.
	for value := 0; value < len(parts); value += 2 {

		tag := value + 1

		// Decode string from base64.
		decValue, err := base64.StdEncoding.DecodeString(parts[value])
		if err != nil {
			return nil, fmt.Errorf("decoding base64 string in CRDT file '%s' failed: %v", fileName, err)
		}

		// Assign decoded value to corresponding
		// tag in elements set.
		s.Elements[parts[tag]] = string(decValue)
	}

	return s, nil
}

// WriteORSetToFile saves an active ORSet onto
// stable storage at location from initialization.
// This allows for a CRDT ORSet to be made persistent
// and later be resumed from prior state.
func (s *ORSet) WriteORSetToFile() error {

	marshalled := ""

	for tag, valueRaw := range s.Elements {

		// Encode value in base64 encoding.
		value := base64.StdEncoding.EncodeToString([]byte(valueRaw))

		// Append value and tag to write-out file.
		if marshalled == "" {
			marshalled = fmt.Sprintf("%v;%s", value, tag)
		} else {
			marshalled = fmt.Sprintf("%s;%v;%s", marshalled, value, tag)
		}
	}

	// Reset position in file to beginning.
	_, err := s.File.Seek(0, os.SEEK_SET)
	if err != nil {
		return fmt.Errorf("error while setting head back to beginning in CRDT file '%s': %v", s.File.Name(), err)
	}

	// Write marshalled set to file.
	newNumOfBytes, err := s.File.WriteString(marshalled)
	if err != nil {
		return fmt.Errorf("failed to write ORSet contents to file '%s': %v", s.File.Name(), err)
	}

	// Adjust file size to just written length of string.
	err = s.File.Truncate(int64(newNumOfBytes))
	if err != nil {
		return fmt.Errorf("error while truncating CRDT file '%s' to new size: %v", s.File.Name(), err)
	}

	// Save to stable storage.
	err = s.File.Sync()
	if err != nil {
		return fmt.Errorf("could not synchronise CRDT file '%s' contents to stable storage: %v", s.File.Name(), err)
	}

	return nil
}

// GetAllValues returns all distinct values
// of a supplied ORSet.
func (s *ORSet) GetAllValues() []string {

	// Make a slice of initial size 0.
	allValues := make([]string, 0)

	// Also prepare a map to store which elements
	// we already considered.
	seenValues := make(map[string]bool)

	for _, value := range s.Elements {

		// Check if we did not yet consider this value.
		_, seen := seenValues[value]
		if seen != true {

			// If so, append it and set seen value to true.
			allValues = append(allValues, value)
			seenValues[value] = true
		}
	}

	// Sort slice of strings.
	sort.Strings(allValues)

	return allValues
}

// Lookup cycles through elements in ORSet and
// returns true if element e is present and
// false otherwise.
func (s *ORSet) Lookup(e string) bool {

	for _, value := range s.Elements {

		// When we find the value while iterating
		// through set, we return true and end loop
		// execution at this point.
		if e == value {
			return true
		}
	}

	return false
}

// AddEffect is the effect part of an update add operation
// defined by the specification. It is executed by all
// replicas of the data set including the source node. It
// inserts given element and tag into the set representation.
func (s *ORSet) AddEffect(e string, tag string, needsWriteBack bool) error {

	// Insert data element e at key tag.
	s.Elements[tag] = e

	if !needsWriteBack {
		return nil
	}

	// Instructed to write changes back to file.
	err := s.WriteORSetToFile()
	if err != nil {

		// Error during write-back to stable storage.

		// Prepare remove set consistent of just added element.
		rSet := make(map[string]string)
		rSet[tag] = e

		// Revert just made changes.
		s.RemoveEffect(rSet, false)

		return fmt.Errorf("error during writing CRDT file back: %v", err)
	}

	return nil
}

// Add is a helper function only to be executed at the
// source node of an update. It executes the prepare and
// effect update parts of an add operation. Afterwards,
// the update instruction is send downstream to all other
// replicas via the send function which takes care of the
// reliable causally-ordered broadcast.
func (s *ORSet) Add(e string, send sendFunc) error {

	// Create a new unique tag.
	tag := uuid.NewV4().String()

	// Apply effect part of update add.
	// Write changes back to stable storage.
	err := s.AddEffect(e, tag, true)
	if err != nil {
		return err
	}

	// Send to other involved nodes.
	send(e, tag)

	return nil
}

// RemoveEffect is the effect part of an update remove
// operation defined by the specification. It is executed
// by all replicas of the data set including the source node.
// It removes supplied set of tags from the ORSet's set.
func (s *ORSet) RemoveEffect(rSet map[string]string, needsWriteBack bool) error {

	// Range over set of received tags to-be-deleted.
	for rTag := range rSet {

		// Each time we see such tag in this replica's
		// set, we delete it.
		_, found := s.Elements[rTag]
		if found {
			delete(s.Elements, rTag)
		}
	}

	if !needsWriteBack {
		return nil
	}

	// Instructed to write changes back to file.
	err := s.WriteORSetToFile()
	if err != nil {

		// Error during write-back to stable
		// storage, revert just made changes.
		for tag, value := range rSet {
			s.AddEffect(value, tag, false)
		}

		return fmt.Errorf("error during writing CRDT file back: %v", err)
	}

	return nil
}

// Remove is a helper function only to be executed
// by the source node of an update remove operation.
// It first handles the prepare part by checking the
// deletion precondition and creating a remove set
// and afterwards executes the effect part locally and
// sends out the remove message to all other replicas.
func (s *ORSet) Remove(e string, send sendFunc) error {

	// Check precondition: is element present in set?
	if s.Lookup(e) != true {
		return fmt.Errorf("element to be removed not found in set")
	}

	// Initialize list of arguments to send out.
	args := make([]string, 0, 8)

	// Initialize set of elements to remove.
	rmElements := make(map[string]string)

	// Otherwise range over set elements.
	for tag, value := range s.Elements {

		if e == value {

			// If we see the element to-be-deleted, we add
			// the associated tag into our prepared remove set.
			rmElements[tag] = e

			// And we also append it to the list of arguments
			// that will be sent out to other replicas.
			args = append(args, e)
			args = append(args, tag)
		}
	}

	// Execute the effect part of the update remove.
	// Also, write changes back to stable storage.
	err := s.RemoveEffect(rmElements, true)
	if err != nil {
		return err
	}

	// Send arguments to other replicas.
	send(args...)

	return nil
}
