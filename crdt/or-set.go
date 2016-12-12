package crdt

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"encoding/base64"
	"io/ioutil"

	"github.com/satori/go.uuid"
)

// Structs

// ORSet conforms to the specification of an observed-
// removed set defined by Shapiro, Preguiça, Baquero
// and Zawirski. It consists of unique IDs and data items.
type ORSet struct {
	lock     *sync.RWMutex
	file     *os.File
	elements map[string]string
}

// sendFunc is used as a parameter to below defined
// AddSendDownstream function that broadcasts an update
// payload to all other replicas.
type sendFunc func(string)

// Functions

// InitORSet returns an empty initialized new
// observed-removed set.
func InitORSet() *ORSet {

	return &ORSet{
		lock:     new(sync.RWMutex),
		elements: make(map[string]string),
	}
}

// InitORSetWithFile takes in a file name and initializes
// a new ORSet with opened file handler to that name as
// designated log file.
func InitORSetWithFile(fileName string) (*ORSet, error) {

	// Init an empty ORSet.
	s := InitORSet()

	// Attempt to create a new CRDT file.
	f, err := os.Create(fileName)
	if err != nil {
		return nil, fmt.Errorf("opening CRDT file '%s' failed with: %s\n", fileName, err.Error())
	}

	// Change permissions.
	err = f.Chmod(0600)
	if err != nil {
		return nil, fmt.Errorf("changing permissions of CRDT file '%s' failed with: %s\n", fileName, err.Error())
	}

	// Assign it to ORSet.
	s.file = f

	// Write newly created CRDT file to stable storage.
	err = s.WriteORSetToFile(false)
	if err != nil {
		return nil, fmt.Errorf("error during CRDT file write-back: %s\n", err.Error())
	}

	return s, nil
}

// InitORSetFromFile parses an ORSet found in the
// supplied file and returns it, initialized with
// elements saved in file.
func InitORSetFromFile(fileName string) (*ORSet, error) {

	// Init an empty ORSet.
	s := InitORSet()

	// Attempt to open CRDT file and assign to set afterwards.
	f, err := os.OpenFile(fileName, os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("opening CRDT file '%s' failed with: %s\n", fileName, err.Error())
	}
	s.file = f

	// Parse contained CRDT state from file.
	contentsRaw, err := ioutil.ReadAll(s.file)
	if err != nil {
		return nil, fmt.Errorf("reading all contents from CRDT file '%s' failed with: %s\n", fileName, err.Error())
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
		return nil, fmt.Errorf("odd number of elements in CRDT file '%s'\n", fileName)
	}

	// Range over all value-tag-pairs.
	for value := 0; value < len(parts); value += 2 {

		tag := value + 1

		// Decode string from base64.
		decValue, err := base64.StdEncoding.DecodeString(parts[value])
		if err != nil {
			return nil, fmt.Errorf("decoding base64 string in CRDT file '%s' failed: %s\n", fileName, err.Error())
		}

		// Assign decoded value to corresponding
		// tag in elements set.
		s.elements[parts[tag]] = string(decValue)
	}

	return s, nil
}

// WriteORSetToFile saves an active ORSet onto
// stable storage at location from initialization.
// This allows for a CRDT ORSet to be made persistent
// and later be resumed from prior state.
func (s *ORSet) WriteORSetToFile(needsLocking bool) error {

	if needsLocking {
		// Write-lock the set and unlock on any exit.
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	marshalled := ""

	for tag, valueRaw := range s.elements {

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
	_, err := s.file.Seek(0, os.SEEK_SET)
	if err != nil {
		return fmt.Errorf("error while setting head back to beginning in CRDT file '%s': %s\n", s.file.Name(), err.Error())
	}

	// Write marshalled set to file.
	newNumOfBytes, err := s.file.WriteString(marshalled)
	if err != nil {
		return fmt.Errorf("failed to write ORSet contents to file '%s': %s\n", s.file.Name(), err.Error())
	}

	// Adjust file size to just written length of string.
	err = s.file.Truncate(int64(newNumOfBytes))
	if err != nil {
		return fmt.Errorf("error while truncating CRDT file '%s' to new size: %s\n", s.file.Name(), err.Error())
	}

	// Save to stable storage.
	err = s.file.Sync()
	if err != nil {
		return fmt.Errorf("could not synchronise CRDT file '%s' contents to stable storage: %s\n", s.file.Name(), err.Error())
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

	for _, value := range s.elements {

		// Check if we did not yet considered this value.
		if _, seen := seenValues[value]; seen != true {

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
func (s *ORSet) Lookup(e string, needsLocking bool) bool {

	// Fallback return value is false.
	found := false

	if needsLocking {
		// Read-lock set and unlock on exit.
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	for _, value := range s.elements {

		// When we find the value while iterating
		// through set, we set return value to true
		// and end loop execution at this point.
		if e == value {
			found = true
			break
		}
	}

	return found
}

// AddEffect is the effect part of an update add operation
// defined by the specification. It is executed by all
// replicas of the data set including the source node. It
// inserts given element and tag into the set representation.
func (s *ORSet) AddEffect(e string, tag string, needsLocking bool, needsWriteBack bool) error {

	if needsLocking {
		// Write-lock set and unlock on exit.
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	// Insert data element e at key tag.
	s.elements[tag] = e

	if needsWriteBack {

		// Instructed to write changes back to file.
		err := s.WriteORSetToFile(false)
		if err != nil {

			// Error during write-back to stable storage.

			// Prepare remove set consistent of just added element.
			rSet := make(map[string]string)
			rSet[tag] = e

			// Revert just made changes.
			s.RemoveEffect(rSet, false, false)

			return fmt.Errorf("error during writing CRDT file back: %s\n", err.Error())
		}
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

	// Write-lock set and unlock on exit.
	s.lock.Lock()
	defer s.lock.Unlock()

	// Apply effect part of update add. Do not lock
	// structure but write changes back to stable storage.
	err := s.AddEffect(e, tag, false, true)
	if err != nil {
		return err
	}

	// Send to other involved nodes.
	send(fmt.Sprintf("%v;%s", base64.StdEncoding.EncodeToString([]byte(e)), tag))

	return nil
}

// RemoveEffect is the effect part of an update remove
// operation defined by the specification. It is executed
// by all replicas of the data set including the source node.
// It removes supplied set of tags from the ORSet's set.
func (s *ORSet) RemoveEffect(rSet map[string]string, needsLocking bool, needsWriteBack bool) error {

	if needsLocking {
		// Write-lock set and unlock on exit.
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	// Range over set of received tags to-be-deleted.
	for rTag := range rSet {

		// Each time we see such tag in this replica's
		// set, we delete it.
		if _, found := s.elements[rTag]; found {
			delete(s.elements, rTag)
		}
	}

	if needsWriteBack {

		// Instructed to write changes back to file.
		err := s.WriteORSetToFile(false)
		if err != nil {

			// Error during write-back to stable storage.

			// Revert just made changes.
			for tag, value := range rSet {
				s.AddEffect(value, tag, false, false)
			}

			return fmt.Errorf("error during writing CRDT file back: %s\n", err.Error())
		}
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

	// Initialize string to send out.
	var msg string

	// Initialize set of elements to remove.
	rmElements := make(map[string]string)

	// Write-lock set and unlock on exit.
	s.lock.Lock()
	defer s.lock.Unlock()

	// Check precondition: is element present in set?
	if s.Lookup(e, false) != true {
		return fmt.Errorf("element to be removed not found in set")
	}

	// Otherwise range over set elements.
	for tag, value := range s.elements {

		if e == value {

			// If we see the element to-be-deleted, we add
			// the associated tag into our prepared remove set.
			rmElements[tag] = e

			// And we also append it to the message that will
			// be sent out to other replicas.
			if msg == "" {
				msg = fmt.Sprintf("%v;%s", base64.StdEncoding.EncodeToString([]byte(e)), tag)
			} else {
				msg = fmt.Sprintf("%s;%v;%s", msg, base64.StdEncoding.EncodeToString([]byte(e)), tag)
			}
		}
	}

	// Execute the effect part of the update remove but do
	// not lock the set structure as we already maintain a lock.
	// Also, write changes back to stable storage.
	err := s.RemoveEffect(rmElements, false, true)
	if err != nil {
		return err
	}

	// Send message to other replicas.
	send(msg)

	return nil
}
