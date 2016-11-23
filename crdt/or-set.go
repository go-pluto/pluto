package crdt

import (
	"fmt"
	"sync"

	"github.com/satori/go.uuid"
)

// Structs

// ORSet conforms to the specification of an observed-
// removed set defined by Shapiro, Preguiça, Baquero
// and Zawirski. It consists of unique IDs and data items.
type ORSet struct {
	lock     *sync.RWMutex
	elements map[string]interface{}
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
		elements: make(map[string]interface{}),
	}
}

// Lookup cycles through elements in ORSet and
// returns true if element e is present and
// false otherwise.
func (s *ORSet) Lookup(e interface{}) bool {

	// Fallback return value is false.
	found := false

	// Read-lock the set.
	s.lock.RLock()

	for _, value := range s.elements {

		// When we find the value while iterating
		// through set, we set return value to true
		// and end loop execution at this point.
		if e == value {
			found = true
			break
		}
	}

	// Relieve read lock.
	s.lock.RUnlock()

	return found
}

// AddEffect is the effect part of an update add operation
// defined by the specification. It is executed by all
// replicas of the data set including the source node. It
// inserts given element and tag into the set representation.
func (s *ORSet) AddEffect(e interface{}, tag string, needsLocking bool) {

	if needsLocking {
		// Write-lock the set.
		s.lock.Lock()
	}

	// Insert data element e at key tag.
	s.elements[tag] = e

	if needsLocking {
		// Relieve write lock.
		s.lock.Unlock()
	}
}

// Add is a helper function only to be executed at the
// source node of an update. It executes the prepare and
// effect update parts of an add operation. Afterwards,
// the update instruction is send downstream to all other
// replicas via the send function which takes care of the
// reliable causally-ordered broadcast.
func (s *ORSet) Add(e interface{}, send sendFunc) {

	// Create a new unique tag.
	tag := uuid.NewV4().String()

	// Write-lock the set.
	s.lock.Lock()

	// Apply effect part of update add.
	s.AddEffect(e, tag, false)

	// Compose downstream update message.
	// TODO: Escape possible ';' in e.
	msg := fmt.Sprintf("add|%v|%s", e, tag)

	// Send to other involved nodes.
	send(msg)

	// Relieve write lock.
	s.lock.Unlock()
}

// RemoveEffect is the effect part of an update remove
// operation defined by the specification. It is executed
// by all replicas of the data set including the source node.
// It removes supplied set of tags from the ORSet's set.
func (s *ORSet) RemoveEffect(rSet map[string]interface{}, needsLocking bool) {

	if needsLocking {
		// Write-lock the set.
		s.lock.Lock()
	}

	// Range over set of received tags to-be-deleted.
	for rTag := range rSet {

		// Each time we see such tag in this replica's
		// set, we delete it.
		if _, found := s.elements[rTag]; found {
			delete(s.elements, rTag)
		}
	}

	if needsLocking {
		// Relieve write lock.
		s.lock.Unlock()
	}
}

// Remove is a helper function only to be executed
// by the source node of an update remove operation.
// It first handles the prepare part by checking the
// deletion precondition and creating a remove set
// and afterwards executes the effect part locally and
// sends out the remove message to all other replicas.
func (s *ORSet) Remove(e interface{}, send sendFunc) error {

	// Initialize needed remove set and msg variables.
	rSet := make(map[string]interface{})
	msg := "rmv|"

	// Write-lock the set.
	s.lock.Lock()

	// Check precondition: is element present in set?
	if s.Lookup(e) != true {

		// If not, relieve write lock and return with error.
		s.lock.Unlock()

		return fmt.Errorf("element to be removed not found in set")
	}

	// Otherwise range over set elements.
	for tag, value := range s.elements {

		// If we see the element to-be-deleted, we add
		// the associated tag into our prepared remove set.
		if e == value {
			rSet[tag] = e
		}
	}

	// Execute the effect part of the update remove but do
	// not lock the set structure as we already maintain a lock.
	s.RemoveEffect(rSet, false)

	// Construct message to send to other replicas.
	for tag, value := range rSet {
		msg = fmt.Sprintf("%s%v|%s", msg, value, tag)
	}

	// Send message to other replicas.
	send(msg)

	// Relieve write lock.
	s.lock.Unlock()

	return nil
}
