package crdt

import (
	"fmt"
	"sync"

	"github.com/satori/go.uuid"
)

// Structs

// ORSet conforms to the specification of an observed-
// removed set defined by Shapiro, Preguiça, Baquero
// and Zawirski. It consists of unique IDs as keys and
// any data as value.
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
func (s *ORSet) AddEffect(e interface{}, tag string) {

	// Write-lock the set.
	s.lock.Lock()

	// Insert data element e at key tag.
	s.elements[tag] = e

	// Relieve write lock.
	s.lock.Unlock()
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

	// Apply effect part of update add.
	s.AddEffect(e, tag)

	// Compose downstream update message.
	// TODO: Escape possible ';' in e.
	msg := fmt.Sprintf("add;%v;%s", e, tag)

	// Send to other involved nodes.
	send(msg)
}
