package crdt

import (
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

// AddSource is the prepare part of an update add
// operation defined by the specification. It is executed
// only at the source node that initiated the addition.
// It returns a random UUID (v4) and it is assumed that
// each of these IDs is unique.
func (s *ORSet) AddSource() string {

	return uuid.NewV4().String()
}

// AddDownstream is the effect part of an update add
// operation defined by the specification. It is executed
// by all replicas of the data set including the source node.
// It inserts given element and tag into the set representation.
func (s *ORSet) AddDownstream(e interface{}, tag string) {

	// TODO: Send to all other replicas.

	// Write-lock the set.
	s.lock.Lock()

	// Insert data element e at key tag.
	s.elements[tag] = e

	// Relieve write lock.
	s.lock.Unlock()
}
