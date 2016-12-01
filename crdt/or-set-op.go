package crdt

import (
	"fmt"
	"strings"
)

// Structs

// ORSetOp represents the broadcast op-based update message
// to all replicas of a CRDT. It contains the update operation
// (add or rmv) and a set of tag-value-pairs.
type ORSetOp struct {
	Operation string
	Arguments map[string]string
}

// Functions

// InitORSetOp returns a fresh ORSetOp variable.
func InitORSetOp() *ORSetOp {

	return &ORSetOp{
		Arguments: make(map[string]string),
	}
}

// String takes in a struct of type ORSetOp and turns it into
// its marshalled version, ready to be sent via broadcast.
func (msg *ORSetOp) String() string {

	// Each CRDT-related network message starts
	// with the operation at the beginning.
	marshalled := msg.Operation

	// Range over involved arguments and append each.
	for tag, value := range msg.Arguments {
		// TODO: Escape possible '|' in value.
		marshalled = fmt.Sprintf("%s|%v|%s", marshalled, value, tag)
	}

	return marshalled
}

// Parse takes in a marshalled (string) version of an ORSetOp
// taken from network communication and turns it back into the
// defined struct representation.
func Parse(msgRaw string) (*ORSetOp, error) {

	// Prepare map for operation arguments.
	args := make(map[string]string)

	// Split message at pipe delimiters.
	parts := strings.Split(msgRaw, "|")

	// If there are less than three parts, this update message
	// is invalid: operation|value|tag.
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid CRDT update message found during parsing")
	}

	// Considering the ORSet, we only accept add and remove updates.
	if (parts[0] != "add") && (parts[0] != "rmv") {
		return nil, fmt.Errorf("unsupported update operation specified in CRDT message")
	}

	// Check if we always receive an even number of arguments:
	// value1|tag1|value2|tag2...
	if ((len(parts) - 1) % 2) != 0 {
		return nil, fmt.Errorf("odd number of arguments, needs even one")
	}

	// Construct arguments map.
	for value := 1; value < len(parts); value += 2 {
		tag := value + 1
		args[parts[tag]] = parts[value]
	}

	return &ORSetOp{
		Operation: parts[0],
		Arguments: args,
	}, nil
}
