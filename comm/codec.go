package comm

import (
	stdlog "log"

	"github.com/golang/protobuf/proto"
)

type NoOpCodec struct{}

// Marshal fulfills the Marshal() ProtoBuf interface
// for of a custom codec (NoOp).
func (noOpC NoOpCodec) Marshal(v interface{}) ([]byte, error) {

	stdlog.Printf("[CODEC] Incoming to outgoing: '%#v'", v)
	data, err := proto.Marshal(v.(proto.Message))
	stdlog.Printf("[CODEC] Outgoing to outgoing: '%#v'", data)

	return data, err
}

// Unmarshal fulfills the Unmarshal() ProtoBuf
// interface for of a custom codec (NoOp).
func (noOpC NoOpCodec) Unmarshal(data []byte, v interface{}) error {

	stdlog.Printf("[CODEC] Unmarshal returns '%#v'", data)
	v = data

	return nil
}

// String fulfills the String() ProtoBuf
// interface for of a custom codec (NoOp).
func (noOpC NoOpCodec) String() string {
	return "noop"
}
