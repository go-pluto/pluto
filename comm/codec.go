package comm

import (
	stdlog "log"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

type NoOpCodec struct{}

// Marshal fulfills the Marshal() ProtoBuf interface
// for of a custom codec (NoOp).
func (noOpC NoOpCodec) Marshal(v interface{}) ([]byte, error) {

	pb := proto.Buffer{}
	protoMsg := v.(proto.Message)

	if err := pb.Marshal(protoMsg); err != nil {
		return nil, errors.Wrap(err, "ProtoBuf marshalling failed")
	}

	stdlog.Printf("Marshal returns '%#v'", pb.Bytes())

	return pb.Bytes(), nil
}

// Unmarshal fulfills the Unmarshal() ProtoBuf
// interface for of a custom codec (NoOp).
func (noOpC NoOpCodec) Unmarshal(data []byte, v interface{}) error {

	stdlog.Printf("Unmarshal returns '%#v'", data)
	v = data

	return nil
}

// String fulfills the String() ProtoBuf
// interface for of a custom codec (NoOp).
func (noOpC NoOpCodec) String() string {
	return "noop"
}
