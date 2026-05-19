package store

import (
	"bytes"
	"github.com/hashicorp/go-msgpack/v2/codec"
)

func encodeMsgpack(v interface{}) []byte {
	var buf bytes.Buffer
	enc := codec.NewEncoder(&buf, &codec.MsgpackHandle{})
	_ = enc.Encode(v)
	return buf.Bytes()
}

func decodeMsgpack(data []byte, v interface{}) error {
	dec := codec.NewDecoderBytes(data, &codec.MsgpackHandle{})
	return dec.Decode(v)
}
