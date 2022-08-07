package kafka

import "github.com/bytedance/sonic"

var DefaultCodec = new(Json)

type Codec interface {
	Marshal(val interface{}) ([]byte, error)
	Unmarshal(buf []byte, val interface{}) error
}


type Json uint8

func (j Json) Marshal(val interface{}) ([]byte, error) {
	return sonic.Marshal(val)
}

func (j Json) Unmarshal(buf []byte, val interface{}) error {
	return sonic.Unmarshal(buf, val)
}