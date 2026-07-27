package store

import (
	"bytes"
	"encoding/gob"
)

type CommandOp uint8

const (
	OpPut    CommandOp = 1
	OpDelete CommandOp = 2
	OpCAS    CommandOp = 3
)

type Command struct {
	Op       CommandOp
	Key      string
	Value    []byte
	Expected []byte
}

type ApplyResult struct {
	OK       bool
	Previous []byte
	Err      string
}

func (c Command) Encode() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeCommand(data []byte) (Command, error) {
	var c Command
	return c, gob.NewDecoder(bytes.NewReader(data)).Decode((&c))
}
