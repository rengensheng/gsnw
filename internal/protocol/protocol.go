package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

// Message types
const (
	MsgNewSession    byte = 0x01
	MsgAttachSession byte = 0x02
	MsgDetachSession byte = 0x03
	MsgListSessions  byte = 0x04
	MsgKillSession   byte = 0x05

	MsgInput  byte = 0x10
	MsgOutput byte = 0x11
	MsgResize byte = 0x12

	MsgNewWindow    byte = 0x20
	MsgSelectWindow byte = 0x21
	MsgKillWindow   byte = 0x22

	MsgError   byte = 0xFE
	MsgSuccess byte = 0xFF
)

// Message represents a protocol message
type Message struct {
	Type    byte
	Payload []byte
}

// ResizePayload contains terminal dimensions
type ResizePayload struct {
	Rows uint16
	Cols uint16
}

// SessionInfo contains session information for listing
type SessionInfo struct {
	Name        string
	WindowCount int
	ActiveWin   int
	Attached    bool
}

// Encode serializes a message to bytes
func (m *Message) Encode() []byte {
	length := uint32(len(m.Payload))
	buf := make([]byte, 5+length)
	buf[0] = m.Type
	binary.BigEndian.PutUint32(buf[1:5], length)
	copy(buf[5:], m.Payload)
	return buf
}

// Decode deserializes a message from a reader
func Decode(r io.Reader) (*Message, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	msgType := header[0]
	length := binary.BigEndian.Uint32(header[1:5])

	if length > 1024*1024 { // 1MB max payload
		return nil, errors.New("payload too large")
	}

	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
	}

	return &Message{Type: msgType, Payload: payload}, nil
}

// EncodeResize creates a resize payload
func EncodeResize(rows, cols uint16) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint16(buf[0:2], rows)
	binary.BigEndian.PutUint16(buf[2:4], cols)
	return buf
}

// DecodeResize parses a resize payload
func DecodeResize(data []byte) (rows, cols uint16, err error) {
	if len(data) < 4 {
		return 0, 0, errors.New("invalid resize payload")
	}
	rows = binary.BigEndian.Uint16(data[0:2])
	cols = binary.BigEndian.Uint16(data[2:4])
	return rows, cols, nil
}

// NewMessage creates a new message
func NewMessage(msgType byte, payload []byte) *Message {
	return &Message{Type: msgType, Payload: payload}
}

// Write sends a message to a writer
func Write(w io.Writer, msg *Message) error {
	_, err := w.Write(msg.Encode())
	return err
}

// Read receives a message from a reader
func Read(r io.Reader) (*Message, error) {
	return Decode(r)
}
