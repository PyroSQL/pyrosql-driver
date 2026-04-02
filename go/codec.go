package pyrosql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/pierrec/lz4/v4"
)

// Message types (client -> server)
const (
	MsgQuery      byte = 0x01
	MsgPrepare    byte = 0x02
	MsgExecute    byte = 0x03
	MsgClose      byte = 0x04
	MsgPing       byte = 0x05
	MsgAuth       byte = 0x06
	MsgCompressed byte = 0x10
	MsgQuit       byte = 0xFF
)

// Capability flags for LZ4 compression negotiation.
const (
	CapLZ4               byte = 0x01
	compressionThreshold      = 8 * 1024
)

// Response types (server -> client)
const (
	RespResultSet byte = 0x01
	RespOK        byte = 0x02
	RespError     byte = 0x03
	RespPong      byte = 0x04
	RespReady     byte = 0x05
)

// Value type tags in result sets
const (
	TypeNull  byte = 0
	TypeI64   byte = 1
	TypeF64   byte = 2
	TypeText  byte = 3
	TypeBool  byte = 4
	TypeBytes byte = 5
)

const headerSize = 5

// frame builds a PWire frame: 1-byte type + 4-byte LE length + payload.
func frame(msgType byte, payload []byte) []byte {
	buf := make([]byte, headerSize+len(payload))
	buf[0] = msgType
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(payload)))
	copy(buf[5:], payload)
	return buf
}

func encodeQuery(sql string) []byte {
	return frame(MsgQuery, []byte(sql))
}

func encodePrepare(sql string) []byte {
	return frame(MsgPrepare, []byte(sql))
}

func encodeExecute(handle uint32, params []string) []byte {
	payload := make([]byte, 4+2)
	binary.LittleEndian.PutUint32(payload[0:4], handle)
	binary.LittleEndian.PutUint16(payload[4:6], uint16(len(params)))
	for _, p := range params {
		b := []byte(p)
		lenBuf := make([]byte, 2)
		binary.LittleEndian.PutUint16(lenBuf, uint16(len(b)))
		payload = append(payload, lenBuf...)
		payload = append(payload, b...)
	}
	return frame(MsgExecute, payload)
}

func encodeClose(handle uint32) []byte {
	payload := make([]byte, 4)
	binary.LittleEndian.PutUint32(payload, handle)
	return frame(MsgClose, payload)
}

func encodePing() []byte {
	return frame(MsgPing, nil)
}

func encodeAuth(user, password string) []byte {
	payload := make([]byte, 0, 2+len(user)+len(password))
	payload = append(payload, byte(len(user)))
	payload = append(payload, []byte(user)...)
	payload = append(payload, byte(len(password)))
	payload = append(payload, []byte(password)...)
	return frame(MsgAuth, payload)
}

func encodeQuit() []byte {
	return frame(MsgQuit, nil)
}

// encodeAuthWithCaps builds an AUTH frame that includes a capability byte.
func encodeAuthWithCaps(user, password string, caps byte) []byte {
	payload := make([]byte, 0, 2+len(user)+len(password)+1)
	payload = append(payload, byte(len(user)))
	payload = append(payload, []byte(user)...)
	payload = append(payload, byte(len(password)))
	payload = append(payload, []byte(password)...)
	payload = append(payload, caps)
	return frame(MsgAuth, payload)
}

// compressFrame compresses a frame payload if it exceeds the threshold.
// Returns a MSG_COMPRESSED frame or the original frame if compression is not beneficial.
func compressFrame(msgType byte, payload []byte) []byte {
	if len(payload) <= compressionThreshold {
		return frame(msgType, payload)
	}

	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	_, err := w.Write(payload)
	if err != nil {
		return frame(msgType, payload)
	}
	if err := w.Close(); err != nil {
		return frame(msgType, payload)
	}
	compressed := buf.Bytes()

	// Check ratio: only use compression if it saves space.
	ratio := float64(len(compressed)) / float64(len(payload))
	if ratio > 0.9 {
		return frame(msgType, payload)
	}

	// Build compressed frame inner payload:
	// [original_type: u8][uncompressed_length: u32 LE][lz4_data]
	inner := make([]byte, 1+4+len(compressed))
	inner[0] = msgType
	binary.LittleEndian.PutUint32(inner[1:5], uint32(len(payload)))
	copy(inner[5:], compressed)
	return frame(MsgCompressed, inner)
}

// decompressFrame decompresses a MSG_COMPRESSED frame payload.
// Returns (original_type, decompressed_payload, error).
func decompressFrame(payload []byte) (byte, []byte, error) {
	if len(payload) < 5 {
		return 0, nil, errors.New("pwire: compressed payload too short")
	}
	originalType := payload[0]
	uncompressedLen := binary.LittleEndian.Uint32(payload[1:5])
	lz4Data := payload[5:]

	decompressed := make([]byte, 0, uncompressedLen)
	r := lz4.NewReader(bytes.NewReader(lz4Data))
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			decompressed = append(decompressed, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, nil, fmt.Errorf("pwire: lz4 decompress: %w", err)
		}
	}

	return originalType, decompressed, nil
}

// readFrame reads a full PWire frame from a reader.
// Returns (type, payload, error).
// If the frame is MSG_COMPRESSED, it is transparently decompressed.
func readFrame(r io.Reader) (byte, []byte, error) {
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, nil, fmt.Errorf("pwire: read header: %w", err)
	}
	msgType := header[0]
	length := binary.LittleEndian.Uint32(header[1:5])

	if length == 0 {
		return msgType, nil, nil
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, fmt.Errorf("pwire: read payload: %w", err)
	}

	// Transparently decompress MSG_COMPRESSED frames.
	if msgType == MsgCompressed {
		origType, decompressed, err := decompressFrame(payload)
		if err != nil {
			return 0, nil, err
		}
		return origType, decompressed, nil
	}

	return msgType, payload, nil
}

// Column describes a column in a result set.
type Column struct {
	Name    string
	TypeTag byte
}

// ResultSet holds a decoded RESULT_SET response.
type ResultSet struct {
	Columns []Column
	Rows    [][]interface{}
}

// OKResult holds a decoded OK response.
type OKResult struct {
	RowsAffected int64
	Tag          string
}

// PyroError represents a server-side error.
type PyroError struct {
	SQLState string
	Message  string
}

func (e *PyroError) Error() string {
	return fmt.Sprintf("pyrosql [%s]: %s", e.SQLState, e.Message)
}

func decodeResultSet(payload []byte) (*ResultSet, error) {
	if len(payload) < 2 {
		return nil, errors.New("pwire: malformed result set")
	}
	colCount := int(binary.LittleEndian.Uint16(payload[0:2]))
	pos := 2

	columns := make([]Column, colCount)
	for i := 0; i < colCount; i++ {
		if pos >= len(payload) {
			return nil, errors.New("pwire: unexpected end in column definitions")
		}
		nameLen := int(payload[pos])
		pos++
		if pos+nameLen+1 > len(payload) {
			return nil, errors.New("pwire: column name overflow")
		}
		name := string(payload[pos : pos+nameLen])
		pos += nameLen
		typeTag := payload[pos]
		pos++
		columns[i] = Column{Name: name, TypeTag: typeTag}
	}

	if pos+4 > len(payload) {
		return nil, errors.New("pwire: missing row count")
	}
	rowCount := int(binary.LittleEndian.Uint32(payload[pos : pos+4]))
	pos += 4

	nullBitmapLen := (colCount + 7) / 8
	rows := make([][]interface{}, 0, rowCount)

	for r := 0; r < rowCount; r++ {
		if pos+nullBitmapLen > len(payload) {
			return nil, errors.New("pwire: missing null bitmap")
		}
		bitmap := payload[pos : pos+nullBitmapLen]
		pos += nullBitmapLen

		row := make([]interface{}, colCount)
		for c := 0; c < colCount; c++ {
			byteIdx := c / 8
			bitIdx := uint(c % 8)
			isNull := byteIdx < len(bitmap) && (bitmap[byteIdx]>>bitIdx)&1 == 1

			if isNull {
				row[c] = nil
				continue
			}

			switch columns[c].TypeTag {
			case TypeI64:
				if pos+8 > len(payload) {
					return nil, errors.New("pwire: i64 overflow")
				}
				val := int64(binary.LittleEndian.Uint64(payload[pos : pos+8]))
				pos += 8
				row[c] = val

			case TypeF64:
				if pos+8 > len(payload) {
					return nil, errors.New("pwire: f64 overflow")
				}
				bits := binary.LittleEndian.Uint64(payload[pos : pos+8])
				val := math.Float64frombits(bits)
				pos += 8
				row[c] = val

			case TypeBool:
				if pos >= len(payload) {
					return nil, errors.New("pwire: bool overflow")
				}
				row[c] = payload[pos] != 0
				pos++

			case TypeText:
				if pos+2 > len(payload) {
					return nil, errors.New("pwire: text length overflow")
				}
				l := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
				pos += 2
				if pos+l > len(payload) {
					return nil, errors.New("pwire: text data overflow")
				}
				row[c] = string(payload[pos : pos+l])
				pos += l

			case TypeBytes:
				if pos+2 > len(payload) {
					return nil, errors.New("pwire: bytes length overflow")
				}
				l := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
				pos += 2
				if pos+l > len(payload) {
					return nil, errors.New("pwire: bytes data overflow")
				}
				b := make([]byte, l)
				copy(b, payload[pos:pos+l])
				row[c] = b
				pos += l

			default:
				// Treat unknown types as bytes (same as TEXT/BYTES wire encoding)
				if pos+2 > len(payload) {
					return nil, errors.New("pwire: unknown type length overflow")
				}
				l := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
				pos += 2
				if pos+l > len(payload) {
					return nil, errors.New("pwire: unknown type data overflow")
				}
				row[c] = string(payload[pos : pos+l])
				pos += l
			}
		}
		rows = append(rows, row)
	}

	return &ResultSet{Columns: columns, Rows: rows}, nil
}

func decodeOK(payload []byte) (*OKResult, error) {
	if len(payload) < 9 {
		return nil, errors.New("pwire: malformed OK response")
	}
	rowsAffected := int64(binary.LittleEndian.Uint64(payload[0:8]))
	tagLen := int(payload[8])
	if 9+tagLen > len(payload) {
		return nil, errors.New("pwire: OK tag overflow")
	}
	tag := string(payload[9 : 9+tagLen])
	return &OKResult{RowsAffected: rowsAffected, Tag: tag}, nil
}

func decodeError(payload []byte) (*PyroError, error) {
	if len(payload) < 7 {
		return nil, errors.New("pwire: malformed ERROR response")
	}
	sqlstate := string(payload[0:5])
	msgLen := int(binary.LittleEndian.Uint16(payload[5:7]))
	if 7+msgLen > len(payload) {
		return nil, errors.New("pwire: error message overflow")
	}
	msg := string(payload[7 : 7+msgLen])
	return &PyroError{SQLState: sqlstate, Message: msg}, nil
}
