package rpc

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	zmq "github.com/pebbe/zmq4"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	pb "github.com/onlinecity/ocmg-api/gen/go/oc/pb/rpc"
)

type Error struct {
	error
	Message      string     `json:"message"`
	Code         uint32     `json:"-"`
	Variables    []string   `json:"variables"`
	IncidentUUID *uuid.UUID `json:"incident_uuid"`
}

// NewError creates a RPC Error incl. UUID
func NewError(message string, code uint32) *Error {
	return NewErrorVariables(message, code, []string{})
}

// NewErrorVariables creates a RPC Error incl. UUID
func NewErrorVariables(message string, code uint32, vars []string) *Error {
	u := uuid.New()
	return &Error{
		Message:      message,
		Code:         code,
		Variables:    vars,
		IncidentUUID: &u,
	}
}

// Error is mandated by the error interface
func (e *Error) Error() string {
	return fmt.Sprintf("(%#04x) %s", e.Code, e.Message)
}

// Zap structured logging
func (e *Error) Zap() {
	zap.L().Warn(e.Message,
		zap.Uint32("code", e.Code),
		zap.Strings("variables", e.Variables),
		zap.Stringer("uuid", e.IncidentUUID),
	)
}

func (e *Error) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddUint32("code", e.Code)
	enc.AddString("message", e.Message)
	if e.IncidentUUID != nil {
		enc.AddString("uuid", e.IncidentUUID.String())
	}
	if len(e.Variables) > 0 {
		if err := enc.AddReflected("variables", e.Variables); err != nil {
			return err
		}
	}
	return nil
}

func (e *Error) MarshalJSON() ([]byte, error) {
	type Alias Error
	return json.Marshal(&struct {
		Code string `json:"code"`
		*Alias
	}{
		Code:  fmt.Sprintf("0x%04X", (*Alias)(e).Code),
		Alias: (*Alias)(e),
	})
}

// RecvValue reads a single value from the socket and decodes it
// Ie: var foo uint32; con.RecvValue(&foo)
func (con *Connection) RecvValue(a interface{}) error {
	switch v := a.(type) {
	case *bool, *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64:
		if err := binary.Read(con, binary.LittleEndian, v); err != nil {
			return err
		}
	case *float32:
		var tmp uint32
		if err := con.RecvValue(&tmp); err != nil {
			return err
		}
		*v = math.Float32frombits(tmp)
		return nil
	case *float64:
		var tmp uint64
		if err := con.RecvValue(&tmp); err != nil {
			return err
		}
		*v = math.Float64frombits(tmp)
		return nil
	case *string:
		s, err := con.Recv(0)
		if err != nil {
			return err
		}
		*v = s
		return nil
	case *[]byte:
		b, err := con.RecvBytes(0)
		if err != nil {
			return err
		}
		*v = b
		return nil
	case proto.Message:
		b, err := con.RecvBytes(0)
		if err != nil {
			return err
		}
		if err := proto.Unmarshal(b, v); err != nil {
			return err
		}
		return nil
	default:
		zap.L().Panic("unsupported type", zap.Reflect("arg", a), zap.Stack("stack"))
	}
	return nil
}

// RecvValues reads values from the socket and decodes them
// Ie: var foo uint32; var bar bool; con.RecvValues(&foo, &bar)
func (con *Connection) RecvValues(a ...interface{}) error {
	for i := 0; i < len(a); i++ {
		if err := con.RecvValue(a[i]); err != nil {
			return err
		}
		more, err := con.HasMore()
		if err != nil {
			return err
		}
		if i+1 < len(a) && !more {
			return errors.New("socket does not have enough data")
		} else if more && i+1 == len(a) {
			return errors.New("socket had more data than expected")
		}
	}
	return nil
}

// SendValue will encode a single value and send it on the socket
// It requires a buffer with up to 8 bytes capacity for it's operation
func (con *Connection) SendValue(a interface{}, more bool, buf *bytes.Buffer) error {
	flag := zmq.SNDMORE
	if !more {
		flag = 0
	}
	switch v := a.(type) {
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		buf.Reset()
		if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
			return err
		}
		if _, err := con.SendBytes(buf.Bytes(), flag); err != nil {
			return err
		}
	case float32:
		buf.Reset()
		if err := binary.Write(buf, binary.LittleEndian, math.Float32bits(v)); err != nil {
			return err
		}
		if _, err := con.SendBytes(buf.Bytes(), flag); err != nil {
			return err
		}
	case float64:
		buf.Reset()
		if err := binary.Write(buf, binary.LittleEndian, math.Float64bits(v)); err != nil {
			return err
		}
		if _, err := con.SendBytes(buf.Bytes(), flag); err != nil {
			return err
		}
	case string:
		if _, err := con.Send(v, flag); err != nil {
			return err
		}
	case []byte:
		if _, err := con.SendBytes(v, flag); err != nil {
			return err
		}
	case proto.Message:
		data, err := proto.Marshal(v)
		if err != nil {
			return err
		}
		if _, err := con.SendBytes(data, flag); err != nil {
			return err
		}
	case [][]byte:
		partLen := len(v)
		for i, part := range v {
			partMore := true
			if i+1 == partLen {
				partMore = more
			}
			if err := con.SendValue(part, partMore, buf); err != nil {
				return err
			}
		}
	case []proto.Message:
		partLen := len(v)
		for i, part := range v {
			partMore := true
			if i+1 == partLen {
				partMore = more
			}
			if err := con.SendValue(part, partMore, buf); err != nil {
				return err
			}
		}
	default:
		// Slices with concrete protobufs need to be reflected
		t := reflect.TypeOf(a)
		if t.Kind() == reflect.Slice {
			v := reflect.ValueOf(a)
			partLen := v.Len()
			for i := 0; i < partLen; i++ {
				part := v.Index(i).Interface()
				partMore := true
				if i+1 == partLen {
					partMore = more
				}
				if err := con.SendValue(part, partMore, buf); err != nil {
					return err
				}
			}
			return nil
		}
		zap.L().Panic("unsupported type",
			zap.Reflect("arg", a),
			zap.Stringer("type", t),
			zap.Stack("stack"),
		)
	}
	return nil
}

// SendReply will send a number of arguments back to a client
// The number of arguments will be prependend on the wire
func (con *Connection) SendReply(a ...interface{}) error {
	partslen := len(a)
	buf := bytes.NewBuffer(make([]byte, 0, 8))

	// Count all parts
	var bodylen int
	for _, v := range a {
		switch p := v.(type) {
		case [][]byte:
			bodylen += len(p)
		case []proto.Message:
			bodylen += len(p)
		default:
			bodylen++
		}
	}

	// Send Header
	if err := con.SendValue(int32(bodylen), true, buf); err != nil {
		return err
	}
	for i, v := range a {
		if err := con.SendValue(v, i+1 < partslen, buf); err != nil {
			return err
		}
	}
	return nil
}

// SendVoid simply sends a zero'd uint32 on the wire
func (con *Connection) SendVoid() error {
	_, err := con.SendBytes(make([]byte, 4), 0)
	return err
}

// SendError will convert a go error to an RPC exception and send it
func (con *Connection) SendError(e error) error {
	if err, ok := e.(*Error); ok {
		return con.SendExceptionVariables(
			err.Message,
			err.Code,
			err.Variables,
			err.IncidentUUID,
		)
	}
	ex := NewError(e.Error(), 1)
	zap.L().Warn("raised exception from error",
		zap.Error(e),
		zap.Object("ex", ex),
		zap.Stringer("uuid", ex.IncidentUUID),
	)
	return con.SendError(ex)
}

// SendException send it without vars or uuid
func (con *Connection) SendException(s string, code uint32) error {
	return con.SendExceptionVariables(s, code, []string{}, nil)
}

// SendExceptionVariables send it with optional vars and/or uuid
func (con *Connection) SendExceptionVariables(s string, code uint32, variables []string, u encoding.BinaryMarshaler) error {
	// Create protobuf with exception
	e := &pb.Exception{
		Message: s,
		Code:    code,
	}
	if len(variables) > 0 {
		e.Variables = variables
	}
	if u != nil {
		if b, err := u.MarshalBinary(); err == nil {
			e.IncidentUuid = b
		}
	}
	var data []byte
	var err error
	data, err = proto.Marshal(e)
	if err != nil {
		zap.L().Panic("marshal error", zap.Error(err))
	}

	buf := bytes.NewBuffer(make([]byte, 0, 8))
	err = con.SendValue(int32(-1), true, buf)
	if err != nil {
		return err
	}
	_, err = con.SendBytes(data, 0)
	return err
}
