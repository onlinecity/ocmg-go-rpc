package rpc

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	zmq "github.com/pebbe/zmq4"
	"go.uber.org/zap"

	pb "github.com/onlinecity/ocmg-api/gen/go/oc/pb/rpc"
)

const DefaultTimeout = 30 * time.Second

// Call performs a single RPC call, with optional arguments
// Set the context to enforce timeouts, if a timeout is not set it might block the goroutine for 30s
// It will inspect the reply header, and return it as the first argument
// If it receives an upstream error, it will be returned as -1, upstream error
// If a communication error occurs, it will be returned as -2, error
func (con *Connection) Call(method string, ctx context.Context, args ...interface{}) (int32, error) {
	if ctx.Err() != nil {
		return -2, ctx.Err()
	}
	// Wait for reply
	var poll time.Duration
	if dead, ok := ctx.Deadline(); ok {
		poll = time.Until(dead)
	} else {
		poll = DefaultTimeout
	}

	return con.CallDuration(method, poll, args...)
}

// CallDuration performs a single RPC call, with a timeout and optional arguments
// See Call() and CallRepeat() for go context based versions
// Returns -2, context.DeadlineExceeded if the poll times out
func (con *Connection) CallDuration(method string, poll time.Duration, args ...interface{}) (int32, error) {
	bodylen := len(args)

	var flag zmq.Flag
	if bodylen > 0 {
		flag = zmq.SNDMORE
	} else {
		flag = 0
	}

	// Send method name
	if sent, err := con.Send(method, flag); err != nil {
		return -2, err
	} else if sent != len(method) {
		return -2, fmt.Errorf("could not send full method name, sent %d bytes", sent)
	}

	// Send arguments
	buf := bytes.NewBuffer(make([]byte, 0, 8))
	for i := 0; i < bodylen; i++ {
		if err := con.SendValue(args[i], i+1 < bodylen, buf); err != nil {
			return -2, err
		}
	}

	if data, err := con.Poll(poll); err != nil || !data {
		if err != nil {
			return -2, err
		} else {
			return -2, context.DeadlineExceeded
		}
	}

	// Parse the reply
	var rescode int32
	if err := con.RecvValue(&rescode); err != nil {
		return -2, err
	}

	// Check for exceptions, convert any to rpc.Error
	if rescode == -1 {
		ex := &pb.Exception{}
		if err := con.RecvValue(ex); err != nil {
			zap.S().Warnf("received error while decoding exception: %v", err)
			return -2, err
		}
		var u *uuid.UUID
		if len(ex.IncidentUuid) > 0 {
			if uu, err := uuid.FromBytes(ex.IncidentUuid); err != nil {
				zap.S().Warn(err)
			} else {
				u = &uu
			}
		}
		return -1, &Error{
			Message:      ex.GetMessage(),
			Code:         ex.GetCode(),
			Variables:    ex.GetVariables(),
			IncidentUuid: u,
		}
	}

	return rescode, nil
}

// CallRepeat will call the RPC method until a positive reply is received, honoring the context
// Set the context to enforce overall timeout and/or cancellation
// Specify the repeat rate, keep it large enough for the server to realistically respond, but low enough so retrying works
func (con *Connection) CallRepeat(method string, rate time.Duration, ctx context.Context, args ...interface{}) (int32, error) {
	for {
		if ctx.Err() != nil {
			return -2, ctx.Err()
		}
		var poll time.Duration
		if dead, ok := ctx.Deadline(); ok {
			poll = time.Until(dead)
			if poll > rate {
				poll = rate
			}
		} else {
			poll = rate
		}
		res, err := con.CallDuration(method, poll, args...)
		if err == context.DeadlineExceeded {
			// Reconnect and try again
			if err := con.Reconnect(); err != nil {
				return -2, err
			}
		} else if err != nil && res != -1 {
			// Something bad happened
			zap.S().Warn(err)

			// Back-off at retry rate, to avoid aggressive looping
			select {
			case <-ctx.Done():
				return -2, ctx.Err()
			case <-time.After(rate):
				if err := con.Reconnect(); err != nil {
					return -2, err
				}
			}
		} else {
			return res, err
		}
	}
}
