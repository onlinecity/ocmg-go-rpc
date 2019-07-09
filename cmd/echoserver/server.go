package main

import (
	"reflect"
	"runtime"
	"time"

	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	"go.uber.org/zap"

	"github.com/onlinecity/ocmg-go-rpc/pkg/rpc"
)

// serverWorker behaves as a threaded worker, using inproc
func serverWorker() {
	// Setup ZeroMQ
	conn, err := rpc.NewConnection(zmq.REP)
	if err != nil {
		zap.S().Fatal(err)
	}
	defer conn.Close()
	if err := conn.Connect("inproc://backend"); err != nil {
		zap.S().Fatal("cant connect to router")
	}

	// The handler implements the actual service, this file is just boiler plate
	handler := RPCHandler{}

	// Setup Service
	for {
		hasdata, err := conn.Poll(time.Duration(500) * time.Millisecond)
		if err != nil {
			zap.S().Fatal(err)
		}
		if !hasdata {
			// Perform house keeping, every 500ms
			// This is where you would keep connections alive etc
			runtime.Gosched()
			continue
		}

		// Inspect the method name
		var head string
		if err := conn.RecvValue(&head); err != nil {
			zap.S().Error(err)
		}

		// Use a switch to "route" RPC calls
		switch head {
		case "": // keep alive
			if _, err := conn.Send("", 0); err != nil {
				zap.S().Warn(err)
			}
		case "discover":
			// Send it back to the caller
			service := reflect.TypeOf((*RPCService)(nil)).Elem()
			if err := conn.SendReply(rpc.ReflectService(service, "echo", 0)); err != nil {
				zap.S().Error(err)
			}
		case "TestVoid":
			if err := conn.SendVoid(); err != nil {
				zap.S().Warn(err)
			}
		case "healthz":
			if err := conn.SendVoid(); err != nil {
				zap.S().Warn(err)
			}
		case "TestSingleEcho":
			var arg []byte
			if err := conn.RecvValue(&arg); err != nil {
				zap.S().Fatal(err)
			}
			res, err := handler.TestSingleEcho(arg)
			if err != nil {
				_ = conn.SendError(err)
			} else if err := conn.SendReply(res); err != nil {
				zap.S().Error(err)
			}
		case "TestEchoDuplicate":
			// This one takes two arguments
			var arg []byte
			var repeat int32
			if err := conn.RecvValues(&arg, &repeat); err != nil {
				zap.S().Fatal(err)
			}
			res, err := handler.TestEchoDuplicate(arg, repeat)
			if err != nil {
				_ = conn.SendError(err)
			} else if err := conn.SendReply(res); err != nil {
				zap.S().Error(err)
			}
		case "TestException":
			err := handler.TestException()
			if err := conn.SendError(err); err != nil {
				zap.S().Error(err)
			}
		case "TestProtobuf":
			res, _ := handler.TestProtobuf()
			foo := make([]proto.Message, len(res))
			for i, r := range res {
				foo[i] = r
			}
			if err := conn.SendReply(foo); err != nil {
				zap.S().Error(err)
			}
		default:
			// Read remaining messages
			if more, err := conn.HasMore(); err != nil {
				zap.S().Error(err)
			} else if more {
				if args, err := conn.RecvMessageBytes(0); err == nil {
					zap.S().Errorw("received invalid method with args",
						"method", head, "args", args)
				}
			}
			_ = conn.SendException("Unknown method", 3)
		}
	}
}

func main() {
	logger, _ := zap.NewDevelopment()
	zap.ReplaceGlobals(logger)
	defer logger.Sync() // nolint:errcheck

	frontend, _ := zmq.NewSocket(zmq.ROUTER)
	defer frontend.Close()
	if err := frontend.Bind("tcp://*:5507"); err != nil {
		zap.S().Fatal(err)
	}

	//  Backend socket talks to workers over inproc
	backend, _ := zmq.NewSocket(zmq.DEALER)
	defer backend.Close()
	if err := backend.Bind("inproc://backend"); err != nil {
		zap.S().Fatal(err)
	}

	//  Launch pool of worker threads, precise number is not critical
	for i := 0; i < 5; i++ {
		go serverWorker()
	}

	//  Connect backend to frontend via a proxy
	err := zmq.Proxy(frontend, backend, nil)
	zap.S().Fatal("Proxy interrupted: %v", err)
}
