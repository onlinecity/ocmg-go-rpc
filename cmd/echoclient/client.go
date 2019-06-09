package main

import (
	"context"
	"flag"
	"time"

	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	"go.uber.org/zap"

	ocmg "github.com/onlinecity/ocmg-api/gen/go/oc/pb"
	pb "github.com/onlinecity/ocmg-api/gen/go/oc/pb/rpc"
	"github.com/onlinecity/ocmg-go-rpc/pkg/rpc"
)

func main() {
	logger, _ := zap.NewDevelopment()
	zap.ReplaceGlobals(logger)
	defer logger.Sync() // nolint:errcheck

	var endpoint = flag.String("endpoint", "tcp://localhost:5507", "where to connect")
	var timeout = flag.Uint("timeout", 2000, "timeout in ms")
	flag.Parse()

	zap.S().Infof("connecting to %q\n", *endpoint)
	client, err := rpc.NewConnection(zmq.REQ)
	if err != nil {
		zap.S().Fatal(err)
	}
	defer client.Close()
	if err := client.Connect(*endpoint); err != nil {
		zap.S().Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(*timeout)*time.Millisecond)
	defer cancel()

	zap.S().Info("test keep alive")
	if _, err := client.Send("", 0); err != nil {
		zap.S().Fatal(err)
	}
	if body, err := client.Recv(0); err != nil || body != "" {
		zap.S().Fatalw("keep alive failed", "err", err, "body", body)
	}

	zap.S().Info("test discover()")
	if body, err := client.Call("discover", ctx); err != nil || body != 1 {
		zap.S().Fatalw("discover failed", "err", err, "body", body)
	} else {
		s := &pb.Service{}
		if err := client.RecvValue(s); err != nil {
			zap.S().Fatal(err)
		}
		zap.S().Info(s)
	}

	zap.S().Info("TestVoid()")
	if body, err := client.Call("TestVoid", ctx); err != nil || body != 0 {
		zap.S().Fatalw("TestVoid failed", "err", err, "body", body)
	}

	zap.S().Info("TestSingleEcho()")
	if body, err := client.Call("TestSingleEcho", ctx, "foo"); err != nil || body != 1 {
		zap.S().Fatalw("TestSingleEcho failed", "err", err, "body", body)
	}
	var foo string
	if err := client.RecvValue(&foo); err != nil || foo != "foo" {
		zap.S().Fatalw("TestSingleEcho error", "err", err, "foo", foo)
	}

	zap.S().Info("TestEchoDuplicate()")
	if body, err := client.Call("TestEchoDuplicate", ctx, "foo", int32(42)); err != nil || body != 42 {
		zap.S().Fatalw("TestEchoDuplicate failed", "err", err, "body", body)
	}
	for i := 0; i < 42; i++ {
		if err := client.RecvValue(&foo); err != nil || foo != "foo" {
			zap.S().Fatalw("TestEchoDuplicate error", "err", err, "foo", foo)
		}
	}

	zap.S().Info("TestProtobuf()")
	if body, err := client.Call("TestProtobuf", ctx); err != nil || body != 2 {
		zap.S().Fatalw("TestProtobuf failed", "err", err, "body", body)
	}

	pbs, _ := client.RecvMessageBytes(0)
	pb := &ocmg.MobileRecipient{}
	for _, p := range pbs {
		proto.Unmarshal(p, pb)
		zap.S().Info(pb)
	}

	zap.S().Info("TestException()")
	if body, err := client.Call("TestException", ctx); body != -1 {
		zap.S().Fatalw("TestException failed", "err", err, "body", body)
	} else {
		ex := err.(*rpc.Error)
		if ex.Message != "Temporary blacklist for MSISDN %1 with hash %2" {
			zap.S().Fatalw("unexpected message", "ex", ex)
		}
		if ex.Code != 0x0214 {
			zap.S().Fatalw("unexpected code", "ex", ex)
		}
		if len(ex.Variables) != 2 {
			zap.S().Fatalw("unexpected variables", "ex", ex, "vars", ex.Variables)
		}
		zap.S().Infow("exception received", "ex", ex, "uuid", ex.IncidentUuid)
	}

}
