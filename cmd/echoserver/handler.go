package main

import (
	ocmg "github.com/onlinecity/ocmg-api/gen/go/oc/pb"
	"github.com/onlinecity/ocmg-go-rpc/pkg/rpc"
)

// declare the interface for this service, used for reflection
type RPCService interface {
	TestVoid() error
	TestSingleEcho([]byte) ([]byte, error)
	TestEchoDuplicate([]byte, int32) ([][]byte, error)
	TestProtobuf() ([]*ocmg.MobileRecipient, error)
	TestException() error
	Healthz() error
	Idle() error
}

// implementation of this simple server
type RPCHandler struct{}

func (h *RPCHandler) TestVoid() error {
	return nil
}

func (h *RPCHandler) TestSingleEcho(arg []byte) ([]byte, error) {
	return arg, nil
}

func (h *RPCHandler) TestEchoDuplicate(arg []byte, repeat int32) ([][]byte, error) {
	res := make([][]byte, 0, repeat)
	var i int32
	for ; i < repeat; i++ {
		res = append(res, arg)
	}
	return res, nil
}

func (h *RPCHandler) TestException() error {
	err := rpc.NewErrorVariables(
		"Temporary blacklist for MSISDN %1 with hash %2",
		0x0214,
		[]string{"4526159917", "foo"},
	)
	err.Zap()
	return err
}

func (h *RPCHandler) Healthz() error {
	return nil
}

func (h *RPCHandler) Idle() error {
	return nil
}

func (h *RPCHandler) TestProtobuf() ([]*ocmg.MobileRecipient, error) {
	return []*ocmg.MobileRecipient{
		{Msisdn: uint64(4526159917)},
		{Msisdn: uint64(4526149424)},
	}, nil
}
