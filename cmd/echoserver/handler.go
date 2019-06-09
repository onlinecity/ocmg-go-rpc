package main

import (
	ocmg "github.com/onlinecity/ocmg-api/gen/go/oc/pb"
	"github.com/onlinecity/ocmg-go-rpc/pkg/rpc"
)

// declare the interface for this service, used for reflection
type RpcService interface {
	TestVoid() error
	TestSingleEcho([]byte) ([]byte, error)
	TestEchoDuplicate([]byte, int32) ([][]byte, error)
	TestProtobuf() ([]*ocmg.MobileRecipient, error)
	TestException() error
	Healthz() error
	Idle() error
}

// implementation of this simple server
type RpcHandler struct{}

func (h *RpcHandler) TestVoid() error {
	return nil
}

func (h *RpcHandler) TestSingleEcho(arg []byte) ([]byte, error) {
	return arg, nil
}

func (h *RpcHandler) TestEchoDuplicate(arg []byte, repeat int32) ([][]byte, error) {
	res := make([][]byte, 0, repeat)
	var i int32
	for ; i < repeat; i++ {
		res = append(res, arg)
	}
	return res, nil
}

func (h *RpcHandler) TestException() error {
	err := rpc.NewErrorVariables(
		"Temporary blacklist for MSISDN %1 with hash %2",
		0x0214,
		[]string{"4526159917", "foo"},
	)
	err.Zap()
	return err
}

func (h *RpcHandler) Healthz() error {
	return nil
}

func (h *RpcHandler) Idle() error {
	return nil
}

func (h *RpcHandler) TestProtobuf() ([]*ocmg.MobileRecipient, error) {
	return []*ocmg.MobileRecipient{
		&ocmg.MobileRecipient{Msisdn: uint64(4526159917)},
		&ocmg.MobileRecipient{Msisdn: uint64(4526149424)},
	}, nil
}
