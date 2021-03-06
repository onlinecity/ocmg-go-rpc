package rpc

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	zmq "github.com/pebbe/zmq4"
	"go.uber.org/zap"
)

// Connection wraps a ZMQ socket with a Poller for ease of use
type Connection struct {
	// Socket is a ZMQ socket
	Socket *zmq.Socket
	// Endpoint is where the socket is connected
	Endpoint string
	// Poller is used for enforcing timeouts only
	Poller *zmq.Poller

	service   string
	domain    string
	zmqtype   zmq.Type
	pollid    int
	pooled    bool
	usedAt    int64 // atomic
	createdAt time.Time
}

// NewConnection creates a connection and adds it to the poller
func NewConnection(t zmq.Type) (*Connection, error) {
	soc, err := zmq.NewSocket(t)
	if err != nil {
		return nil, err
	}
	con := Connection{
		Socket:  soc,
		Poller:  zmq.NewPoller(),
		zmqtype: t,
	}
	con.pollid = con.Poller.Add(soc, zmq.POLLIN)
	return &con, nil
}

func (con *Connection) UsedAt() time.Time {
	unix := atomic.LoadInt64(&con.usedAt)
	return time.Unix(unix, 0)
}

func (con *Connection) SetUsedAt(tm time.Time) {
	atomic.StoreInt64(&con.usedAt, tm.Unix())
}

// Read makes the connection compatible with the Reader interface
func (con *Connection) Read(p []byte) (n int, err error) {
	arg, err := con.Socket.RecvBytes(0)
	if err != nil {
		return 0, err
	}
	if len(p) != len(arg) {
		return 0, fmt.Errorf("slice unexpected size, recv %d, slice fits %d",
			len(arg), len(p))
	}
	return copy(p, arg), nil
}

// SimplifiedSRV adds our K8S hack and returns the first result
func SimplifiedSRV(ctx context.Context, service, domain string) (string, uint16, error) {
	var fqdn string
	if strings.HasSuffix(domain, "cluster.local") {
		fqdn = strings.Join([]string{service, domain}, ".")
	} else {
		fqdn = domain
	}
	_, addrs, err := net.DefaultResolver.LookupSRV(ctx, service, "tcp", fqdn)
	if err != nil {
		return "", 0, err
	}
	if len(addrs) == 0 {
		return "", 0, fmt.Errorf("no SRV records for %s._tcp.%s", service, fqdn)
	}
	first := addrs[0]
	return first.Target, first.Port, nil
}

const DefaultDomain = "gwapi.svc.cluster.local"

// ConnectSrv is compatible with our usual way of handling SRV records
// DNS TTL is not honered
func (con *Connection) ConnectSrv(service string, domain *string) error {
	if domain == nil {
		con.domain = DefaultDomain
	} else {
		con.domain = *domain
	}
	target, port, err := SimplifiedSRV(context.Background(), service, *domain)
	if err != nil {
		return err
	}
	con.service = service
	con.Endpoint = fmt.Sprintf("tcp://%s:%d", target, port)
	return con.Socket.Connect(con.Endpoint)
}

// Connect to an arbitrary endpoint, see also ConnectSrv()
func (con *Connection) Connect(endpoint string) error {
	con.Endpoint = endpoint
	return con.Socket.Connect(endpoint)
}

// Reconnect will close and dump the old socket, resetting to a known state
func (con *Connection) Reconnect() error {
	if err := con.Socket.Close(); err != nil {
		zap.L().Error("ignoring socket close error", zap.Error(err))
	}
	if con.pollid != -1 {
		if err := con.Poller.Remove(con.pollid); err != nil {
			zap.L().Warn("failed to remove socket from poller", zap.Error(err))
			// just reset the poller
			con.Poller = zmq.NewPoller()
		}
		con.pollid = -1
	}
	var socket *zmq.Socket
	var err error
	if socket, err = zmq.NewSocket(con.zmqtype); err != nil {
		return err
	}
	con.Socket = socket
	err = con.Socket.Connect(con.Endpoint)
	if err != nil {
		return err
	}
	con.pollid = con.Poller.Add(con.Socket, zmq.POLLIN)
	return nil
}

// Close socket and release the poller
func (con *Connection) Close() error {
	if con.Socket != nil {
		if err := con.Socket.Close(); err != nil {
			return err
		}
	}
	con.Poller = nil
	return nil
}

// HasMore is a convenience function
func (con *Connection) HasMore() (bool, error) {
	return con.Socket.GetRcvmore()
}

// RecvMessageBytes is a convenience function, wrapping the socket
func (con *Connection) RecvMessageBytes(flags zmq.Flag) (msg [][]byte, err error) {
	return con.Socket.RecvMessageBytes(flags)
}

// Send is a convenience function, wrapping the socket
func (con *Connection) Send(data string, flags zmq.Flag) (int, error) {
	return con.Socket.Send(data, flags)
}

// SendBytes is a convenience function, wrapping the socket
func (con *Connection) SendBytes(data []byte, flags zmq.Flag) (int, error) {
	return con.Socket.SendBytes(data, flags)
}

// Recv is a convenience function, wrapping the socket
func (con *Connection) Recv(flags zmq.Flag) (string, error) {
	return con.Socket.Recv(flags)
}

// RecvBytes is a convenience function, wrapping the socket
func (con *Connection) RecvBytes(flags zmq.Flag) ([]byte, error) {
	return con.Socket.RecvBytes(flags)
}

// Poll will call the poller to poll the one socket we have
func (con *Connection) Poll(timeout time.Duration) (bool, error) {
	polled, err := con.Poller.Poll(timeout)
	if err != nil {
		return false, err
	}
	return (len(polled) > 0), nil
}
