package server

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"

	"github.com/willdot/raft/raft"
)

type VoteRequest struct {
	Term int
}

type VoteResponse struct {
	Term       int
	VoteResult bool
}

type AddPeerRequest struct {
	Addr string
}

type AddPeerResponse struct {
}

type HeartbeatRequest struct {
	Term int
}

type HeartbeatResponse struct {
}

type RPCServer struct {
	raft *raft.Raft
	addr string
}

func NewRPCServer(addr string, raft *raft.Raft) (*RPCServer, error) {
	server := &RPCServer{
		raft: raft,
		addr: addr,
	}

	return server, nil
}

func (s *RPCServer) Start() error {
	_ = rpc.Register(s)

	rpc.HandleHTTP()

	list, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	fmt.Printf("serving RPC server on %s\n", s.addr)

	return http.Serve(list, nil)
}

func (s *RPCServer) RequestVote(request VoteRequest, response *VoteResponse) error {
	fmt.Printf("handling request for vote with term %d\n", request.Term)

	resp := s.raft.Vote(request.Term)

	*response = VoteResponse{
		Term:       resp.Term,
		VoteResult: resp.VoteResult,
	}
	return nil
}

func (s *RPCServer) AddPeer(request AddPeerRequest, resp *AddPeerResponse) error {
	fmt.Printf("adding peer with addr '%s'\n", request.Addr)

	s.raft.AddPeer(raft.Peer{Addr: request.Addr})

	*resp = AddPeerResponse{}
	return nil
}

func (s *RPCServer) Heartbeat(request HeartbeatRequest, resp *HeartbeatResponse) error {
	s.raft.HeartbeatReceived(request.Term)

	*resp = HeartbeatResponse{}
	return nil
}
