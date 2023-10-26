package service

import (
	"fmt"
	"net/http"
	"net/rpc"
	"strings"
	"time"

	"github.com/willdot/raft/raft"
)

type Service struct {
	httpClient http.Client
	serverAddr string
}

func NewService(serverAddr string) Service {
	client := http.Client{
		Timeout: time.Second * 10,
	}
	return Service{
		httpClient: client,
		serverAddr: serverAddr,
	}
}

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

type HeartbeatRequest struct {
	Term int
}

// Send a term, recieve a term, if the vote was given or an error
func (s *Service) RequestVotes(term int, peer string) (*raft.VoteResponse, error) {
	client, err := rpc.DialHTTP("tcp", peer)
	if err != nil {
		return nil, err
	}

	var resp VoteResponse
	voteReq := VoteRequest{
		Term: term,
	}

	err = client.Call("RPCServer.RequestVote", voteReq, &resp)
	if err != nil {
		return nil, err
	}

	return &raft.VoteResponse{
		Term:       resp.Term,
		VoteResult: resp.VoteResult,
	}, nil
}

// Send the current term and ID, reveive a term or an error
func (s *Service) AppendEntries(term, id int, peer raft.Peer) (int, error) {
	return 0, nil
}

// Sends heartbeats to the peers
func (s *Service) SendHeartbeats(term int, peers []string) {
	for _, peer := range peers {
		client, err := rpc.DialHTTP("tcp", peer)
		if err != nil {
			if !strings.Contains(err.Error(), "connection refused") {
				fmt.Printf("failed to dial peer: %s\n", err)
			}
			continue
		}

		err = client.Call("RPCServer.Heartbeat", HeartbeatRequest{Term: term}, nil)
		if err != nil {
			fmt.Printf("failed to send heartbear to peer: %s\n", err)
			continue
		}
	}
}

func (s *Service) AddPeer(peers []string) error {
	for _, peer := range peers {
		client, err := rpc.DialHTTP("tcp", peer)
		if err != nil {
			return err
		}

		// TODO: Peer ID - what do we want to use it for if anything?
		err = client.Call("RPCServer.AddPeer", AddPeerRequest{Addr: s.serverAddr}, nil)
		if err != nil {
			return err
		}
		fmt.Println("peer added")
	}

	return nil
}
