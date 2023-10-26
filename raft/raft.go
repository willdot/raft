package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type VoteResponse struct {
	Term       int
	VoteResult bool
}

type state string

const (
	followerState  state = "follower"
	candidateState state = "candidate"
	leaderState    state = "leader"
)

type Service interface {
	// Send a term, recieve a term, if the vote was given or an error
	RequestVotes(term int, peer string) (*VoteResponse, error)
	// Send the current term and ID, reveive a term or an error
	AppendEntries(term, id int, peer Peer) (int, error)
	// Send heartbeats to all peers
	SendHeartbeats(term int, peers []string)
}

type Peer struct {
	Addr string
}

type Raft struct {
	service Service

	peers   map[string]struct{}
	peersMu sync.Mutex

	leaderLastHeartbeat time.Time

	state        state
	stateMu      sync.Mutex
	term         int
	waitForVotes time.Duration
}

func NewRaft(s Service, waitForVotes time.Duration, peers []string) (*Raft, error) {
	p := make(map[string]struct{})

	for _, peer := range peers {
		p[peer] = struct{}{}
	}

	r := Raft{
		service:             s,
		state:               followerState,
		term:                0,
		waitForVotes:        waitForVotes,
		leaderLastHeartbeat: time.Now(),
		peers:               p,
	}

	go r.electionTimer(context.Background())

	return &r, nil
}

func getElectionTimeout() time.Duration {
	min := 150
	max := 300
	return time.Millisecond * time.Duration(rand.Intn(max-min+1)+min)
}

func (r *Raft) Vote(propsedTerm int) VoteResponse {
	res := VoteResponse{
		Term: r.term,
	}

	if propsedTerm > r.term {
		res.VoteResult = true
	}

	return res
}

func (r *Raft) AddPeer(peer Peer) {
	r.peersMu.Lock()
	defer r.peersMu.Unlock()

	r.peers[peer.Addr] = struct{}{}
}

func (r *Raft) getPeers() map[string]struct{} {
	r.peersMu.Lock()
	defer r.peersMu.Unlock()

	return r.peers
}

func (r *Raft) HeartbeatReceived(term int) {
	r.leaderLastHeartbeat = time.Now()

	// if the term of the heartbeat is higher, update our term to be the same and if
	// we are a leader, step down as a new leader has been elected
	if term > r.term {
		r.term = term
		if r.getState() == leaderState {
			r.updateState(followerState)
		}
	}
}

func (r *Raft) electionTimer(ctx context.Context) {
	electionTimeout := getElectionTimeout()

	timerDuration := time.Millisecond * 100

	timer := time.NewTimer(timerDuration)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			timer.Reset(timerDuration)
			if r.shouldStartElection(electionTimeout) {
				fmt.Println("starting election")
				r.startElection(ctx)
			}

		case <-ctx.Done():
			fmt.Println("context cancelled, stopping election timer")
			return
		}
	}
}

func (r *Raft) shouldStartElection(electionTimeout time.Duration) bool {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()

	currentState := r.state

	// if not in follower state, no need to check
	if currentState == leaderState || currentState == candidateState {
		return false
	}

	// check if heard from leader
	if time.Since(r.leaderLastHeartbeat) < electionTimeout {
		return false
	}

	return true
}

func calculateRequiredVotes(numberOfPeers int) int {
	// need a quarom of votes which is a majority.
	// for example:
	// 3 nodes requires 2 votes
	// 4 nodes requires 3 votes
	// 5 nodes requires 3 votes
	// 6 nodes requires 4 votes

	return ((numberOfPeers + 1) / 2) + 1
}

func (r *Raft) getVoteFromPeer(peer string, votes chan<- *VoteResponse) {
	result, err := r.service.RequestVotes(r.term, peer)
	if err != nil {
		fmt.Printf("failed to request votes: %s\n", err)
	}

	votes <- result
}

func (r *Raft) startElection(ctx context.Context) {
	r.updateState(candidateState)
	r.term = r.term + 1

	votesChan := make(chan *VoteResponse)

	peers := r.getPeers()

	for peer := range peers {
		peer := peer
		go r.getVoteFromPeer(peer, votesChan)
	}

	ctx, cancel := context.WithTimeout(ctx, r.waitForVotes)
	defer cancel()

	// get as many votes as we can before the timeout
	votes := make([]*VoteResponse, 0, len(r.peers))
	for i := 0; i < len(r.peers); i++ {
		select {
		case vote := <-votesChan:
			if vote != nil {
				votes = append(votes, vote)
			}
		case <-ctx.Done():
			// timed out, see if we have enough votes or wait for the next election
			fmt.Println("timed out waiting for votes")
			break
		}
	}

	fmt.Printf("got '%d' votes from peers\n", len(votes))

	// now work out the results based on the votes we have
	requiredVotes := calculateRequiredVotes(len(r.peers))
	// we vote for ourselves so always start with 1 vote
	yesVotes := 1
	for _, vote := range votes {
		if vote.Term > r.term {
			// another peer has a higher term so we can't be leader
			r.updateState(followerState)
			return
		}
		if vote.VoteResult {
			yesVotes++
		}
	}

	// if we have enough votes then update to leader
	fmt.Printf("required votes '%d' and have '%d' votes\n", requiredVotes, yesVotes)
	if yesVotes >= requiredVotes {
		r.updateState(leaderState)

		go r.SendHeartbeats()
		return
	}

	// we didn't get enough votes so back to being a follower
	r.updateState(followerState)
}

func (r *Raft) SendHeartbeats() {
	for {
		if r.getState() != leaderState {
			break
		}

		peers := r.getPeers()

		peersToSendTo := make([]string, 0, len(peers))
		for peer := range peers {
			peersToSendTo = append(peersToSendTo, peer)
		}

		// TODO: some sort of cancellation is required
		r.service.SendHeartbeats(r.term, peersToSendTo)

		time.Sleep(time.Millisecond * 10)
	}
}

func (r *Raft) updateState(state state) {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()

	fmt.Printf("updating state from '%s' to '%s'\n", r.state, state)

	r.state = state
}

func (r *Raft) getState() state {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	return r.state
}
