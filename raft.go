package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type VoteResponse struct {
	term       int
	voteResult bool
}

type state string

const (
	followerState  state = "follower"
	candidateState state = "candidate"
	leaderState    state = "leader"
)

type Service interface {
	// Send a term, recieve a term, if the vote was given or an error
	RequestVotes(term int, peerID string) (*VoteResponse, error)
	// Send the current term and ID, reveive a term or an error
	AppendEntries(term, id int, peerID string) (int, error)
}

type Raft struct {
	service Service
	mu      sync.Mutex

	peers []string

	leaderLastHeartbeat time.Time

	state        state
	term         int
	waitForVotes time.Duration
}

func NewRaft(s Service, waitForVotes time.Duration) (*Raft, error) {
	r := Raft{
		service:      s,
		state:        followerState,
		term:         0,
		waitForVotes: waitForVotes,
	}

	return &r, nil
}

func getElectionTimeout() time.Duration {
	min := 150
	max := 300
	return time.Millisecond * time.Duration(rand.Intn(max-min+1)+min)
}

func (r *Raft) electionTimer(ctx context.Context) error {
	electionTimeout := getElectionTimeout()

	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			if r.shouldStartElection(electionTimeout) {
				r.startElection(ctx)
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *Raft) shouldStartElection(electionTimeout time.Duration) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

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
		fmt.Printf("failed to request votes: %e", err)
	}

	votes <- result
}

func (r *Raft) startElection(ctx context.Context) {
	r.updateState(candidateState)
	r.term = r.term + 1

	votesChan := make(chan *VoteResponse)

	for _, peer := range r.peers {
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
			break
		}
	}

	// now work out the results based on the votes we have
	requiredVotes := calculateRequiredVotes(len(r.peers))
	// we vote for ourselves so always start with 1 vote
	yesVotes := 1
	for _, vote := range votes {
		if vote.term > r.term {
			// another peer has a higher term so we can't be leader
			r.updateState(followerState)
			return
		}
		if vote.voteResult {
			yesVotes++
		}
	}

	// if we have enough votes then update to leader
	if yesVotes >= requiredVotes {
		r.updateState(leaderState)
		return
	}

	// we didn't get enough votes so back to being a follower
	r.updateState(followerState)
}

func (r *Raft) updateState(state state) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = state
}

func (r *Raft) getState() state {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}
