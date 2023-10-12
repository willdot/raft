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

	state state
	term  int
}

func NewRaft(s Service) (*Raft, error) {
	r := Raft{
		service: s,
		state:   followerState,
		term:    0,
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
				r.startElection()
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
	// 6 nodes requires 5 votes
	return ((numberOfPeers + 1) / 2) + 1
}

func (r *Raft) startElection() {
	r.updateState(candidateState)
	r.term = r.term + 1

	// we start at 1 because we vote for ourselves
	yesVoteCount := 1
	votesReceived := 0
	requiredVotes := calculateRequiredVotes(len(r.peers))

	for _, peer := range r.peers {
		go func(peer string) {
			defer func() {
				// if we've received all the votes now, check the results and update to either leader or follower
				if votesReceived != len(r.peers) {
					return
				}

				if yesVoteCount >= requiredVotes {
					r.updateState(leaderState)
					return
				}
				r.updateState(followerState)
			}()

			result, err := r.service.RequestVotes(r.term, peer)
			votesReceived++
			if err != nil {
				r.updateState(followerState)

				fmt.Printf("failed to request votes: $%s\n", err)
				return
			}

			// while waiting for the result, if for some reason we are no longer
			// a candidate then abort
			if r.getState() != candidateState {
				return
			}

			// if the peer term is higher, it will be leader
			if result.term > r.term {
				r.updateState(followerState)
				return
			}

			if !result.voteResult {
				return
			}

			yesVoteCount++

			// if we have enough votes we are leader
			if yesVoteCount >= requiredVotes {
				r.updateState(leaderState)
				return
			}
		}(peer)
	}
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
