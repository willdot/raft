package raft

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type peer struct {
	term    int
	voteYes bool
	err     error
}

type fakeService struct {
	peers map[string]peer
}

func (f *fakeService) RequestVotes(term int, peerID string) (*VoteResponse, error) {
	peer, ok := f.peers[peerID]
	if !ok {
		return nil, fmt.Errorf("peer not found")
	}

	if peer.err != nil {
		return nil, peer.err
	}

	return &VoteResponse{
		term:       peer.term,
		voteResult: peer.voteYes,
	}, nil
}

func (f *fakeService) AppendEntries(term, id int, peerID string) (int, error) {
	peer, ok := f.peers[peerID]
	if !ok {
		return 0, fmt.Errorf("peer not found")
	}

	return peer.term, peer.err
}

func TestElectionTimerStateHearsFromLeaderWithinTime(t *testing.T) {
	service := fakeService{}

	raft, err := NewRaft(&service)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()

	raft.leaderLastHeartbeat = time.Now()

	go raft.electionTimer(ctx)

	<-ctx.Done()

	// check the state is still follower
	assert.Equal(t, followerState, raft.state)
}

func TestElectionTimerStateChangesMajorityVoteYes(t *testing.T) {
	service := fakeService{
		peers: map[string]peer{
			"peer1": peer{term: 0, voteYes: true},
			"peer2": peer{term: 0, voteYes: false},
		},
	}

	raft, err := NewRaft(&service)
	require.NoError(t, err)

	raft.leaderLastHeartbeat = time.Now().Add(-time.Second)
	raft.peers = []string{"peer1", "peer2"}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()

	go raft.electionTimer(ctx)

	<-ctx.Done()

	// check the state is leader
	assert.Equal(t, leaderState, raft.state)
}

func TestElectionTimerStateMostOtherPeersVotesNo(t *testing.T) {
	service := fakeService{
		peers: map[string]peer{
			"peer1": peer{term: 0, voteYes: false},
			"peer2": peer{term: 0, voteYes: false},
			"peer3": peer{term: 0, voteYes: true},
			"peer4": peer{term: 0, voteYes: false},
		},
	}

	raft, err := NewRaft(&service)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()

	raft.leaderLastHeartbeat = time.Now().Add(-time.Second)
	raft.peers = []string{"peer1", "peer2", "peer3", "peer4"}

	go raft.electionTimer(ctx)

	<-ctx.Done()

	// check the state is still follower
	assert.Equal(t, followerState, raft.state)
}

func TestElectionTimerStateAllOtherPeersVotesNo(t *testing.T) {
	service := fakeService{
		peers: map[string]peer{
			"peer1": peer{term: 0, voteYes: false},
			"peer2": peer{term: 0, voteYes: false},
		},
	}

	raft, err := NewRaft(&service)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()

	raft.leaderLastHeartbeat = time.Now().Add(-time.Second)
	raft.peers = []string{"peer1", "peer2"}

	go raft.electionTimer(ctx)

	<-ctx.Done()

	// check the state is still follower
	assert.Equal(t, followerState, raft.state)
}

func TestElectionTimerStateOtherPeerHasHigherTerm(t *testing.T) {
	service := fakeService{
		peers: map[string]peer{
			"peer1": peer{term: 2, voteYes: false},
			"peer2": peer{term: 0, voteYes: true},
		},
	}

	raft, err := NewRaft(&service)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()

	raft.leaderLastHeartbeat = time.Now().Add(-time.Second)
	raft.peers = []string{"peer1", "peer2"}

	go raft.electionTimer(ctx)

	<-ctx.Done()

	// check we are either a follower or a leader.
	// peer 1 says it has a higher term, but if peer 2 responds first
	// with its "yes" vote, then we get a majority and win the vote
	// even though peer1 has a higher term.
	if raft.state != leaderState && raft.state != followerState {
		t.Fatalf("expecting to be either a leader of follower but actually a '%s'", raft.state)
	}
}
