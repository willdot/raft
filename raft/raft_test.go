package raft

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakePeer struct {
	term            int
	voteYes         bool
	requestDuration time.Duration
	err             error
}

type fakeService struct {
	peers map[string]fakePeer
}

func (f *fakeService) RequestVotes(term int, peer string) (*VoteResponse, error) {
	fakeP, ok := f.peers[peer]
	if !ok {
		return nil, fmt.Errorf("peer not found")
	}

	time.Sleep(fakeP.requestDuration)

	if fakeP.err != nil {
		return nil, fakeP.err
	}

	return &VoteResponse{
		Term:       fakeP.term,
		VoteResult: fakeP.voteYes,
	}, nil
}

func (f *fakeService) AppendEntries(term, id int, peer Peer) (int, error) {
	fakeP, ok := f.peers[peer.Addr]
	if !ok {
		return 0, fmt.Errorf("peer not found")
	}

	return fakeP.term, fakeP.err
}

func (f *fakeService) SendHeartbeats(term int, peers []string) {
}

func TestElectionTimerStateHearsFromLeaderWithinTime(t *testing.T) {
	service := fakeService{}

	peers := []string{"peer1", "peer2"}

	raft, err := NewRaft(&service, time.Second, peers)
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
		peers: map[string]fakePeer{
			"peer1": fakePeer{term: 0, voteYes: true},
			"peer2": fakePeer{term: 0, voteYes: false},
		},
	}

	peers := []string{"peer1", "peer2"}

	raft, err := NewRaft(&service, time.Second, peers)
	require.NoError(t, err)

	raft.leaderLastHeartbeat = time.Now().Add(-time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	go raft.electionTimer(ctx)

	<-ctx.Done()

	// check the state is leader
	assert.Equal(t, leaderState, raft.state)
}

func TestElectionTimerStateMostOtherPeersVotesNo(t *testing.T) {
	service := fakeService{
		peers: map[string]fakePeer{
			"peer1": fakePeer{term: 0, voteYes: false},
			"peer2": fakePeer{term: 0, voteYes: false},
			"peer3": fakePeer{term: 0, voteYes: true},
			"peer4": fakePeer{term: 0, voteYes: false},
		},
	}

	peers := []string{"peer1", "peer2", "peer3", "peer4"}
	raft, err := NewRaft(&service, time.Second, peers)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()

	raft.leaderLastHeartbeat = time.Now().Add(-time.Second)

	go raft.electionTimer(ctx)

	<-ctx.Done()

	// check the state is still follower
	assert.Equal(t, followerState, raft.state)
}

func TestElectionTimerStateAllOtherPeersVotesNo(t *testing.T) {
	service := fakeService{
		peers: map[string]fakePeer{
			"peer1": fakePeer{term: 0, voteYes: false},
			"peer2": fakePeer{term: 0, voteYes: false},
		},
	}

	peers := []string{"peer1", "peer2"}
	raft, err := NewRaft(&service, time.Second, peers)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	raft.leaderLastHeartbeat = time.Now().Add(-time.Second)

	go raft.electionTimer(ctx)

	<-ctx.Done()

	// check the state is still follower
	assert.Equal(t, followerState, raft.state)
}

func TestElectionTimerStateOtherPeerHasHigherTerm(t *testing.T) {
	service := fakeService{
		peers: map[string]fakePeer{
			"peer1": fakePeer{term: 2, voteYes: false},
			"peer2": fakePeer{term: 0, voteYes: true},
		},
	}

	peers := []string{"peer1", "peer2"}
	raft, err := NewRaft(&service, time.Second, peers)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()

	raft.leaderLastHeartbeat = time.Now().Add(-time.Second)

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

func TestElectionTimerStateOnePeerErrors(t *testing.T) {
	service := fakeService{
		peers: map[string]fakePeer{
			"peer1": fakePeer{err: fmt.Errorf("something bad happened")},
			"peer2": fakePeer{term: 0, voteYes: true},
		},
	}

	peers := []string{"peer1", "peer2"}
	raft, err := NewRaft(&service, time.Second, peers)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()

	raft.leaderLastHeartbeat = time.Now().Add(-time.Second)

	go raft.electionTimer(ctx)

	<-ctx.Done()

	// check the state is leader
	assert.Equal(t, leaderState, raft.state)
}

func TestElectionTimerStateOneTakesTooLongToRespond(t *testing.T) {
	service := fakeService{
		peers: map[string]fakePeer{
			"peer1": fakePeer{requestDuration: time.Second * 10},
			"peer2": fakePeer{term: 0, voteYes: true},
		},
	}

	peers := []string{"peer1", "peer2"}
	raft, err := NewRaft(&service, time.Second, peers)
	require.NoError(t, err)

	// TODO: work out how to make these tests quicker without this wait
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	raft.leaderLastHeartbeat = time.Now().Add(-time.Second)
	//raft.peers = []Peer{Peer{ID: "peer1"}, Peer{ID: "peer2"}}

	go raft.electionTimer(ctx)

	<-ctx.Done()

	// check the state is leader
	assert.Equal(t, leaderState, raft.state)
}
