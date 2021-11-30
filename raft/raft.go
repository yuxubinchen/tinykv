// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"

	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id   uint64
	Aws  int
	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	m1 := make(map[uint64]*Progress)
	m2 := make(map[uint64]bool)
	for _, i := range c.peers {
		m1[i] = nil
		m2[i] = false
	}
	return &Raft{
		id:    c.ID,
		State: StateFollower,
		RaftLog: &RaftLog{
			storage: c.Storage,
		},
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		votes:            m2,
		Prs:              m1,
	}
	return nil
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {

}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower:
		{
			r.electionElapsed++
			if r.electionElapsed >= r.electionTimeout {
				r.becomeCandidate()
				r.Step(pb.Message{
					MsgType: eraftpb.MessageType_MsgHup,
					From:    r.id,
					Term:    r.Term,
				})
			}
		}
	case StateCandidate:
		{
			r.electionElapsed++
			if r.electionElapsed >= r.electionTimeout {
				r.becomeCandidate()
				r.Step(pb.Message{
					MsgType: eraftpb.MessageType_MsgHup,
					From:    r.id,
					Term:    r.Term,
				})
			}
		}
	case StateLeader:
		{
			r.heartbeatElapsed++
			if r.heartbeatElapsed >= r.heartbeatTimeout {
				r.Step(pb.Message{
					MsgType: eraftpb.MessageType_MsgBeat,
					To:      r.id,
					From:    r.id,
					Term:    r.Term + 1,
				})
			}
		}
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Aws = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Term++
	r.Aws = 0
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.State = StateLeader
	// NOTE: Leader should propose a noop entry on its term
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		{
			switch m.MsgType {
			case eraftpb.MessageType_MsgHup:
				{
					r.becomeCandidate()
					r.Step(pb.Message{
						MsgType: eraftpb.MessageType_MsgHup,
						From:    r.id,
						Term:    r.Term,
					})
				}
			case eraftpb.MessageType_MsgPropose:
				{
				}
			case eraftpb.MessageType_MsgAppend:
				{
					if r.Term < m.Term {
						r.Term = m.Term
					}
				}
			case eraftpb.MessageType_MsgAppendResponse:
				{
					if r.Term > m.Term {
						r.Step(pb.Message{
							MsgType: eraftpb.MessageType_MsgRequestVoteResponse,
							From:    r.id,
							Term:    r.Term,
							Reject:  false,
						})
					} else {
						r.Step(pb.Message{
							MsgType: eraftpb.MessageType_MsgRequestVoteResponse,
							From:    r.id,
							To:      m.From,
							Term:    r.Term,
							Reject:  true,
						})
					}
				}
			case eraftpb.MessageType_MsgRequestVote:
				{
					if r.Term > m.Term {
						r.msgs = append(r.msgs, pb.Message{
							MsgType: eraftpb.MessageType_MsgRequestVoteResponse,
							From:    r.id,
							Term:    r.Term,
							Reject:  false,
						})
					} else {
						r.msgs = append(r.msgs, pb.Message{
							MsgType: eraftpb.MessageType_MsgRequestVoteResponse,
							From:    r.id,
							To:      m.From,
							Term:    r.Term,
							Reject:  true,
						})
					}
				}
			case eraftpb.MessageType_MsgSnapshot:
			case eraftpb.MessageType_MsgHeartbeatResponse:
			case eraftpb.MessageType_MsgTimeoutNow:
			}
		}
	case StateCandidate:
		{
			switch m.MsgType {
			case eraftpb.MessageType_MsgHup:
				{
					r.electionElapsed = 0
					for j, _ := range r.votes {
						if j == r.id {
							continue
						}
						r.msgs = append(r.msgs, pb.Message{
							MsgType: eraftpb.MessageType_MsgRequestVote,
							To:      j,
							From:    r.id,
							Term:    r.Term,
						})
					}
				}
			case eraftpb.MessageType_MsgPropose:
			case eraftpb.MessageType_MsgAppend:
				{
					if r.Term < m.Term {
						r.becomeFollower(r.Term, r.Lead)
						r.Term = m.Term
					}
				}
			case eraftpb.MessageType_MsgAppendResponse:
			case eraftpb.MessageType_MsgRequestVote:
				{
					r.msgs = append(r.msgs, pb.Message{
						MsgType: eraftpb.MessageType_MsgRequestVoteResponse,
						From:    r.id,
						To:      m.From,
						Term:    r.Term,
						Reject:  false,
					})
				}
			case eraftpb.MessageType_MsgRequestVoteResponse:
				{
					r.votes[m.From] = !m.Reject
					if m.Reject == true {
						r.Aws++
					}
					if r.Aws >= len(r.votes)/2 {
						r.becomeLeader()
					}
				}
			case eraftpb.MessageType_MsgSnapshot:
			case eraftpb.MessageType_MsgHeartbeatResponse:
			case eraftpb.MessageType_MsgTimeoutNow:
			}
		}
	case StateLeader:
		{
			switch m.MsgType {
			case eraftpb.MessageType_MsgBeat:
				{
					for j, _ := range r.votes {
						if j == r.id {
							continue
						}
						r.Step(pb.Message{
							MsgType: eraftpb.MessageType_MsgHeartbeat,
							To:      j,
							From:    r.id,
							Term:    r.Term,
						})
					}
				}
			case eraftpb.MessageType_MsgPropose:
				{
					r.msgs = append(r.msgs, pb.Message{
						MsgType: m.MsgType,
						To:      m.To,
						From:    r.id,
						Term:    r.Term,
						Entries: m.Entries,
					})
				}
			case eraftpb.MessageType_MsgAppend:
				{
					if r.Term < m.Term {
						r.becomeFollower(r.Term, r.Lead)
						r.Term = m.Term
					}
					r.msgs = append(r.msgs, pb.Message{
						MsgType: m.MsgType,
						To:      m.To,
						From:    r.id,
						Term:    r.Term,
						Entries: m.Entries,
					})
				}
			case eraftpb.MessageType_MsgSnapshot:
			case eraftpb.MessageType_MsgAppendResponse:
			case eraftpb.MessageType_MsgRequestVote:
				{
					if r.Term > m.Term {
						r.msgs = append(r.msgs, pb.Message{
							MsgType: eraftpb.MessageType_MsgRequestVoteResponse,
							From:    r.id,
							Term:    r.Term,
							Reject:  false,
						})
					} else {
						r.becomeFollower(r.Term, 0)
						r.msgs = append(r.msgs, pb.Message{
							MsgType: eraftpb.MessageType_MsgRequestVoteResponse,
							From:    r.id,
							To:      m.From,
							Term:    r.Term,
							Reject:  true,
						})
					}
				}
			case eraftpb.MessageType_MsgHeartbeat:
				{
					r.msgs = append(r.msgs, pb.Message{
						MsgType: eraftpb.MessageType_MsgHeartbeat,
						To:      m.To,
						From:    r.id,
						Term:    r.Term,
						Entries: nil,
					})
				}
			case eraftpb.MessageType_MsgHeartbeatResponse:
			case eraftpb.MessageType_MsgTransferLeader:
			}
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
