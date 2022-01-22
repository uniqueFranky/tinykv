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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"time"
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
	id uint64

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

	randEleTimeout int
	peers          []uint64
	committed      uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	prs := make(map[uint64]*Progress)
	for _, i := range c.peers {
		prs[i] = &Progress{0, 0}
	}
	rand.Seed(time.Now().UnixNano())
	r := Raft{
		id:               c.ID,
		Term:             0,
		Vote:             0,
		RaftLog:          newLog(c.Storage),
		Prs:              prs,
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             nil,
		Lead:             0,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
		peers:            c.peers,
		committed:        0,
		randEleTimeout:   c.ElectionTick + rand.Intn(c.ElectionTick),
	}
	return &r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	ent := make([]*pb.Entry, 0, 1)
	snapshot, _ := r.RaftLog.storage.Snapshot()
	ent = append(ent, &r.RaftLog.entries[r.Prs[to].Next-snapshot.Metadata.Index])
	logTerm, err := r.RaftLog.Term(r.Prs[to].Next)
	if err != nil {
		return false
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType:              pb.MessageType_MsgAppend,
		To:                   to,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              logTerm,
		Index:                r.Prs[to].Next,
		Entries:              ent,
		Commit:               0,
		Snapshot:             nil,
		Reject:               false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType:              pb.MessageType_MsgHeartbeat,
		To:                   to,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              0,
		Index:                0,
		Entries:              nil,
		Commit:               0,
		Snapshot:             nil,
		Reject:               false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed == r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			msg := pb.Message{
				MsgType:              pb.MessageType_MsgBeat,
				To:                   r.id,
				From:                 r.id,
				Term:                 r.Term,
				LogTerm:              0,
				Index:                0,
				Entries:              nil,
				Commit:               0,
				Snapshot:             nil,
				Reject:               false,
				XXX_NoUnkeyedLiteral: struct{}{},
				XXX_unrecognized:     nil,
				XXX_sizecache:        0,
			}
			r.Step(msg)
		}
		r.electionElapsed++
	case StateCandidate:
		fallthrough
	case StateFollower:
		r.electionElapsed++
		if r.electionElapsed == r.randEleTimeout {
			msg := pb.Message{
				MsgType:              pb.MessageType_MsgHup,
				To:                   r.id,
				From:                 r.id,
				Term:                 r.Term,
				LogTerm:              0,
				Index:                0,
				Entries:              nil,
				Commit:               0,
				Snapshot:             nil,
				Reject:               false,
				XXX_NoUnkeyedLiteral: struct{}{},
				XXX_unrecognized:     nil,
				XXX_sizecache:        0,
			}
			r.randEleTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
			r.Step(msg)
		}
	default:
		log.Panic("Unknown State.")
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if term > r.Term {
		r.Vote = 0
	}
	r.Term = term
	r.Lead = lead
	r.electionElapsed = 0
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.State = StateCandidate
	r.electionElapsed = 0
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true

}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.Lead = r.id
	r.Vote = 0
	for _, v := range r.peers {
		if v == r.id {
			continue
		}
		r.sendAppend(v)
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {

	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
			return nil
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)
			return nil
		case pb.MessageType_MsgPropose:
			r.handlePropose(m)
			return nil
		case pb.MessageType_MsgBeat:
			r.handleBeat(m)
			return nil
		case pb.MessageType_MsgRequestVote:
			r.handleVoteRequest(m)
			return nil
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
			return nil
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
			return nil

		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
			return nil
		case pb.MessageType_MsgRequestVote:
			r.handleVoteRequest(m)
			return nil
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleVoteResponse(m)
			return nil
		case pb.MessageType_MsgHup:
			r.handleHup(m)
			return nil
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
			return nil
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
			return nil
		}
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
			return nil
		case pb.MessageType_MsgRequestVote:
			r.handleVoteRequest(m)
			return nil
		case pb.MessageType_MsgHup:
			r.handleHup(m)
			return nil
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
			return nil
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
			return nil
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType:              pb.MessageType_MsgAppendResponse,
			To:                   m.From,
			From:                 r.id,
			Term:                 r.Term,
			LogTerm:              0,
			Index:                0,
			Entries:              nil,
			Commit:               0,
			Snapshot:             nil,
			Reject:               true,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		})
		return
	}
	r.becomeFollower(m.Term, m.From)
	if m.Commit > r.committed {
		r.committed = m.Commit
	}
	if m.Entries != nil {
		for _, v := range m.Entries {
			if v.Data != nil {
				r.RaftLog.entries = append(r.RaftLog.entries, *v)
			}
		}
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType:              pb.MessageType_MsgAppendResponse,
		To:                   m.From,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              0,
		Index:                0,
		Entries:              nil,
		Commit:               0,
		Snapshot:             nil,
		Reject:               false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	})
}

func (r *Raft) handleBeat(m pb.Message) {
	for _, i := range r.peers {
		if i == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType:              pb.MessageType_MsgHeartbeat,
			To:                   i,
			From:                 r.id,
			Term:                 m.Term,
			LogTerm:              0,
			Index:                0,
			Entries:              nil,
			Commit:               0,
			Snapshot:             nil,
			Reject:               false,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		})
	}
}
func (r *Raft) handleVoteResponse(m pb.Message) {
	get := 0
	if m.Reject == false {
		if r.votes[m.From] == false {
			r.Vote++
		}
		r.votes[m.From] = true
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, 0)
		return
	}
	for _, i := range r.peers {
		if r.votes[i] == true {
			get++
		}
	}
	if get > len(r.peers)/2 {
		r.becomeLeader()
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType:              pb.MessageType_MsgHeartbeatResponse,
			To:                   m.From,
			From:                 r.id,
			Term:                 r.Term,
			LogTerm:              0,
			Index:                0,
			Entries:              nil,
			Commit:               0,
			Snapshot:             nil,
			Reject:               true,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		})
		return
	}
	r.becomeFollower(m.Term, m.From)
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

func (r *Raft) handleVoteRequest(m pb.Message) {
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType:              pb.MessageType_MsgRequestVoteResponse,
			To:                   m.From,
			From:                 r.id,
			Term:                 r.Term,
			LogTerm:              0,
			Index:                0,
			Entries:              nil,
			Commit:               0,
			Snapshot:             nil,
			Reject:               true,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		})
		return
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	msg := pb.Message{
		MsgType:              pb.MessageType_MsgRequestVoteResponse,
		To:                   m.From,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              0,
		Index:                0,
		Entries:              nil,
		Commit:               0,
		Snapshot:             nil,
		Reject:               false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}
	if r.Vote == 0 || r.Vote == m.From {
		r.Vote = m.From
		r.msgs = append(r.msgs, msg)
	} else {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
	}
}

func (r *Raft) handleHup(m pb.Message) {
	r.becomeCandidate()
	var i uint64
	for _, i = range r.peers {
		if i == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType:              pb.MessageType_MsgRequestVote,
			To:                   i,
			From:                 r.id,
			Term:                 r.Term,
			LogTerm:              0,
			Index:                0,
			Entries:              nil,
			Commit:               0,
			Snapshot:             nil,
			Reject:               false,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		})
	}
	if len(r.peers) == 1 {
		r.becomeLeader()
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	if r.State != StateLeader {
		return
	}
	snapshot, _ := r.RaftLog.storage.Snapshot()

	for _, e := range m.Entries {
		lastEnt := r.RaftLog.entries[r.RaftLog.LastIndex()-snapshot.Metadata.Index]
		ne := *e
		ne.Term = r.Term
		ne.Index = lastEnt.Index + 1
		r.RaftLog.entries = append(r.RaftLog.entries, ne)
		for _, i := range r.peers {
			if i == r.id {
				continue
			}
			r.msgs = append(r.msgs, pb.Message{
				MsgType:              pb.MessageType_MsgAppend,
				To:                   i,
				From:                 r.id,
				Term:                 r.Term,
				LogTerm:              r.Term,
				Index:                lastEnt.Index,
				Entries:              []*pb.Entry{&ne},
				Commit:               r.committed,
				Snapshot:             nil,
				Reject:               false,
				XXX_NoUnkeyedLiteral: struct{}{},
				XXX_unrecognized:     nil,
				XXX_sizecache:        0,
			})
		}
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Reject == false {
		r.Prs[m.From] = &Progress{
			Match: m.Index - 1,
			Next:  m.Index,
		}
		cm := 0
		for _, i := range r.peers {
			if i == r.id {
				continue
			}
			if r.Prs[i].Next > m.Commit {
				cm++
			}
		}
		if cm+1 > len(r.peers)/2 {
			r.committed++
		}
	}
}
