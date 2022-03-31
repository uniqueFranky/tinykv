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
	"fmt"
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
	votes   map[uint64]bool
	haveGot map[uint64]bool
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
		prs[i] = &Progress{0, 1}
	}
	hardstate, _, _ := c.Storage.InitialState()

	rand.Seed(time.Now().UnixNano())
	r := Raft{
		id:               c.ID,
		Term:             hardstate.Term,
		Vote:             hardstate.Vote,
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
		haveGot:          make(map[uint64]bool),
	}
	return &r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	match := r.Prs[to].Match
	next := r.Prs[to].Next
	//fmt.Println("send to", to, "next = ", next)
	logTerm, _ := r.RaftLog.Term(match)
	snapshot, _ := r.RaftLog.storage.Snapshot()
	ents := make([]*pb.Entry, 0, 1)
	for i := next; i <= r.RaftLog.LastIndex(); i++ {
		ents = append(ents, &r.RaftLog.entries[i-snapshot.Metadata.Index-1])
	}
	msg := pb.Message{
		MsgType:              pb.MessageType_MsgAppend,
		To:                   to,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              logTerm,
		Index:                match,
		Entries:              ents,
		Commit:               r.RaftLog.committed,
		Snapshot:             nil,
		Reject:               false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}
	r.msgs = append(r.msgs, msg)
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
		Commit:               r.committed,
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
				Commit:               r.RaftLog.committed,
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
	r.haveGot = make(map[uint64]bool)
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
	r.haveGot = make(map[uint64]bool)
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
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		EntryType:            pb.EntryType_EntryNormal,
		Term:                 r.Term,
		Index:                r.RaftLog.LastIndex() + 1,
		Data:                 nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	})
	r.Prs[r.id].Match++
	r.Prs[r.id].Next++
	for _, v := range r.peers {
		if v == r.id {
			continue
		}
		r.Prs[v].Match = 0
		r.Prs[v].Next = 1
		r.sendAppend(v)
	}
	r.haveGot = make(map[uint64]bool)
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
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartBeatResponse(m)
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

func (r *Raft) updateLog(ents []*pb.Entry, rp int) {
	ep := 0
	tmp := make([]pb.Entry, 0, 1)
	for rp < len(r.RaftLog.entries) && ep < len(ents) {
		if r.RaftLog.entries[rp].Index < ents[ep].Index {
			tmp = append(tmp, r.RaftLog.entries[rp])
			rp++
		} else if r.RaftLog.entries[rp].Index > ents[ep].Index {
			tmp = append(tmp, *ents[ep])
			ep++
		} else {
			if r.RaftLog.entries[rp].Term == ents[ep].Term {
				tmp = append(tmp, *ents[ep])
				ep++
				rp++
			} else {
				for ; ep < len(ents); ep++ {
					tmp = append(tmp, *ents[ep])
				}
			}
		}
	}
	for ; rp < len(r.RaftLog.entries); rp++ {
		tmp = append(tmp, r.RaftLog.entries[rp])
	}
	for ; ep < len(ents); ep++ {
		tmp = append(tmp, *ents[ep])
	}
	r.RaftLog.entries = tmp
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).

	if m.Term < r.Term { //refuse if the message is from a stale leader
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
	if m.Entries != nil && len(m.Entries) == 0 {
		if m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
		}
		return
	}
	//ok := false
	//if m.Index == 0 {
	//	tmp := make([]pb.Entry, 0, 1)
	//	for _, v := range m.Entries {
	//		tmp = append(tmp, *v)
	//	}
	//	r.RaftLog.entries = tmp
	//	if m.Commit > r.RaftLog.committed {
	//		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	//	}
	//	t, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	//	r.msgs = append(r.msgs, pb.Message{
	//		MsgType:              pb.MessageType_MsgAppendResponse,
	//		To:                   m.From,
	//		From:                 r.id,
	//		Term:                 r.Term,
	//		LogTerm:              t,
	//		Index:                r.RaftLog.LastIndex(),
	//		Entries:              nil,
	//		Commit:               0,
	//		Snapshot:             nil,
	//		Reject:               false,
	//		XXX_NoUnkeyedLiteral: struct{}{},
	//		XXX_unrecognized:     nil,
	//		XXX_sizecache:        0,
	//	})
	//	return
	//}
	////to find a pos where (the entry before the first entry in the message) matches with the node's log
	//for i, v := range r.RaftLog.entries {
	//	if v.Index == m.Index {
	//		ok = true
	//		if v.Term != m.LogTerm { //delete it and update
	//			tmp := make([]pb.Entry, 0, 1)
	//			for j := 0; j < i; j++ {
	//				tmp = append(tmp, r.RaftLog.entries[j])
	//			}
	//			for _, p := range m.Entries {
	//				tmp = append(tmp, *p)
	//			}
	//			r.RaftLog.entries = tmp
	//		} else {
	//			r.updateLog(m.Entries, i+1)
	//		}
	//		break
	//	}
	//}
	//t, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	//msg := pb.Message{
	//	MsgType:              pb.MessageType_MsgAppendResponse,
	//	To:                   m.From,
	//	From:                 r.id,
	//	Term:                 r.Term,
	//	LogTerm:              t,
	//	Index:                r.RaftLog.LastIndex(),
	//	Entries:              nil,
	//	Commit:               0,
	//	Snapshot:             nil,
	//	Reject:               false,
	//	XXX_NoUnkeyedLiteral: struct{}{},
	//	XXX_unrecognized:     nil,
	//	XXX_sizecache:        0,
	//}
	//if ok == false {
	//	msg.Reject = true
	//}
	//r.msgs = append(r.msgs, msg)
	//if m.Commit > r.RaftLog.committed {
	//	r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	//}
	ok := false
	var pos uint64
	for i, v := range r.RaftLog.entries {
		if v.Term == m.LogTerm && v.Index == m.Index {
			ok = true
			pos = uint64(i) + 1
			break
		}
	}
	if m.Index == 0 { //the first log
		ok = true
		pos = 0
	}
	if ok == false {
		r.msgs = append(r.msgs, pb.Message{
			MsgType:              pb.MessageType_MsgAppendResponse,
			To:                   m.From,
			From:                 r.id,
			Term:                 r.Term,
			LogTerm:              m.LogTerm,
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
	tmp := make([]pb.Entry, 0, 1)
	for i := 0; uint64(i) < pos; i++ {
		tmp = append(tmp, r.RaftLog.entries[i])
	}
	//to check if there is an unmatched entry
	snapshot, _ := r.RaftLog.storage.Snapshot()
	consist := true
	for ii, v := range m.Entries {
		i := uint64(ii)
		if i+pos < uint64(len(r.RaftLog.entries)) {

			if v.Term != r.RaftLog.entries[i+pos].Term || v.Index != r.RaftLog.entries[i+pos].Index { //unmatched
				for j := i; j < uint64(len(m.Entries)); j++ {
					tmp = append(tmp, *m.Entries[j])
				}
				r.RaftLog.stabled = i + pos - 1 + snapshot.Metadata.Index + 1
				consist = false
				break
			} else { //match
				tmp = append(tmp, *m.Entries[i])
			}
		} else {
			tmp = append(tmp, *m.Entries[i])
		}
	}
	if consist && int(pos)+len(m.Entries) < len(r.RaftLog.entries) {
		tmp = append(tmp, r.RaftLog.entries[int(pos)+len(m.Entries):]...)
	}
	r.RaftLog.entries = tmp
	r.msgs = append(r.msgs, pb.Message{
		MsgType:              pb.MessageType_MsgAppendResponse,
		To:                   m.From,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              0,
		Index:                r.RaftLog.LastIndex(),
		Entries:              nil,
		Commit:               0,
		Snapshot:             nil,
		Reject:               false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	})
	//fmt.Println("m.comit", m.Commit, "r.comit", r.RaftLog.committed, "lasrindex:", r.RaftLog.LastIndex())
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
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
			Commit:               m.Commit,
			Snapshot:             nil,
			Reject:               false,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		})
	}
}
func (r *Raft) handleVoteResponse(m pb.Message) {
	got := 0
	r.haveGot[m.From] = true
	if m.Reject == false {
		r.votes[m.From] = true
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, 0)
		return
	}

	for _, i := range r.peers {
		if r.votes[i] == true {
			got++
		}
	}

	if got > len(r.peers)/2 {
		r.becomeLeader()
	}

	//for _, i := range r.peers {
	//	if r.haveGot[i] == true {
	//		got++
	//	}
	//	if r.votes[i] == true {
	//		get++
	//	}
	//}
	//if get > len(r.peers)/2 {
	//	r.becomeLeader()
	//} else if got > len(r.peers)/2 {
	//	r.becomeFollower(m.Term, 0)
	//}
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
	r.RaftLog.committed = m.Commit
	t, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	msg := pb.Message{
		MsgType:              pb.MessageType_MsgAppendResponse,
		To:                   m.From,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              t,
		Index:                r.RaftLog.LastIndex(),
		Entries:              nil,
		Commit:               r.RaftLog.committed,
		Snapshot:             nil,
		Reject:               false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}
	r.msgs = append(r.msgs, msg)
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
	if m.Term < r.Term || (m.Term == r.Term && r.Vote != 0 && r.Vote != m.From) {
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
		r.becomeFollower(m.Term, 0)
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
	t, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	if m.LogTerm > t || (m.LogTerm == t && m.Index >= r.RaftLog.LastIndex()) {
		if r.Vote == 0 || r.Vote == m.From {
			r.Vote = m.From
			r.becomeFollower(m.Term, m.From)
			r.msgs = append(r.msgs, msg)
		} else {
			msg.Reject = true
			r.msgs = append(r.msgs, msg)
		}
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
		t, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
		r.msgs = append(r.msgs, pb.Message{
			MsgType:              pb.MessageType_MsgRequestVote,
			To:                   i,
			From:                 r.id,
			Term:                 r.Term,
			LogTerm:              t,
			Index:                r.RaftLog.LastIndex(),
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
	ents := make([]pb.Entry, 0, 1)
	for i, v := range m.Entries {
		v.Term = r.Term
		v.EntryType = pb.EntryType_EntryNormal
		v.Index = r.RaftLog.LastIndex() + uint64(i+1)
		ents = append(ents, *v)
	}
	r.Prs[r.id].Match += uint64(len(ents))
	r.Prs[r.id].Next += uint64(len(ents))
	r.RaftLog.entries = append(r.RaftLog.entries, ents...)
	for _, i := range r.peers {
		if i == r.id {
			continue
		}
		r.sendAppend(i)
	}
	if len(r.peers) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Reject == false {
		r.Prs[m.From] = &Progress{
			Match: m.Index,
			Next:  m.Index + 1,
		}
		//fmt.Println(m.From, "Match:", r.Prs[m.From].Match, "Next:", r.Prs[m.From].Next)
		if m.Index <= r.RaftLog.committed {
			return
		}
		sat := 0
		for _, i := range r.peers {
			if i == r.id {
				continue
			}
			if r.Prs[i].Match >= m.Index {
				sat++
			}
		}
		t, _ := r.RaftLog.Term(m.Index)
		if sat+1 > len(r.peers)/2 && (t == r.Term) {

			r.RaftLog.committed = m.Index
			for _, v := range r.peers {
				if v == r.id {
					continue
				}
				r.sendAppend(v)
			}
		}

	} else {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, 0)
		} else {
			r.Prs[m.From].Next--
			r.Prs[m.From].Match--
			fmt.Println("Rejected")
			r.sendAppend(m.From)
		}
	}
}

func (r *Raft) handleHeartBeatResponse(m pb.Message) {

	if m.Index < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}

}
