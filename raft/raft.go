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

// validate checks if the config is valid
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
	id uint64 // 节点id

	Term uint64 // 当前任期
	Vote uint64 // 当前任期的给票的节点

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

	// initialize the raft with configuration
	r := &Raft{
		id:               c.ID,
		Term:             0, // 初始为0
		Vote:             None,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress), // 初始化 Prs map
		votes:            make(map[uint64]bool),      // 初始化 votes map
		State:            StateFollower,              // 等待leader的心跳
		heartbeatTimeout: c.HeartbeatTick,
		heartbeatElapsed: 0,
		electionTimeout:  c.ElectionTick,
		electionElapsed:  0,
	}

	for _, peer := range c.peers {
		r.Prs[peer] = &Progress{}
		r.votes[peer] = false
	}

	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {

	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	heartbeat_message := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, heartbeat_message)
}

// tick advances the internal logical clock by a single tick.
// 此处tick模拟的是逻辑心跳, 即调用tick()函数时, 逻辑心跳时间+1
func (r *Raft) tick() {
	r.electionElapsed++
	r.heartbeatElapsed++

	switch r.State {
	// leader 只关注心跳
	case StateLeader:
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			// 向所有fellower发送心跳信息
			for id := range r.votes {
				if id != r.id {
					r.sendHeartbeat(id)
				}
			}
		}
	// follower 和 candidate 关注选举超时
	case StateFollower, StateCandidate:
		if r.electionElapsed >= r.electionTimeout {
			r.becomeCandidate()
			r.Step(pb.Message{From: r.id, To: None, MsgType: pb.MessageType_MsgHup})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Term = term
	r.Vote = 0
	r.Lead = lead
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	
	// 清空投票
	for id := range r.votes {
		r.votes[id] = false
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.electionElapsed = 0
	// 选举超时，任期加一
	r.State = StateCandidate
	r.Term++
	// 发起选举，给自己投票
	r.Vote = r.id
	r.votes[r.id] = true

	// 向集群的每个节点发起拉票选举
	for id := range r.votes {
		if id != r.id {
			r.Step(pb.Message{From: r.id, To: id, MsgType: pb.MessageType_MsgRequestVote})
		}
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// 设置状态
	r.State = StateLeader
	r.Lead = r.id

	// 重置计时器
	r.electionElapsed = 0
	r.heartbeatElapsed = 0

	// 初始化所有节点的进度
	// 获取当前最后一个日志索引
	lastIndex := r.RaftLog.LastIndex()
	for id, pr := range r.Prs {
		if id == r.id {
			// Leader 的进度：Match 指向最后一条日志，Next 指向下一条待发送的日志
			pr.Match = lastIndex
			pr.Next = lastIndex + 1
		} else {
			// Follower 的进度：初始为 0，Next 从 lastIndex + 1 开始
			pr.Match = 0
			pr.Next = lastIndex + 1
		}
	}

	// 5. 提议一个 no-op entry
	// 这确保 leader 在其任期提交了至少一个条目，这是 Raft 的要求
	// no-op entry 是一个空 entry（Data 为空），用于确认 leadership
	r.Step(pb.Message{
		From:    r.id,
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{}},
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	switch m.MsgType {
	// 'MessageType_MsgHup' is a local message used for election. If an election timeout happened,
	// the node should pass 'MessageType_MsgHup' to its Step method and start a new election.
	case pb.MessageType_MsgHup:
	 
	// 'MessageType_MsgBeat' is a local message that signals the leader to send a heartbeat
	// of the 'MessageType_MsgHeartbeat' type to its followers.
	case pb.MessageType_MsgBeat:
		
	// 'MessageType_MsgPropose' is a local message that proposes to append data to the leader's log entries.
	case pb.MessageType_MsgPropose:

	// 'MessageType_MsgAppend' contains log entries to replicate.
	case pb.MessageType_MsgAppend:
	
	// 'MessageType_MsgAppendResponse' is response to log replication request('MessageType_MsgAppend').
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntries(m)
	// 'MessageType_MsgRequestVote' requests votes for election.
	case pb.MessageType_MsgRequestVote:
	
	// 'MessageType_MsgRequestVoteResponse' contains responses from voting request.
	case pb.MessageType_MsgRequestVoteResponse:
	
	// 'MessageType_MsgSnapshot' requests to install a snapshot message.
	case pb.MessageType_MsgSnapshot:
	
	// 'MessageType_MsgHeartbeat' sends heartbeat from leader to its followers.
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)

	// 'MessageType_MsgHeartbeatResponse' is a response to 'MessageType_MsgHeartbeat'
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeat(m)
	// 'MessageType_MsgTransferLeader' requests the leader to transfer its leadership.
	case pb.MessageType_MsgTransferLeader:
	
	// 'MessageType_MsgTimeoutNow' send from the leader to the leadership transfer target, to let
	// the transfer target timeout immediately and start a new election.
	case pb.MessageType_MsgTimeoutNow:
	
	default:
		fmt.Println("Unknown message type")
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	switch r.State {
	case StateLeader:
		// leader
		if r.Term < m.Term {
			r.becomeFollower(m.Term, m.From)
		}
	case StateFollower:
		// follower
		if r.Term <= m.Term {
			r.Term = m.Term
			r.electionElapsed = 0
		}
	case StateCandidate:
		// candidate
		if r.Term <= m.Term {
			r.becomeFollower(m.Term, m.From)
		}
	}
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
