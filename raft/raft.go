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
	// 获取该 peer 的进度
	pr, ok := r.Prs[to]
	if !ok {
		return false
	}

	// prevLogIndex 是该 peer 下一个需要接收的日志索引的前一个
	prevLogIndex := pr.Next - 1

	// 获取 prevLogIndex 对应的 term
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		return false
	}

	// 获取要发送的 entries (从 prevLogIndex + 1 开始)
	var entries []*pb.Entry
	lastIndex := r.RaftLog.LastIndex()
	if pr.Next <= lastIndex {
		// 需要发送日志条目
		// 遍历 entries 获取从 pr.Next 开始的条目
		for i := pr.Next; i <= lastIndex; i++ {
			// 从 RaftLog.entries 中获取
			if len(r.RaftLog.entries) > 0 {
				firstEntryIndex := r.RaftLog.entries[0].Index
				if i >= firstEntryIndex {
					offset := i - firstEntryIndex
					if int(offset) < len(r.RaftLog.entries) {
						ent := r.RaftLog.entries[offset]
						entries = append(entries, &ent)
						continue
					}
				}
			}
			// 从 storage 获取
			ents, err := r.RaftLog.storage.Entries(i, i+1)
			if err == nil && len(ents) > 0 {
				entries = append(entries, &ents[0])
			}
		}
	}

	// 构造 MsgAppend 消息
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}

	r.msgs = append(r.msgs, msg)
	return true
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
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	// follower 和 candidate 关注选举超时
	// 只有超时的时候才会进行选举操作
	case StateFollower, StateCandidate:
		if r.electionElapsed >= r.electionTimeout {
			r.becomeCandidate()
			// 选举操作
			for id := range r.votes {
				r.votes[id] = false
			}
			r.Vote = r.id
			r.votes[r.id] = true
			r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetVote()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0

	// 选举超时，任期加一
	r.State = StateCandidate
	r.Term++
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.State = StateLeader
	r.Lead = r.id

	r.electionElapsed = 0
	r.heartbeatElapsed = 0

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

	// 提议一个 no-op entry
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
		r.becomeCandidate()
		r.Vote = r.id
		r.votes = make(map[uint64]bool)
		r.votes[r.id] = true

		for id := range r.Prs {
			if id != r.id {
				r.msgs = append(r.msgs, pb.Message{
					From:    r.id,
					To:      id,
					Term:    r.Term,
					MsgType: pb.MessageType_MsgRequestVote,
					Index:   r.RaftLog.LastIndex(),
					LogTerm: r.RaftLog.LastTerm(),
				})
			}
		}

		if len(r.votes) > len(r.Prs)/2 {
			r.becomeLeader()
		}

	// 'MessageType_MsgBeat' is a local message that signals the leader to send a heartbeat
	// of the 'MessageType_MsgHeartbeat' type to its followers.
	case pb.MessageType_MsgBeat:
		for id := range r.Prs {
			if id != r.id {
				r.sendHeartbeat(id)
			}
		}

	// 'MessageType_MsgPropose' is a local message that proposes to append data to the leader's log entries.
	case pb.MessageType_MsgPropose:

	// 'MessageType_MsgAppend' contains log entries to replicate.
	case pb.MessageType_MsgAppend:
		// fellower receive from leader for msgappend
		r.handleAppendEntries(m)

	// 'MessageType_MsgAppendResponse' is response to log replication request('MessageType_MsgAppend').
	case pb.MessageType_MsgAppendResponse:
		// leader receive from follower for msgappendResponse
		r.handleAppendEntries(m)
	// 'MessageType_MsgRequestVote' requests votes for election.
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	// 'MessageType_MsgRequestVoteResponse' contains responses from voting request.
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVote(m)
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
// 处理两种情况:
// 1. Follower/Candidate 收到 Leader 的 MsgAppend
// 2. Leader 收到 Follower 的 MsgAppendResponse
func (r *Raft) handleAppendEntries(m pb.Message) {
	switch r.State {
	case StateLeader:
		// Leader 收到 MsgAppendResponse
		r.handleAppendEntriesResponse(m)
	case StateFollower, StateCandidate:
		// Follower/Candidate 收到 MsgAppend
		r.handleAppendEntriesRequest(m)
	}
}

// handleAppendEntriesRequest 处理 Follower/Candidate 收到的 MsgAppend
func (r *Raft) handleAppendEntriesRequest(m pb.Message) {
	// 规则1: 如果 term < 当前 term，拒绝
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}

	// 如果消息的 term 更大，或者当前是 candidate，转为 follower
	if m.Term > r.Term || r.State == StateCandidate {
		r.becomeFollower(m.Term, m.From)
	}

	// 更新 leader 信息和重置选举超时
	r.Lead = m.From
	r.electionElapsed = 0

	// 规则2: 检查 prevLogIndex 和 prevLogTerm 是否匹配
	if m.Index > 0 {
		prevLogTerm, err := r.RaftLog.Term(m.Index)
		if err != nil || prevLogTerm != m.LogTerm {
			// prevLog 不匹配，拒绝并告知当前 lastIndex 让 leader 回退
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Index:   r.RaftLog.LastIndex(),
				Reject:  true,
			})
			return
		}
	}

	// 规则3: 如果有新 entries，追加到日志
	if len(m.Entries) > 0 {
		// 找到与现有日志冲突的位置
		// 冲突定义：相同 index 但不同 term
		conflictIndex := uint64(0)
		for _, ent := range m.Entries {
			entryIndex := ent.Index
			entryTerm := ent.Term

			// 检查该位置是否已有条目
			existingTerm, err := r.RaftLog.Term(entryIndex)
			if err != nil {
				// 条目不存在，没有冲突，后续都是新条目
				break
			}
			if existingTerm != entryTerm {
				// 发现冲突，记录冲突位置
				conflictIndex = entryIndex
				break
			}
			// 如果 term 相同，说明已经存在相同的条目，继续检查下一个
		}

		// 如果发现冲突，需要从冲突位置开始覆盖
		if conflictIndex > 0 {
			// 从 conflictIndex 开始，用新条目覆盖
			// 需要清空现有的 unstable entries，并从冲突点开始重建
			r.RaftLog.entries = make([]pb.Entry, 0)

			// 找到冲突条目在 m.Entries 中的位置
			for _, ent := range m.Entries {
				if ent.Index >= conflictIndex {
					r.RaftLog.entries = append(r.RaftLog.entries, *ent)
				}
			}
		} else {
			// 没有冲突，追加不存在的新条目
			for _, ent := range m.Entries {
				// 检查是否已存在
				existingTerm, err := r.RaftLog.Term(ent.Index)
				if err != nil || existingTerm != ent.Term {
					// 不存在或不匹配，追加
					r.RaftLog.entries = append(r.RaftLog.entries, *ent)
				}
			}
		}
	}

	// 规则4: 更新 commit index
	if m.Commit > r.RaftLog.committed {
		// 计算当前最新的日志索引
		// 根据论文: commitIndex = min(leaderCommit, index of last new entry)
		// 如果没有新 entry，last new entry 是 prevLogIndex
		lastNewIndex := m.Index
		if len(m.Entries) > 0 {
			lastNewIndex = m.Entries[len(m.Entries)-1].Index
		}
		r.RaftLog.committed = min(m.Commit, lastNewIndex)
	}

	// 发送成功响应
	lastIndex := r.RaftLog.LastIndex()

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Index:   lastIndex,
		Reject:  false,
	})
}

// handleAppendEntriesResponse 处理 Leader 收到的 MsgAppendResponse
func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	// 如果响应的 term 更大，说明有更新的 leader，转为 follower
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}

	// 获取该 peer 的进度
	pr, ok := r.Prs[m.From]
	if !ok {
		return
	}

	if m.Reject {
		// 拒绝: prevLog 不匹配，回退 Next
		// 使用响应中的 Index 来调整 Next
		// 简单的回退策略: Next = Index + 1 (Index 是 follower 的 lastIndex)
		if m.Index > 0 {
			pr.Next = m.Index + 1
		} else {
			pr.Next = 1
		}
		// 重新发送
		r.sendAppend(m.From)
	} else {
		// 成功: 更新 Match 和 Next
		pr.Match = m.Index
		pr.Next = m.Index + 1

		// 尝试更新 committed
		r.maybeCommit()
	}
}

// maybeCommit 尝试提交日志
// 如果有超过半数的节点 Match >= 某个 index，则可以提交
func (r *Raft) maybeCommit() {
	// 遍历所有可能的 commit index，从高到低
	// 找到最大的可以被多数节点确认的 index
	for index := r.RaftLog.LastIndex(); index > r.RaftLog.committed; index-- {
		// 检查该 index 的 term 是否是当前 term（Raft 只能提交当前 term 的日志）
		term, err := r.RaftLog.Term(index)
		if err != nil || term != r.Term {
			continue
		}

		// 计算有多少节点的 Match >= index
		count := 1 // leader 自己
		for id, pr := range r.Prs {
			if id != r.id && pr.Match >= index {
				count++
			}
		}

		// 如果超过半数，更新 committed
		if count > len(r.Prs)/2 {
			r.RaftLog.committed = index
			break
		}
	}
}

// 处理集群收到的 MsgRequestVote 和 MsgRequestVoteResponse
func (r *Raft) handleRequestVote(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVote:
		// 处理投票请求
		Votemsg := pb.Message{From: r.id, To: m.From, Reject: false, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse}
		// 如果已经投票或者msg的term小，则拒绝投票
		if r.Vote != None || m.Term < r.Term {
			Votemsg.Reject = true
		}
		r.msgs = append(r.msgs, Votemsg)
	case pb.MessageType_MsgRequestVoteResponse:
		if !m.Reject {
			r.votes[m.From] = true
			if len(r.votes) > len(r.Prs)/2 {
				r.becomeLeader()
			}
		} else {
			if m.Term > r.Term {
				r.becomeFollower(m.Term, None)
			}
		}
	}
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

// 重置投票的状态
func (r *Raft) resetVote() {
	r.Vote = None
	for id := range r.votes {
		r.votes[id] = false
	}
}
