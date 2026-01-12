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

/*
Package raft 发送和接收 eraftpb 包中定义的 Protocol Buffer 格式的消息。

Raft 是一种协议，节点集群可以使用它来维护复制状态机。
状态机通过使用复制日志保持同步。
有关 Raft 的更多详细信息，请参阅 Diego Ongaro 和 John Ousterhout 撰写的“In Search of an Understandable Consensus Algorithm”
(https://ramcloud.stanford.edu/raft.pdf)。

Usage

raft 中的主要对象是 Node。你可以使用 raft.StartNode 从头开始启动 Node，
或者使用 raft.RestartNode 从某些初始状态启动 Node。

从头开始启动节点：

  storage := raft.NewMemoryStorage()
  c := &Config{
    ID:              0x01,
    ElectionTick:    10,
    HeartbeatTick:   1,
    Storage:         storage,
  }
  n := raft.StartNode(c, []raft.Peer{{ID: 0x02}, {ID: 0x03}})

从之前的状态重启节点：

  storage := raft.NewMemoryStorage()

  // recover the in-memory storage from persistent
  // snapshot, state and entries.
  storage.ApplySnapshot(snapshot)
  storage.SetHardState(state)
  storage.Append(entries)

  c := &Config{
    ID:              0x01,
    ElectionTick:    10,
    HeartbeatTick:   1,
    Storage:         storage,
    MaxInflightMsgs: 256,
  }

  // restart raft without peer information.
  // peer information is already included in the storage.
  n := raft.RestartNode(c)

既然你持有了一个 Node，你就有了几个职责：

首先，你必须从 Node.Ready() 通道读取并处理它包含的更新。
除了步骤 2 中提到的情况外，这些步骤可以并行执行。

1. 如果 HardState、Entries 和 Snapshot 不为空，则将它们写入持久存储。
注意，当写入索引为 i 的 Entry 时，任何先前持久化的索引 >= i 的条目都必须被丢弃。

2. 将所有 Messages 发送到 To 字段中命名的节点。重要的是，
在最新的 HardState 持久化到磁盘并且之前任何 Ready 批次写入的所有 Entries 都已写入之前，
不发送任何消息（消息可以在同一批次的条目正在持久化时发送）。

注意：编组消息不是线程安全的；重要的是要确保在编组时没有新的条目被持久化。
实现此目的的最简单方法是直接在主 raft 循环中序列化消息。

3. 将 Snapshot（如果有）和 CommittedEntries 应用到状态机。
如果任何已提交的 Entry 具有类型 EntryType_EntryConfChange，请调用 Node.ApplyConfChange()
将其应用到节点。此时可以通过在调用 ApplyConfChange 之前将 NodeId 字段设置为零来取消配置更改
（但必须以某种方式调用 ApplyConfChange，取消的决定必须完全基于状态机，而不是基于外部信息，例如观察到的节点健康状况）。

4. 调用 Node.Advance() 以发出准备好接收下一批更新的信号。
这可以在步骤 1 之后的任何时间完成，尽管所有更新必须按照 Ready 返回的顺序进行处理。

其次，所有持久化的日志条目必须通过 Storage 接口的实现可用。
提供的 MemoryStorage 类型可用于此目的（如果你在重启时重新填充其状态），
或者你可以提供自己的基于磁盘的实现。

Third, when you receive a message from another node, pass it to Node.Step:

	func recvRaftRPC(ctx context.Context, m eraftpb.Message) {
		n.Step(ctx, m)
	}

最后，你需要定期调用 Node.Tick()（可能通过 time.Ticker）。
Raft 有两个重要的超时：心跳和选举超时。但是，在 raft 包内部，时间由抽象的“tick”表示。

The total state machine handling loop will look something like this:

  for {
    select {
    case <-s.Ticker:
      n.Tick()
    case rd := <-s.Node.Ready():
      saveToStorage(rd.State, rd.Entries, rd.Snapshot)
      send(rd.Messages)
      if !raft.IsEmptySnap(rd.Snapshot) {
        processSnapshot(rd.Snapshot)
      }
      for _, entry := range rd.CommittedEntries {
        process(entry)
        if entry.Type == eraftpb.EntryType_EntryConfChange {
          var cc eraftpb.ConfChange
          cc.Unmarshal(entry.Data)
          s.Node.ApplyConfChange(cc)
        }
      }
      s.Node.Advance()
    case <-s.done:
      return
    }
  }

要从你的节点提议对状态机的更改，请获取你的应用程序数据，将其序列化为字节切片并调用：

	n.Propose(data)

如果提议被提交，数据将出现在类型为 eraftpb.EntryType_EntryNormal 的已提交条目中。
不能保证提议的命令会被提交；你可能需要在超时后重新提议。

To add or remove a node in a cluster, build ConfChange struct 'cc' and call:

	n.ProposeConfChange(cc)

配置更改提交后，将返回一些类型为 eraftpb.EntryType_EntryConfChange 的已提交条目。
你必须通过以下方式将其应用到节点：

	var cc eraftpb.ConfChange
	cc.Unmarshal(data)
	n.ApplyConfChange(cc)

注意：ID 代表集群中一直存在的唯一节点。
即使旧节点已被删除，给定的 ID 也必须仅使用一次。
这意味着，例如 IP 地址不适合作为节点 ID，因为它们可能会被重复使用。节点 ID 必须非零。

Implementation notes

此实现与最终的 Raft 论文 (https://ramcloud.stanford.edu/~ongaro/thesis.pdf) 保持最新，
尽管我们对成员变更协议的实现与第 4 章中描述的有所不同。
保留了成员变更一次发生一个节点的关键不变性，但在我们的实现中，
成员变更在其条目被应用时生效，而不是在其添加到日志时生效（因此条目是在旧成员资格下提交的，而不是新成员资格）。
这在安全性方面是等效的，因为旧配置和新配置保证重叠。

为了确保我们不会尝试通过匹配日志位置同时提交两个成员变更（这既不安全，因为它们应该有不同的法定人数要求），
我们简单地禁止任何提议的成员变更，只要领导者的日志中出现任何未提交的变更。

这种方法在你尝试从双成员集群中删除成员时会引入一个问题：
如果其中一个成员在另一个成员收到 confchange 条目的提交之前死亡，
则无法再删除该成员，因为集群无法取得进展。
因此，强烈建议在每个集群中使用三个或更多节点。

MessageType

Package raft 以 Protocol Buffer 格式（在 eraftpb 包中定义）发送和接收消息。
每个状态（follower, candidate, leader）在推进给定的 eraftpb.Message 时实现其自己的 'step' 方法
（'stepFollower', 'stepCandidate', 'stepLeader'）。每个步骤由其 eraftpb.MessageType 决定。
注意，每个步骤都由一个名为 'Step' 的通用方法检查，该方法会对节点和传入消息的 term 进行安全检查，以防止过时的日志条目：

	'MessageType_MsgHup' 用于选举。如果节点是 follower 或 candidate，
	则 'raft' 结构中的 'tick' 函数设置为 'tickElection'。如果 follower 或 candidate
	在选举超时之前未收到任何心跳，它会将 'MessageType_MsgHup' 传递给其 Step 方法，
	并成为（或保持）candidate 以开始新的选举。

	'MessageType_MsgBeat' 是一种内部类型，用于向 leader 发出发送 'MessageType_MsgHeartbeat' 类型心跳的信号。
	如果节点是 leader，则 'raft' 结构中的 'tick' 函数设置为 'tickHeartbeat'，
	并触发 leader 定期向其 follower 发送 'MessageType_MsgHeartbeat' 消息。

	'MessageType_MsgPropose' 提议将其日志条目追加到数据中。这是一种特殊类型，
	用于将提议重定向到 leader。因此，send 方法用其 HardState 的 term 覆盖 eraftpb.Message 的 term，
	以避免将其本地 term 附加到 'MessageType_MsgPropose'。当 'MessageType_MsgPropose' 传递给 leader 的 'Step' 方法时，
	leader 首先调用 'appendEntry' 方法将其日志条目追加到其日志中，
	然后调用 'bcastAppend' 方法将这些条目发送给其 peer。当传递给 candidate 时，
	'MessageType_MsgPropose' 被丢弃。当传递给 follower 时，'MessageType_MsgPropose' 通过 send 方法存储在 follower 的邮箱 (msgs) 中。
	它与发送者的 ID 一起存储，稍后由 rafthttp 包转发给 leader。

	'MessageType_MsgAppend' 包含要复制的日志条目。Leader 调用 bcastAppend，
	后者调用 sendAppend，发送即将复制的 'MessageType_MsgAppend' 类型的日志。
	当 'MessageType_MsgAppend' 传递给 candidate 的 Step 方法时，candidate 恢复为 follower，
	因为这表明有一个有效的 leader 正在发送 'MessageType_MsgAppend' 消息。
	Candidate 和 follower 以 'MessageType_MsgAppendResponse' 类型响应此消息。

	'MessageType_MsgAppendResponse' 是对日志复制请求 ('MessageType_MsgAppend') 的响应。
	当 'MessageType_MsgAppend' 传递给 candidate 或 follower 的 Step 方法时，
	它通过调用 'handleAppendEntries' 方法进行响应，该方法向 raft 邮箱发送 'MessageType_MsgAppendResponse'。

	'MessageType_MsgRequestVote' 请求投票选举。当节点是 follower 或 candidate 并且 'MessageType_MsgHup' 传递给其 Step 方法时，
	节点调用 'campaign' 方法来竞选成为 leader。一旦调用 'campaign' 方法，
	节点变为 candidate 并向集群中的 peer 发送 'MessageType_MsgRequestVote' 以请求投票。
	当传递给 leader 或 candidate 的 Step 方法且消息的 Term 低于 leader 或 candidate 的 Term 时，
	'MessageType_MsgRequestVote' 将被拒绝（返回 Reject 为 true 的 'MessageType_MsgRequestVoteResponse'）。
	如果 leader 或 candidate 收到具有更高 term 的 'MessageType_MsgRequestVote'，它将恢复为 follower。
	当 'MessageType_MsgRequestVote' 传递给 follower 时，只有当发送者的最后 term 大于 MessageType_MsgRequestVote 的 term
	或者发送者的最后 term 等于 MessageType_MsgRequestVote 的 term 但发送者的最后提交索引大于或等于 follower 的索引时，
	follower 才投票给发送者。

	'MessageType_MsgRequestVoteResponse' 包含来自投票请求的响应。当 'MessageType_MsgRequestVoteResponse' 传递给 candidate 时，
	candidate 计算它赢得了多少选票。如果超过多数（法定人数），它变为 leader 并调用 'bcastAppend'。
	如果 candidate 收到多数否决票，它恢复为 follower。

	'MessageType_MsgSnapshot' 请求安装快照消息。当节点刚刚成为 leader 或 leader 收到 'MessageType_MsgPropose' 消息时，
	它调用 'bcastAppend' 方法，然后该方法对每个 follower 调用 'sendAppend' 方法。
	在 'sendAppend' 中，如果 leader 无法获取 term 或 entries，
	leader 通过发送 'MessageType_MsgSnapshot' 类型消息来请求快照。

	'MessageType_MsgHeartbeat' 发送来自 leader 的心跳。当 'MessageType_MsgHeartbeat' 传递给 candidate
	且消息的 term 高于 candidate 的 term 时，candidate 恢复为 follower 并根据此心跳中的索引更新其提交索引。
	并将其发送到其邮箱。当 'MessageType_MsgHeartbeat' 传递给 follower 的 Step 方法且消息的 term 高于 follower 的 term 时，
	follower 使用消息中的 ID 更新其 leaderID。

	'MessageType_MsgHeartbeatResponse' 是对 'MessageType_MsgHeartbeat' 的响应。
	当 'MessageType_MsgHeartbeatResponse' 传递给 leader 的 Step 方法时，leader 知道哪个 follower 进行了响应。

*/
package raft
