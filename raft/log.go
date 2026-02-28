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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// 从 storage 获取初始状态
	hs, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}

	// 获取 storage 中的第一条日志索引，用于初始化 entries
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	l := &RaftLog{
		storage:   storage,
		committed: hs.Commit,
		applied:   firstIndex - 1,
		stabled:   firstIndex - 1,
		entries:   make([]pb.Entry, 0),
	}

	return l
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	ents := make([]pb.Entry, 0)

	// 如果有 unstable entries，它们可能会覆盖 storage 中的部分条目
	if len(l.entries) > 0 {
		firstUnstableIndex := l.entries[0].Index

		// 获取 storage 中在 unstable 之前的 entries
		firstIndex, err := l.storage.FirstIndex()
		if err != nil {
			return l.entries
		}
		if firstUnstableIndex > firstIndex {
			stableEnts, err := l.storage.Entries(firstIndex, firstUnstableIndex)
			if err == nil {
				ents = append(ents, stableEnts...)
			}
		}

		// 添加 unstable entries（覆盖 storage 中相同 index 的条目）
		ents = append(ents, l.entries...)
	} else {
		// 没有 unstable entries，直接获取 storage 中的所有条目
		firstIndex, err := l.storage.FirstIndex()
		if err != nil {
			return nil
		}
		lastIndex, err := l.storage.LastIndex()
		if err != nil {
			return nil
		}

		if lastIndex >= firstIndex {
			stableEnts, err := l.storage.Entries(firstIndex, lastIndex+1)
			if err == nil {
				ents = append(ents, stableEnts...)
			}
		}
	}

	return ents
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return make([]pb.Entry, 0)
	}
	var ents []pb.Entry
	for _, e := range l.entries {
		if e.Index > l.stabled {
			ents = append(ents, e)
		}
	}
	return ents
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if l.committed <= l.applied {
		return nil
	}

	lo := l.applied + 1
	hi := l.committed + 1

	if len(l.entries) == 0 {
		entries, err := l.storage.Entries(lo, hi)
		if err != nil {
			return nil
		}
		return entries
	}

	firstEntryIndex := l.entries[0].Index

	if lo < firstEntryIndex {
		storageEntries, err := l.storage.Entries(lo, firstEntryIndex)
		if err == nil {
			ents = append(ents, storageEntries...)
		}
		lo = firstEntryIndex
	}

	if lo >= hi {
		return ents
	}

	offset := lo - firstEntryIndex
	if int(offset) >= len(l.entries) {
		return ents
	}

	endOffset := hi - firstEntryIndex
	if int(endOffset) > len(l.entries) {
		endOffset = uint64(len(l.entries))
	}

	ents = append(ents, l.entries[offset:endOffset]...)
	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// 优先从 entries 中获取
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	// 从 storage 获取
	index, err := l.storage.LastIndex()
	if err != nil {
		return 0
	}
	return index
}

// LastTerm return the term of the last log entry
func (l *RaftLog) LastTerm() uint64 {
	lastIndex := l.LastIndex()
	if lastIndex == 0 {
		return 0
	}
	term, err := l.Term(lastIndex)
	if err != nil {
		return 0
	}
	return term
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// 检查 entries 中是否包含该索引
	if len(l.entries) > 0 {
		firstEntryIndex := l.entries[0].Index
		if i >= firstEntryIndex {
			offset := i - firstEntryIndex
			if int(offset) < len(l.entries) {
				return l.entries[offset].Term, nil
			}
		}
	}
	// 从 storage 获取
	return l.storage.Term(i)
}
