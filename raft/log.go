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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"log"
)

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
	firstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panic("storage must not be nil")
	}

	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}
	raftLog := &RaftLog{
		storage:   storage,
		committed: hardState.Commit,
		/* exists bug but all program depend on it
		func (ms *MemoryStorage) firstIndex() uint64 {
			return ms.ents[0].Index + 1
		}*/
		applied:         firstIndex - 1,
		stabled:         lastIndex,
		entries:         entries,
		pendingSnapshot: nil,
		firstIndex:      firstIndex,
	}
	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	// 在某些情况下，存储层（如 storage）会进行日志压缩操作，
	// 将旧的日志条目从存储中删除，并更新 truncatedIndex。但是，内存中的 entries 列表可能还没有同步更新
	truncatedIndex, _ := l.storage.FirstIndex()
	if len(l.entries) > 0 {
		firstIndex := l.entries[0].Index
		if truncatedIndex > firstIndex {
			l.entries = l.entries[truncatedIndex-firstIndex:]
		}
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	// 日志索引一般不是从0开始的，所以这里要用日志索引去映射切片索引
	if len(l.entries) > 0 {
		if (l.stabled-l.FirstIndex()+1 < 0) ||
			(l.stabled-l.FirstIndex()+1 > uint64(len(l.entries))) {
			return nil
		}
		return l.entries[l.stabled-l.FirstIndex()+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.applied-l.FirstIndex()+1 : l.committed-l.FirstIndex()+1]
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		//i, _ := l.storage.LastIndex()
		//return i
		return l.stabled
	}

	//return l.entries[0].Index + uint64(len(l.entries)) - 1
	return l.entries[len(l.entries)-1].Index
}

func (l *RaftLog) FirstIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		i, _ := l.storage.FirstIndex()
		return i - 1
	}
	return l.entries[0].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		offset := l.FirstIndex()
		if i >= offset {
			index := i - l.FirstIndex()
			if index >= uint64(len(l.entries)) {
				return 0, ErrUnavailable
			}
			return l.entries[index].Term, nil
		}
	}
	// 如果entries为空，或者i>firstIndex，代表要从已经持久化的storage里面拿term
	term, err := l.storage.Term(i)
	// 检查请求的索引是否在快照范围内
	if errors.Is(err, ErrUnavailable) && !IsEmptySnap(l.pendingSnapshot) {
		if i < l.pendingSnapshot.Metadata.Index {
			// 如果请求的索引小于快照索引，返回错误
			err = ErrCompacted
		}
	}
	return term, err
}
