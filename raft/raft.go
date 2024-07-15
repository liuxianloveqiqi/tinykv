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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	// create raft
	hardState, confState, _ := c.Storage.InitialState()
	var raft = &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress), // follower's nextIndex
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
	}
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	if c.Applied > 0 {
		raft.RaftLog.applied = c.Applied
	}

	lastIndex := raft.RaftLog.LastIndex()
	for _, peer := range c.peers {
		raft.Prs[peer] = &Progress{Next: lastIndex + 1, Match: 0}
	}
	raft.becomeFollower(0, None)
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {

	// 计算上一条日志的索引
	prevIndex := r.Prs[to].Next - 1
	// 尝试获取上一条日志的任期
	prevLogTerm, err := r.RaftLog.Term(prevIndex)
	// 如果出现错误（例如日志不存在），尝试发送快照消息
	if err != nil {
		// 构造并发送快照消息
		err := r.Step(pb.Message{MsgType: pb.MessageType_MsgSnapshot})
		// 如果发送快照消息时出错，或者快照消息发送成功，都返回false，表示未能成功发送Append消息
		if err != nil {
			return false
		}
		return false
	}

	// 初始化要发送的日志条目切片
	var entries []*pb.Entry
	// 获取最后一条日志的索引加1，即下一条日志的预期索引
	n := r.RaftLog.LastIndex() + 1
	// 获取日志的第一条索引
	firstIndex := r.RaftLog.FirstIndex()
	// 从上一条日志的下一条开始，到最后一条日志，将日志条目添加到发送切片中
	for i := prevIndex + 1; i < n; i++ {
		entries = append(entries, &r.RaftLog.entries[i-firstIndex])
	}
	// 构造Append消息
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		LogTerm: prevLogTerm,
		Index:   prevIndex,
		Entries: entries,
	}
	r.msgs = append(r.msgs, msg)
	return true

}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if _, ok := r.Prs[to]; !ok {
		return
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		Entries: nil,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVote(to uint64) bool {
	// 获取最后一条日志的索引
	lastIndex := r.RaftLog.LastIndex()
	// 获取最后一条日志的任期
	lastLogTerm, err := r.RaftLog.Term(lastIndex)
	// 如果获取日志任期时出现错误（例如日志不存在），则尝试发送快照消息
	if err != nil {
		err := r.Step(pb.Message{MsgType: pb.MessageType_MsgSnapshot})
		// 如果发送快照消息时出错，返回false，表示请求投票失败
		if err != nil {
			return false
		}
		return false
	}

	// 构造请求投票的消息，注意这里entries可以不用
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote, // 消息类型：请求投票
		From:    r.id,                          // 发送者ID
		To:      to,                            // 接收者ID
		Term:    r.Term,                        // 当前任期
		Commit:  r.RaftLog.committed,           // 已提交的日志索引
		LogTerm: lastLogTerm,                   // 最后一条日志的任期
		Index:   lastIndex,                     // 最后一条日志的索引
		Entries: nil,                           // 请求投票消息不包含日志条目
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		fallthrough // 如果当前状态是follower，执行与candidate相同的逻辑
	case StateCandidate:
		r.electionElapsed++                         // 增加选举计时器
		if r.electionElapsed >= r.electionTimeout { // 如果达到选举超时
			r.electionElapsed = 0                                     // 重置选举计时器
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup}) // 触发新一轮选举
			if err != nil {                                           // 如果处理消息时出错
				return // 直接返回，不继续执行
			}
		}
	case StateLeader:
		r.heartbeatElapsed++                          // 增加心跳计时器
		if r.heartbeatElapsed >= r.heartbeatTimeout { // 如果达到心跳超时
			r.heartbeatElapsed = 0                                     // 重置心跳计时器
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat}) // 发送心跳消息
			if err != nil {                                            // 如果处理消息时出错
				return // 直接返回，不继续执行
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
	case StateCandidate:
	case StateLeader:
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

// softState 返回 Raft 实例的当前软状态。
// softState包括当前的领导者和 Raft 的状态（追随者，候选人，领导者）。
func (r *Raft) softState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}

// hardState 返回 Raft 实例的当前硬状态。
// hardState包括当前的任期，当前任期的投票，以及已知被提交的最高日志条目的索引。
func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}
