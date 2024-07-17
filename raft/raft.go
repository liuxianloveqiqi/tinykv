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
	"math/rand"
	"sort"
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
	// raft.becomeFollower(0, None)
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
		// 心跳还有作用就是用于告知节点哪些日志可以进行提交
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
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = None
	r.heartbeatElapsed = 0
	// 加上偏移随机时间，减少选举冲突和脑分裂
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Lead = None
	r.Term++
	// 成为candidate投自己
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.electionElapsed -= rand.Intn(r.electionTimeout)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: 成为leader后，应该在当前任期内提出一个空的entry
	r.State = StateLeader
	r.heartbeatElapsed = 0 // 重置心跳计时器
	r.electionElapsed = 0  // 重置选举计时器
	r.Lead = r.id          // 设置自己为领导者
	lastIndex := r.RaftLog.LastIndex()
	for peer := range r.Prs {
		r.Prs[peer].Next = lastIndex + 1
		r.Prs[peer].Match = 0 // 重置follower的匹配索引
	}

	// 向日志中添加一个空操作条目，表示领导者在当前任期内的操作
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1})
	// Tips!!! 必须先match=next，再next++
	r.Prs[r.id].Match = r.Prs[r.id].Next
	r.Prs[r.id].Next += 1

	for peer := range r.Prs {
		// tip:排除自己
		if peer != r.id {
			r.sendAppend(peer) // 向所有追随者发送追加日志请求
		}
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		err := r.FollowerStep(m)
		if err != nil {
			return err
		}
	case StateCandidate:
		err := r.CandidateStep(m)
		if err != nil {
			return err
		}
	case StateLeader:
		err := r.LeaderStep(m)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Raft) FollowerStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgBeat: //leader only
	case pb.MessageType_MsgPropose: // 转给leader处理
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse: //leader only
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse: //Candidate only
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse: //leader only
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

func (r *Raft) CandidateStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgBeat: //leader only
	case pb.MessageType_MsgPropose: //dropped
	case pb.MessageType_MsgAppend:
		// candidate如果收到term更大的message，则会转为follower
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse: //leader only
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.votes[m.From] = !m.Reject
		voteNum := len(r.votes)
		// 投票没有半数直接返回
		length := len(r.Prs) / 2
		if voteNum <= length {
			return nil
		}
		// 统计投票结果并且决定身份转化
		grant := 0
		denials := 0
		for _, status := range r.votes {
			if status {
				grant++
			} else {
				denials++
			}
		}
		if grant > length {
			r.becomeLeader()
		} else if denials > length {
			r.becomeFollower(r.Term, m.From)
		}
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse: //leader only
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

func (r *Raft) LeaderStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		for peer := range r.Prs {
			if peer != r.id {
				r.sendHeartbeat(peer)
			}
		}
	case pb.MessageType_MsgPropose:
		r.handleMsgPropose(m)
	case pb.MessageType_MsgAppend:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleMsgAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse: //Candidate only
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat: //error
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

func (r *Raft) handleRequestVote(m pb.Message) {
	// 如果请求的任期小于当前任期，拒绝投票
	if m.Term < r.Term {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, msg)
		return
	}
	// 如果不是follower并且请求的任期大于当前任期，变成follower
	if r.State != StateFollower && m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	// term小就要投票
	if r.Term < m.Term {
		r.Vote = None
		r.Term = m.Term
	}

	// 如果未投票或已投给请求者就能投，投给别人就不能投了，直接拒绝，重复投票只视为一次有效投
	if r.Vote == None || r.Vote == m.From {

		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)
		// 确保只有拥有最新日志的candidate才能获得投票
		if m.LogTerm > lastTerm ||
			(m.LogTerm == lastTerm && m.Index >= lastIndex) {
			r.Vote = m.From
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  false,
			}
			r.msgs = append(r.msgs, msg)
			return
		}
	}
	// 默认拒绝投票
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
	}
	r.msgs = append(r.msgs, msg)
}

// Handle the campaign to start a new election
// Once 'campaign' method is called, the node becomes candidate
// and sends `MessageType_MsgRequestVote` to peers in cluster to request votes.
func (r *Raft) campaign() {
	r.becomeCandidate()
	// 这里有卡一个节点的case，直接变leader
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
	for peer := range r.Prs {
		if peer != r.id {
			r.sendRequestVote(peer)
		}
	}
}

// 集群中的节点向leader转发用户提交的数据
func (r *Raft) handleMsgPropose(m pb.Message) {
	// 追加日志条目
	lastIndex := r.RaftLog.LastIndex()
	for i, entry := range m.Entries {
		entry.Term = r.Term                                   // 设置日志条目的任期为当前任期
		entry.Index = lastIndex + uint64(i) + 1               // 计算并设置日志条目的索引
		r.RaftLog.entries = append(r.RaftLog.entries, *entry) // 追加到日志中
	}
	// 更新当前节点的匹配和下一个日志索引
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1

	// 向所有其他节点广播追加日志请求
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
	// 如果集群只有一个节点，直接提交日志
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

func (r *Raft) sendAppendResponse(to uint64, reject bool, index uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Index:   index,
	}
	r.msgs = append(r.msgs, msg)
	return
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// 如果消息的任期小于当前任期，拒绝该消息并返回
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true, 0)
		return
	}
	// 如果消息的任期大于当前任期，更新当前任期
	if m.Term > r.Term {
		r.Term = m.Term
	}
	// 如果消息来源不是当前的领导者，更新领导者信息
	if m.From != r.Lead {
		r.Lead = m.From
	}
	// 重置选举计时器，随机减少一些时间，以避免同时到期
	r.electionElapsed -= rand.Intn(r.electionTimeout)
	// 检查日志一致性
	index := m.Index
	lastIndex := r.RaftLog.LastIndex()
	if m.Index <= lastIndex {
		LogTerm, _ := r.RaftLog.Term(m.Index)
		if m.LogTerm == LogTerm {
			// 如果存在冲突的条目（相同索引但任期不同），删除该条目及其之后的所有条目
			// 遍历接收到的日志条目
			for i, entry := range m.Entries {
				var Term uint64 // 用于存储日志条目的任期
				// 设置每个日志条目的索引，基于消息中的起始索引加上偏移量
				entry.Index = m.Index + uint64(i) + 1
				// 如果条目的索引小于或等于当前节点的最后一个日志的索引，则需要检查任期以确定是否存在冲突
				if entry.Index <= lastIndex {
					Term, _ = r.RaftLog.Term(entry.Index)
					// 如果任期不匹配，说明在当前索引处发生了冲突
					if Term != entry.Term {
						firstIndex := r.RaftLog.FirstIndex()
						// 删除冲突条目及其之后的所有条目
						r.RaftLog.entries = r.RaftLog.entries[0 : m.Index-firstIndex+1]
						// TODO 更新最后一个索引为当前日志的最后一个索引
						lastIndex = r.RaftLog.LastIndex()
						// 将新条目追加到日志中
						r.RaftLog.entries = append(r.RaftLog.entries, *entry)
					}
				} else {
					// 如果条目的索引大于当前节点的最后一个日志的索引，则直接追加到日志中
					r.RaftLog.entries = append(r.RaftLog.entries, *entry)
				}
			}
			// 更新stable索引，stable索引表示在当前任期内，最后一个与领导者一致的日志条目的索引
			for _, entry := range r.RaftLog.entries {
				if entry.Term <= r.Term {
					index = entry.Index
				}
			}

			r.RaftLog.stabled = index
			r.Vote = None // 重置投票状态，因为已经接收到了领导者的日志条目
			r.sendAppendResponse(m.From, false, r.RaftLog.LastIndex())
			// 如果领导者的commit索引大于当前节点的commit索引，则更新commit索引
			// 这里使用min函数是为了确保不会将commit索引设置为超出当前日志范围的值
			if m.Commit > r.RaftLog.committed {
				r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
			}
			return
		}
	}
	// return reject AppendResponse
	r.sendAppendResponse(m.From, true, 0)
	return
}

func (r *Raft) handleMsgAppendResponse(m pb.Message) {
	// 如果拒绝了附加日志请求
	if m.Reject == true {
		/*
			比如leader前面认为从日志索引为10的位置开始向节点A同步数据，但是节点A拒绝了这次数据同步同时返回RejectHint为2，
			说明节点A告知leader在它上面保存的最大日志索引ID为2，这样下一次leader就可以直接从索引为2的日志数据开始同步数据到节点A。
			而如果没有这个RejectHint成员，leader只能在每次被拒绝数据同步后都递减1进行下一次数据同步，显然这样是低效的。
		*/
		r.Prs[m.From].Next--
		r.sendAppend(m.From) // 重新发送附加日志请求
		return
	}
	// 每次leader节点在收到一条MsgAppResp类型消息，同时msg.Reject又是false的情况下，都需要去检查当前有哪些日志是超过半数的节点同意的
	// 更新follower的匹配和下一个日志索引
	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1
	// 计算是否可以提交日志
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, prs := range r.Prs {
		match[i] = prs.Match
		i++
	}
	// 用sort优化快速找到
	sort.Sort(match)
	// 找到中位数，即大多数节点已经commit的最小日志索引
	Match := match[(len(r.Prs)-1)/2]
	// println("match:", Match, "r.RaftLog.committed:", r.RaftLog.committed)
	if Match > r.RaftLog.committed {
		logTerm, _ := r.RaftLog.Term(Match)
		// 只有在同一任期才能commit
		if logTerm == r.Term {
			r.RaftLog.committed = Match
			// tips! leader 在 committed 发生变化的时候一定要去通知 follower，否则集群的 committed 会不同步
			for peer := range r.Prs {
				if peer != r.id {
					r.sendAppend(peer)
				}
			}
		}
	}
	return
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// 如果消息的任期小于当前任期，则拒绝心跳并回复
	if m.Term < r.Term {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, msg)
		return
	}
	// 如果消息的任期大于当前任期，更新当前任期
	if m.Term > r.Term {
		r.Term = m.Term
	}
	// 如果消息来源不是当前领导者，更新当前领导者
	if m.From != r.Lead {
		r.Lead = m.From
	}

	// Tip bug:TestCommitWithHeartbeat2AB，收到心跳后要同步committed
	r.RaftLog.committed = m.Commit
	// 重置选举计时器，随机减少一些时间，以避免同时到期
	r.electionElapsed -= rand.Intn(r.electionTimeout)
	// 发送心跳响应消息，表示接收成功
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  false,
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
