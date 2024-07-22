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

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft
	// Your Data Here (2A).
	prevSoftState *SoftState
	prevHardState pb.HardState
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	raft := newRaft(config)
	rn := &RawNode{
		Raft:          raft,
		prevSoftState: raft.softState(),
		prevHardState: raft.hardState(),
	}
	return rn, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	ready := Ready{
		SoftState:        nil,
		HardState:        pb.HardState{},
		Entries:          rn.Raft.RaftLog.unstableEntries(),
		Snapshot:         pb.Snapshot{},
		CommittedEntries: rn.Raft.RaftLog.nextEnts(),
		Messages:         rn.Raft.msgs,
	}

	curSoftState := rn.Raft.softState()
	// 如果当前软状态与之前保存的软状态不同，则更新 Ready 中的软状态，并更新之前保存的软状态
	if !(curSoftState.Lead == rn.prevSoftState.Lead &&
		curSoftState.RaftState == rn.prevSoftState.RaftState) {
		ready.SoftState = curSoftState
		rn.prevSoftState = curSoftState
	}

	curHardState := rn.Raft.hardState()
	// 如果当前硬状态与之前保存的硬状态不相等，则更新 Ready 中的硬状态
	// tip! 注释掉了更新之前保存的硬状态的代码，可能是因为在实际的 Ready 方法调用后，外部逻辑会处理这个更新
	if !isHardStateEqual(curHardState, rn.prevHardState) {
		ready.HardState = curHardState // 更新 Ready 中的硬状态
		// rn.prevHardState = curHardState
	}

	// 清空 Raft 中待发送的msg列表
	rn.Raft.msgs = nil

	return ready // 返回填充好的 Ready 结构体
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	// When Something needs to store return true else return false
	// hardState changes return true
	// 检查软状态是否有变化，如果有变化则返回 true
	curSoftState := rn.Raft.softState()
	if !isSoftStateEqual(curSoftState, rn.prevSoftState) {
		return true
	}

	hardState := rn.Raft.hardState()
	preHardState := rn.prevHardState
	// 当前hardState不为空并且与之前的hardState不相等，则说明hardState发生了变化，需要持久化
	if !IsEmptyHardState(hardState) &&
		!isHardStateEqual(hardState, preHardState) {
		return true
	}
	// 如果存在unstable entries、待发送的msg或者已经commited but no applied的日志条目，则返回 true
	// unstable Entries or messages exists return true
	if len(rn.Raft.RaftLog.unstableEntries()) > 0 ||
		len(rn.Raft.msgs) > 0 || len(rn.Raft.RaftLog.nextEnts()) > 0 {
		return true
	}
	return false

}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	// Your Code Here (2A).
	// update hardState
	if !IsEmptyHardState(rd.HardState) {
		rn.prevHardState = rd.HardState
	}
	if len(rd.Entries) > 0 {
		// 更新持久化日志，将unstable的日志stable
		// rd.Entries是unstableEntries
		rn.Raft.RaftLog.stabled = rd.Entries[len(rd.Entries)-1].Index
	}
	if len(rd.CommittedEntries) > 0 {
		rn.Raft.RaftLog.applied = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
	}
	// 日志可能的压缩
	rn.Raft.RaftLog.maybeCompact()
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
