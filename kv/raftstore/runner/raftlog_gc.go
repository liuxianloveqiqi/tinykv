package runner

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
)

type RaftLogGCTask struct {
	RaftEngine *badger.DB
	RegionID   uint64
	StartIdx   uint64
	EndIdx     uint64
}

type raftLogGcTaskRes uint64

type raftLogGCTaskHandler struct {
	taskResCh chan<- raftLogGcTaskRes
}

func NewRaftLogGCTaskHandler() *raftLogGCTaskHandler {
	return &raftLogGCTaskHandler{}
}

// gcRaftLog does the GC job and returns the count of logs collected.
func (r *raftLogGCTaskHandler) gcRaftLog(raftDb *badger.DB, regionId, startIdx, endIdx uint64) (uint64, error) {
	// 确定需要进行垃圾回收的日志索引范围。
	firstIdx := startIdx
	if firstIdx == 0 {
		// 如果没有指定起始索引，则尝试从数据库中查找实际的起始索引。
		firstIdx = endIdx
		err := raftDb.View(func(txn *badger.Txn) error {
			startKey := meta.RaftLogKey(regionId, 0)
			ite := txn.NewIterator(badger.DefaultIteratorOptions)
			defer ite.Close()
			if ite.Seek(startKey); ite.Valid() {
				var err error
				if firstIdx, err = meta.RaftLogIndex(ite.Item().Key()); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
	}

	if firstIdx >= endIdx {
		// 如果计算得到的起始索引大于等于结束索引，表示没有需要进行垃圾回收的日志。
		log.Infof("no need to gc, [regionId: %d]", regionId)
		return 0, nil
	}

	// 使用 WriteBatch 创建一个批量删除操作。
	raftWb := engine_util.WriteBatch{}
	for idx := firstIdx; idx < endIdx; idx += 1 {
		// 为每个日志索引生成对应的日志键，并将其添加到批量删除操作中。
		key := meta.RaftLogKey(regionId, idx)
		raftWb.DeleteMeta(key)
	}
	if raftWb.Len() != 0 {
		// 如果批量操作中有待删除的键，则将这些删除操作写入数据库。
		if err := raftWb.WriteToDB(raftDb); err != nil {
			return 0, err
		}
	}
	// 返回被清理的日志数量。
	return endIdx - firstIdx, nil

}

func (r *raftLogGCTaskHandler) reportCollected(collected uint64) {
	if r.taskResCh == nil {
		return
	}
	r.taskResCh <- raftLogGcTaskRes(collected)
}

func (r *raftLogGCTaskHandler) Handle(t worker.Task) {
	logGcTask, ok := t.(*RaftLogGCTask)
	if !ok {
		log.Errorf("unsupported worker.Task: %+v", t)
		return
	}
	log.Debugf("execute gc log. [regionId: %d, endIndex: %d]", logGcTask.RegionID, logGcTask.EndIdx)
	collected, err := r.gcRaftLog(logGcTask.RaftEngine, logGcTask.RegionID, logGcTask.StartIdx, logGcTask.EndIdx)
	if err != nil {
		log.Errorf("failed to gc. [regionId: %d, collected: %d, err: %v]", logGcTask.RegionID, collected, err)
	} else {
		log.Debugf("collected log entries. [regionId: %d, entryCount: %d]", logGcTask.RegionID, collected)
	}
	r.reportCollected(collected)
}
