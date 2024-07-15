# Project2-RaftKV

## 参考

https://www.codedump.info/post/20180922-etcd-raft





在本节中，我们将实现一个基于 Raft 的高可用 KV 服务器，这不仅需要您实现Raft 算法（Project2A/C），还需要去实际使用它（Project2B）。整节分为三个小节，依次为：

- Implement the basic Raft algorithm；
- Build a fault-tolerant KV server on top of Raft；
- Add the support of raftlog GC and snapshot；

简单地来说，Project2A 用来实现基于 Raft 的同步过程，Project2B 用来实现 Raft 的上层调用与底层存储，Project2C 用来在前两者的基础上增加日志压缩和快照的功能。在进入代码实现之前，我们要先理解好 Raft 的思想。

## Raft 算法



Raft 是一个易于理解的一致性算法，其会保证结果的最终一致性。Raft 将一致性算法分解成了几个关键模块，如领导人选举、日志同步与安全性。

- **领导人选举**：当现存的领导人发生故障的时候，一个新的领导人需要被选举出来。
- **日志同步**：领导人必须从客户端接受日志条目，然后同步到集群中的其他节点，并强制要求其他节点的日志和自己保持一致，即强领导特性。
- **安全性**：如果有任何的服务器节点已经应用了一个确定的日志条目到它的状态机中，那么其他服务器节点不能在同一个日志索引位置应用一个不同的指令。

三大部分的细节在后续代码实现中再详细说明，这里要先理清 Raft 最为核心的运行逻辑，即 Leader、Follower、Candidate 三种角色的互相转换与 RPC 处理。在阐述三种角色关系之前，需要先清楚各节点上必须含有的字段。

首先是所有的节点上都应该含有的字段（不区分角色），其中包含了节点的基本信息、投票情况、日志记录、日志相关指针等等，分为持久性状态与易失性状态，如表所示：

| 字段        | 意义                                     | 状态   |
| ----------- | ---------------------------------------- | ------ |
| NodeID      | 节点 ID                                  | 持久性 |
| currentTerm | 节点当前的 Term                          | 持久性 |
| votedFor    | 当前 Term 内，节点把票投给了谁           | 持久性 |
| log[]       | 日志条目                                 | 持久性 |
| commitIndex | 已知已提交的最高的日志条目的索引         | 易失性 |
| lastApplied | 已经被应用到状态机的最高的日志条目的索引 | 易失性 |

其中，提交是指集群中 `半数以上` 的节点均同步了这条日志，才算作提交。

领导人唯一地从客户端接受指令，以日志条目的方式同步给集群中的其余节点，为了维持每个节点的同步信息，领导人需要额外两个字段去进行记录，如下：

| 字段         | 意义                                                         | 状态   |
| ------------ | ------------------------------------------------------------ | ------ |
| nextIndex[]  | 对于每个节点，待发送到该节点的下一个日志条目的索引，初值为领导人最后的日志条目索引 + 1 | 易失性 |
| matchIndex[] | 对于每个节点，已知的已经同步到该节点的最高日志条目的索引，初值为0，表示没有 | 易失性 |

Raft 通过基础的两个 RPC 来维持节点之间的通信，分别为日志追加 RPC（AppendEntries RPC）、请求投票 RPC（RequestVote RPC）。

- **AppendEntries RPC**：由领导人调用，发送给集群中的其他节点，用于日志条目的同步，同时也被当作心跳使用。（这里需要注意下，论文中 Heartbeat 和 AppendEntries RPC 是同一个 Msg，但在 TinyKV 中并不是，我们实现的过程中，Hearbeat 是一个独立的 Msg，和 AppendEntries RPC 不一样）
- **RequestVote RPC**：由候选人发送，用来征集选票。

两类 RPC 的收发与处理，每个角色均不一样。这里连同三种角色的互相转换、RPC 的处理、日志的应用等操作，整合为一张图，便于读者更清晰的理清它们之间的运行逻辑，如下：

[![raft角色关系](https://raw.githubusercontent.com/liuxianloveqiqi/Xian-imagehost/main/image/202407142215562.png)](https://github.com/sakura-ysy/TinyKV-2022-doc/blob/main/doc/project2/raft角色关系.png)

## project2A

本小节实验需要实现基础的 Raft 算法，且不需要考虑快照操作。我们共需要实现三个模块，分别为 RawNode、Raft 和 RaftLog，分别对应文件 `rawnode.go`、`raft.go` 和 `log.go` ，这三个模块，共同构成一层，我将其称为 `raft 层`。在具体实现之前，要先弄清三者的关系。

- **RawNode**：该模块用来接收上层传来的信息，将信息下传给 Raft 模块。比如，上层传递一个 Msg 给 RawNode，这个 Msg 可能是 心跳请求、日志提交请求、日志追加请求等等。然后 RawNode 收到这个 Msg 之后，将其交给 Raft 模块去处理。比如，上层交给 RawNode 一个日志提交请求，然后 RawNode 将其交给 Raft ，如果该节点是领导人，那么其就会追加这条日志，然后发送给其他节点进行同步。另外，RawNode 不仅用来接受请求然后传递给 Raft，还要用来收到 Raft 的同步结果然后传递给上层。RawNode 要负责检查 Raft 模块是否已有同步好的日志需要应用、是否有 Msg 需要发送、是否有日志需要持久化等等，然后把这些信息（Ready）交给上层，上层会据此进行处理。总的来说，RawNode 是 raft 层暴露给上层的一个模块，用来进行信息的传递。
- **Raft**：整个 raft 层最关键的就是它，它就是实现 Raft 算法的核心模块。其中，领导人选举、日志追加、角色转换等等均是在这里实现。Raft 模块会接受 RawNode 发来的信息，然后在进群中进行相关的同步操作，同步结果不需要主动返回给 RawNode，因为 RawNode 自己会进行检查。
- **RaftLog**：该模块用来暂存节点的日志条目，同时还要维护一些日志指针，如 committed、applied 等等。

总结一下，RawNode 是 raft 层中暴露在外面的模块，用于该层与上层的信息交互，Raft 模块是 raft 层中的核心模块，算法的核心逻辑均在该模块实现，RaftLog 用来暂存日志信息，并维护相关指针。三者的关系用图表示如下：

[![图片1](https://raw.githubusercontent.com/liuxianloveqiqi/Xian-imagehost/main/image/202407142220131.png)](https://github.com/sakura-ysy/TinyKV-2022-doc/blob/main/doc/project2/图片1.png)

其中，Msg 不单指上层传来的请求，也有可能是其他节点发来的 Msg，比如两个 RPC，但这些 Msg 都是通过上层发送与接收的，然后 RawNode 将其传给 Raft 去处理。

### project2aa

![img](https://raw.githubusercontent.com/liuxianloveqiqi/Xian-imagehost/main/image/202407142230000.png)