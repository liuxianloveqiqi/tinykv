# 架构图

![img](https://raw.githubusercontent.com/liuxianloveqiqi/Xian-imagehost/main/image/202408312005435.png)

# 底层存储引擎RockesDB

![LSMT（Log-Structured Merge-Tree） - zhengbiyu - 博客园](https://raw.githubusercontent.com/liuxianloveqiqi/Xian-imagehost/main/image/202408291817150.png)

# Raft

## Leader选举

![raft角色关系](https://raw.githubusercontent.com/liuxianloveqiqi/Xian-imagehost/main/image/202408291830705.png)

## log日志

![img](https://raw.githubusercontent.com/liuxianloveqiqi/Xian-imagehost/main/image/202408300950169.png)

## Read Index

![image-20240831173244481](https://raw.githubusercontent.com/liuxianloveqiqi/Xian-imagehost/main/image/202408311732564.png)

## peer

![img](https://raw.githubusercontent.com/liuxianloveqiqi/Xian-imagehost/main/image/202408300951956.png)

# multi Raft

单个 Raft-Group 在 KV 的场景下存在一些弊端:

(1) 系统的存储容量受制于单机的存储容量（使用分布式存储除外）。

(2) 系统的性能受制于单机的性能（读写请求都由Leader节点处理）。

MultiRaft 需要解决的一些核心问题：

(1) 数据何如分片。

(2) 分片中的数据越来越大，需要分裂产生更多的分片，组成更多 Raft-Group。

(3) 分片的调度，让负载在系统中更平均（分片副本的迁移，补全，Leader 切换等等）。

![image-20240830104751703](https://raw.githubusercontent.com/liuxianloveqiqi/Xian-imagehost/main/image/202408301047829.png)

# Percolator

![image-20240901102835317](https://raw.githubusercontent.com/liuxianloveqiqi/Xian-imagehost/main/image/202409011028446.png)
