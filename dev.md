# TinyKV Project 2A 开发日志

## Project 2A 测试目标

| 命令 | 测试目标 | 功能描述 |
|------|----------|----------|
| **make project2a** | `./raft -run 2A` | 运行所有 2A 系列测试（包括 2AA、2AB、2AC） |
| **make project2aa** | `./raft -run 2AA` | **Leader 选举**：测试 term 更新、选举超时、心跳广播、Follower 转 Candidate、投票请求等 |
| **make project2ab** | `./raft -run 2AB` | **日志复制**：测试 Leader 复制日志、提交条目、Follower 追加日志、日志同步、投票等 |
| **make project2ac** | `./raft -run 2AC` | **RawNode 启动/重启**：测试 RawNode 初始化、提案提交、节点重启后状态恢复 |

## Step 函数的作用

`Step` 是 **处理所有传入消息的统一入口**，它根据消息类型和节点状态路由到不同的处理逻辑。

### 消息流程

```
                    ┌─────────────────────────────────────┐
                    │           消息来源                   │
                    └─────────────────────────────────────┘
                                     │
        ┌────────────────────────────┼────────────────────────────┐
        ▼                            ▼                            ▼
  本地消息                       外部消息                      用户请求
(MsgHup/MsgBeat)          (MsgAppend/MsgVote等)            (Propose)
        │                            │                            │
        └────────────────────────────┼────────────────────────────┘
                                     ▼
                              ┌──────────────┐
                              │   Step(m)    │  ← 统一入口
                              └──────────────┘
                                     │
                    ┌────────────────┼────────────────┐
                    ▼                ▼                ▼
             handleHeartbeat  handleAppendEntries  handleVote  ...
```

### 关键点

1. **所有消息都通过 Step 处理**：无论是本地消息（选举超时、心跳触发）还是外部消息（RPC 请求/响应）

2. **发送消息 vs 处理消息**：
   - **发送**：直接追加到 `r.msgs` 队列（如 `sendHeartbeat`）
   - **接收/处理**：必须通过 `Step` 函数

3. **消息分类**：
   - **本地消息**（`MsgHup`, `MsgBeat`, `MsgPropose`）：节点内部触发
   - **RPC 消息**（`MsgAppend`, `MsgRequestVote`, `MsgHeartbeat` 等）：节点间通信
   - **响应消息**（`*Response` 类型）：对 RPC 的回复

4. **Ready 机制**：外部通过 `Ready()` 取出 `r.msgs` 中的消息发送给其他节点
