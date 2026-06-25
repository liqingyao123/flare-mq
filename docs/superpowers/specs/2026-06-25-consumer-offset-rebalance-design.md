# Consumer Offset 持久化与队列分配设计

**日期：** 2026-06-25
**状态：** 待实现

---

## 1. 问题

1. Consumer offset 仅存本地内存 `consumeProgress`，重启后从 0 开始，导致消息重复消费
2. Consumer 硬编码 `queueId=0`，所有 Consumer 拉同一队列，无法并行消费
3. 无 Consumer 组协调机制，多个 Consumer 无法合理分配队列

## 2. 目标

- Offset 持久化到 Broker，Consumer 重启后从断点继续消费
- Consumer Group 内队列分配，每个 Consumer 负责一组 Queue
- Consumer 加入/离开时自动 Rebalance

## 3. 整体架构

```
Consumer Group: "order-group"
  ├─ Consumer-A (注册到 NameServer)
  ├─ Consumer-B (注册到 NameServer)
  └─ Consumer-C (注册到 NameServer)

NameServer:
  ├─ consumerGroupTable: 管理 consumerGroup → consumerId 列表
  └─ 心跳超时清理死 Consumer

Broker:
  ├─ ConsumerOffsetManager:
  │   ConcurrentHashMap<(group,topic,queueId), offset>
  │   每 5s flush → consumerOffset.json
  └─ handleUpdateConsumerOffset / handleQueryConsumerOffset

Consumer:
  ├─ QueueAllocationManager: 注册、心跳、rebalance 计算
  └─ ConsumerImpl: 多 queue 并发拉取 + offset 上报/恢复
```

## 4. 新增协议

```
CONSUMER_REGISTER_REQUEST      = 50
CONSUMER_REGISTER_RESPONSE     = 51
CONSUMER_HEARTBEAT_REQUEST     = 52
CONSUMER_HEARTBEAT_RESPONSE    = 53
UPDATE_CONSUMER_OFFSET_REQUEST = 44
UPDATE_CONSUMER_OFFSET_RESPONSE= 45
QUERY_CONSUMER_OFFSET_REQUEST  = 46
QUERY_CONSUMER_OFFSET_RESPONSE = 47
```

## 5. Broker 端

### 5.1 ConsumerOffsetManager

**新文件：** `ruyuan-mq-broker/src/main/java/.../offset/ConsumerOffsetManager.java`

```
updateOffset(group, topic, queueId, offset):
  key = "{group}@{topic}@{queueId}"
  map.merge(key, offset, Math::max)   ← 只向前推进

getOffset(group, topic, queueId):
  return map.getOrDefault(key, 0L)

persistOffsets():
  ScheduledExecutorService 每 5s
  序列化 map → consumerOffset.json

loadOffsets():
  Broker 启动时从 consumerOffset.json 反序列化
  文件不存在或损坏 → 从空开始
```

### 5.2 BrokerRequestHandler 新增

```
handleUpdateConsumerOffset(request):
  解析 { consumerGroup, topic, queueId, offset }
  → consumerOffsetManager.updateOffset(...)
  → return SUCCESS

handleQueryConsumerOffset(request):
  解析 { consumerGroup, topic, queueId }
  offset = consumerOffsetManager.getOffset(...)
  return { offset: xxx }
```

### 5.3 ClusterManager 集成

ConsumerOffsetManager 在 Broker 启动时创建，传入持久化文件路径。定时 flush 在后台运行。

## 6. NameServer 端

### 6.1 Consumer 注册管理

在 `ServiceRegistry` 中新增：

```
consumerGroupTable: ConcurrentHashMap<consumerGroup, ConcurrentHashMap<consumerId, ConsumerHeartbeatData>>

ConsumerHeartbeatData:
  consumerId: String
  consumerGroup: String
  lastHeartbeatTime: long
  topics: List<String>

registerConsumer(consumerGroup, consumerId, topics):
  → 更新 consumerGroupTable
  → 返回同组所有 consumerId 列表（排序后）

heartbeat(consumerGroup, consumerId):
  → 更新 lastHeartbeatTime

unregisterConsumer(consumerGroup, consumerId):
  → 从表中移除
```

### 6.2 心跳清理

HealthChecker 现有定时任务中新增：遍历 consumerGroupTable，清理超过 60s 未心跳的 consumer。发现变更后通知同组其他 consumer。

### 6.3 NameServerRequestHandler 新增

```
handleConsumerRegister(request):
  解析 { consumerGroup, consumerId, topics }
  → consumerList = serviceRegistry.registerConsumer(...)
  → return { consumerIdList: [...] }

handleConsumerHeartbeat(request):
  解析 { consumerGroup, consumerId }
  → serviceRegistry.heartbeat(...)
  → return SUCCESS
```

## 7. Consumer 端

### 7.1 QueueAllocationManager

**新文件：** `ruyuan-mq-client/src/main/java/.../consumer/QueueAllocationManager.java`

职责：注册到 NameServer、心跳、Rebalance 计算（含状态机合并）

#### Rebalance 状态机

```
     ┌──────────────┐
     │    IDLE      │ ← 正常消费中
     └──────┬───────┘
            │ 检测到 group 成员变化
            ▼
     ┌──────────────────┐
     │  REBALANCE_WAIT  │ 启动随机 timer (3~10s)
     │                  │
     │  期间又有新变化？  │──→ 重置 timer，继续等
     │                  │
     │  timer 到期      │──→ 进入下一阶段
     └──────┬───────────┘
            ▼
     ┌──────────────────────────┐
     │  REBALANCE_IN_PROGRESS   │ 拉取最新 consumer 列表
     │                          │ 计算分配
     │                          │ 停止旧 queue，启动新 queue
     └──────────┬───────────────┘
                │ 完成
                ▼
           ┌──────────────┐
           │    IDLE      │
           └──────────────┘
```

Coalesce 效果：100 个 consumer 同时启动 → 每人只在变化停止后执行 1~2 次 Rebalance（vs 无状态机时每人 ~99 次，总共 ~10,000 次）。

#### 核心方法

```
initialize(nameServerAddr, consumerGroup, consumerId, topics):
  → 连接 NameServer
  → 随机 sleep 0~3s（错开首次注册）
  → 注册 consumerId，获取同组 consumer 列表
  → 计算初始队列分配
  → 启动心跳定时任务（30s 间隔）
  → 启动 rebalance 检查定时任务（30s 间隔）

checkRebalance():
  → 拉取最新同组 consumer 列表
  → 与本地缓存对比
  → 无变化 → 跳过
  → 有变化 → 触发状态机（IDLE → REBALANCE_WAIT）
    如果已在 WAIT → 重置 timer（coalesce）

executeRebalance():
  → REBALANCE_IN_PROGRESS
  → 拉取最新 consumer 列表
  → calculateAllocation(topic, queueCount, sortedConsumerIds)
  → 对比新旧分配，得出需要释放和新增的 queue
  → 调用 ConsumerImpl.onRebalance(oldQueues, newQueues)
  → 回到 IDLE

calculateAllocation(topic, queueCount, consumers):
  // 确定性平均分配算法
  consumerList = sort(consumers)
  index = consumerList.indexOf(myId)
  queuesPerConsumer = queueCount / consumerCount
  remainder = queueCount % consumerCount
  // 前 remainder 个 consumer 多拿一个 queue
  return 分配给当前 consumer 的 queueId 列表（不变动的不触达变化）
```

### 7.2 ConsumerImpl 改造

**修改文件：** `ConsumerImpl.java`

```
start():
  1. 连接 NameServer
  2. 创建 QueueAllocationManager，注册 + 计算初始队列分配
  3. 查询 Broker 恢复各 queue 的 offset
  4. 对每个分配的 queue 启动 pull 任务
  5. 启动 offset 上报定时任务（5s 间隔）
  6. 启动 rebalance 检查定时任务

pullMessageForQueue(topic, queueId):
  // 替代原来的 pullMessageForTopic，不再硬编码 queueId=0
  progressKey = topic_queueId
  offset = consumeProgress.getOrDefault(progressKey, 0L)
  → pullMessage(topic, queueId, offset, batchSize)
  → 更新进度 → 触发 offset 上报

onRebalance(oldQueues, newQueues):
  1. 停止 oldQueues 的 pull 任务（不再拉新消息）
  2. 等待消费线程池处理完在途消息（最多 10s）
  3. 超时后强制清理未完成的消息
  4. 释放旧 queue，启动 newQueues 的 pull 任务
  5. 对每个新 queue 查询 Broker 恢复 offset

  // 关键保障: offset 只随 ACK 上报，没 ACK 的消息不算已消费
  // 新 consumer 从上次已提交 offset 开始拉取，未 ACK 消息自然被重新投递

consumeMessages():
  消费成功后:
    ackMessage(messageId)           ← 已有
    reportOffsetToBroker(topic, queueId, offset)  ← 新增

queryOffsetFromBroker(topic, queueId):
  → 向 Broker 发 QUERY_CONSUMER_OFFSET_REQUEST
  → consumeProgress.put(progressKey, offset)

reportOffsetToBroker(topic, queueId, offset):
  → 向 Broker 发 UPDATE_CONSUMER_OFFSET_REQUEST

startOffsetReportTask():
  ScheduledExecutor 每 5s:
    遍历 consumeProgress → reportOffsetToBroker()
```

## 8. 数据流

### 8.1 消费流程

```
Consumer 启动
  ├─ 1. 注册到 NameServer: CONSUMER_REGISTER_REQUEST
  │       → 获得 consumerId 列表 + topic 路由
  ├─ 2. calculateAllocation → 负责 Queue 0, 1, 2
  ├─ 3. 查 Broker: QUERY_CONSUMER_OFFSET_REQUEST
  │       → 恢复各 queue 的上次 offset
  └─ 4. 对 Queue 0, 1, 2 分别启动 pull 定时任务

每次 pull:
  pullMessage("order-topic", queueId=0, offset=15, 10)
  → Broker 返回 3 条消息 → Found(nextBeginOffset=18, ...)
  → consumeProgress.put("order-topic_0", 18)
  → 消费线程处理 3 条消息
  → 每条 ACK + reportOffsetToBroker("order-topic", 0, 18)
  → Broker ConsumerOffsetManager: map["order-group@order-topic@0"] = 18

定时 offset 上报（5s 兜底）:
  → consumeProgress 所有数据推给 Broker
```

### 8.2 Rebalance 流程

```
Consumer-C 加入 "order-group"
  ├─ 注册到 NameServer（启动时随机 sleep 0~3s，避免与 A、B 同时 Rebalance）
  ├─ NameServer 返回消费者列表: [A, B, C]（之前是 [A, B]）
  │
  ├─ Consumer-A（下次 rebalance 检查）:
  │     calculateAllocation(topic, 8, [A,B,C])
  │     之前: 0,1,2,3 → 之后: 0,1,2  (释放 Queue 3)
  │     → onRebalance(old=[0,1,2,3], new=[0,1,2])
  │     → 停止 Queue 3 pull → 等待在途消息处理完(10s超时)
  │     → 释放 Queue 3
  │
  ├─ Consumer-B:
  │     之前: 4,5,6,7 → 之后: 3,4,5  (释放 6,7, 获得 3)
  │     → 停止 Queue 6,7 pull → 等待处理 → 释放
  │     → 查询 Broker 恢复 Queue 3 offset → 启动 Queue 3 pull
  │
  └─ Consumer-C:
        第一次: Queue 6, 7
        → 查询 Broker 恢复 offset → 启动 pull
        → Queue 6, 7 上 Consumer-B 未 ACK 的消息会被重新拉取
```

## 9. 错误处理

| 场景 | 处理 |
|------|------|
| Broker 重启 → offset 文件丢失 | 从 0 开始（Consumer 拉取 offset=0 → 重消费已有消息，业务幂等兜底） |
| offset 文件损坏 | 丢弃并重新开始 |
| ACK 上报 offset 失败 | 不阻塞，定时 5s 兜底重试 |
| Rebalance 期间 offset 提交 | 旧 queue 的 pull 已停止，不再产生新消费，无冲突 |
| Consumer 心跳超时未注销 | NameServer 60s 清理，下次 rebalance 其他 consumer 接管其 queue |
| 多个 Consumer 上报同一 queue offset | 不会发生（同一 queue 只有一个 consumer）；即使发生，`Math.max` 防回退 |
| Rebalance 时正在消费的消息 | 限时等待（10s）→ 超时强制释放。没 ACK 的消息 offset 未提交，新 consumer 从已提交 offset 开始拉取，未 ACK 消息自然重新投递 |
| 多个 Consumer 同时启动 | 启动时随机 sleep 0~3s 错开注册时间；状态机 coalesce 合并触发，等待期间的新变化只重置 timer；分配算法确定（按 consumerId 排序），各 consumer 独立算出同一结果 |

## 10. 测试策略

| 类型 | 测试 |
|------|------|
| 单元 | ConsumerOffsetManager: update/get/persist/load |
| 单元 | QueueAllocationManager: calculateAllocation 各种 consumer 数 |
| 单元 | NameServer: consumer register/heartbeat/unregister |
| 单元 | BrokerRequestHandler: handleUpdateConsumerOffset/handleQueryConsumerOffset |
| 集成 | 单 Consumer 消费 → 重启 → 从断点继续 |
| 集成 | 多 Consumer 消费 → rebalance 触发 → 队列重新分配 |
| 集成 | Consumer 下线 → 心跳超时 → 其他 consumer 接管 |

## 11. 改动范围汇总

| 模块 | 改动 | 说明 |
|------|------|------|
| ruyuan-mq-protocol | 小 | MessageType 新增 8 个枚举值 |
| ruyuan-mq-nameserver | 中 | ServiceRegistry 新增 consumer 管理；NameServerRequestHandler 新增 2 个 handler；HealthChecker 新增 consumer 超时清理 |
| ruyuan-mq-broker | 中 | 新增 ConsumerOffsetManager；BrokerRequestHandler 新增 2 个 handler；ClusterManager 集成 |
| ruyuan-mq-client | 大 | 新增 QueueAllocationManager；ConsumerImpl 大幅改造（多 queue、offset 上报/恢复、rebalance） |
