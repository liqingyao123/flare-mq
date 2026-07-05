# Queue 负载均衡修复设计

## 问题

Broker 端 `handleSendMessage` 调用 `selectLeastLoadedQueue()` 选择队列，但消息写入成功后从未调用 `queue.incrementMessageCount()`，导致所有队列的 messageCount 始终为 0。`Stream.min()` 在全部相等时返回第一个元素 (queue 0)，消息总是写入 queue 0。

## 方案

Broker 端决定队列（不传 queueId），修复计数器更新。

## 改动

**文件**: `flare-mq-broker/src/main/java/com/ruyuan/mq/broker/BrokerRequestHandler.java`

在 `handleSendMessage` 方法中，`putMessage` 成功后调用 `queue.incrementMessageCount()`:

```
选队列 → 构造消息 → putMessage → [新增] queue.incrementMessageCount() → 返回响应
```

### 不改动

- 客户端 `TopicRouteInfo.selectQueue()` — 保持不变，轮询选 Broker 仍有价值
- `SendRequest` DTO — 不加 queueId 字段
- `QueueManager.selectLeastLoadedQueue()` — 不做改动，修复计数器即可工作
- `QueueConfig.incrementMessageCount()` — 方法已存在，无需新增代码

## 验证

修复后（4 队列，初始 count 均为 0）：

```
msg1 → min=all 0 → queue 0 → count0=1
msg2 → min=1,0,0,0 → queue 1 → count1=1
msg3 → min=1,1,0,0 → queue 2 → count2=1
msg4 → min=1,1,1,0 → queue 3 → count3=1
msg5 → min=all 1 → queue 0 → count0=2
...
```

均匀分布。现有测试 `testSelectLeastLoadedQueue` 已覆盖 `incrementMessageCount` + `selectLeastLoadedQueue` 联动。
