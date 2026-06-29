# 消费 ACK 与重试机制：打通 AckManager 链路

## 目标

修正上一版方案（不推进 offset → at-least-once）带来的竞态问题，改为 RocketMQ 模式：Pull 线程照常推进 offset，消费失败走 Broker 侧重投递补偿。

## 动机

上一版 `2026-06-27-ack-offset-optimization` 方案的核心问题是 Pull 线程（每 1ms 调度）和 Consume 线程（异步线程池）存在竞态：消费未完成时 offset 未推进，Pull 线程会重复拉取同一批消息。

正确做法：Pull 线程大胆推进 offset（不阻塞、不重复拉取），消费失败不依赖 offset 回退，而是将消息回传 Broker 进入重试队列。

## 架构

```
Pull线程 (不变): 拉取消息 → advance offset → 提交到 consumeExecutor
                     ↓ 发消息时 Broker 调用 addPendingAck(messageId, offset)

Consume线程:
  ├─ SUCCESS → ackMessage → Broker: AckManager.ackMessage()
  └─ FAILURE → sendMessageBack → Broker: AckManager.addToRetryQueue()

Broker定时任务:
  checkAckTimeout() 每30s → PENDING超时消息 → addToRetryQueue
  checkRetryMessages() 每5s → 到期重试消息 → 按offset从CommitLog读 → 回投原Topic
  重试16次后 → moveToDeadLetterQueue
```

核心原则：offset 推进与消息确认解耦。offset 只管"是否拉取过"，Ack 只管"是否消费成功"。

## 前置依赖：messageId 贯通（阻塞性问题）

当前 messageId 在 Pull-Ack 链路中全程丢失：

```
Producer → SendRequest.messageId (有值)
    ↓  handleSendMessage 创建 ruanyuan-mq.store.Message → 未设 messageId
CommitLog (messageId 未持久化)
    ↓  handlePullMessage → SimpleMessage → 无 messageId
Consumer 解析 PullResponse → Message.messageId = null
    ↓  ackMessage(null) → AckManager 收到空 messageId
```

**必须在实现 ACK 重试之前完成以下贯通：**

| 层 | 改动 |
|----|------|
| `ruyuan-mq.store.Message` | 新增 `messageId` 字段 + getter/setter |
| `MessageSerializer` | 序列化/反序列化 messageId（加在 body 之前或 properties 中） |
| Broker `handleSendMessage` | `storeMsg.setMessageId(sendReq.messageId)` |
| Broker `SimpleMessage` | 新增 `messageId` 字段 |
| Broker `handlePullMessage` | `sm.messageId = m.getMessageId()` |
| Client `SimpleMessageDTO` | 新增 `messageId` 字段 |
| Client `handlePullResponse` | `msg.setMessageId(sm.messageId)` |

完成后，Consumer 侧的 `message.getMessageId()` 才返回有效值，整个 ACK 链路才走得通。

## 详细设计

### 1. ACK 协议体扩展

扩展 `ACK_MESSAGE_REQUEST` 的 JSON body，区分成功确认和失败回传：

```json
{
  "ackType": "SUCCESS",
  "messageId": "msg-xxx",
  "consumerGroup": "my-group"
}
```

失败回传时附带 topic 和失败原因：

```json
{
  "ackType": "FAILURE",
  "messageId": "msg-xxx",
  "consumerGroup": "my-group",
  "topic": "my-topic",
  "queueId": 0,
  "failureReason": "业务处理异常"
}
```

### 2. Client 侧（ConsumerImpl.java）

`consumeMessages()` 方法：成功分支不变，失败分支从只打日志改为调用新增的 `sendMessageBackToBroker()`：

```java
if (status.isSuccess()) {
    stats.recordConsumeSuccess(costTime, message.getMessageSize());
    ackMessage(message.getMessageId());
} else {
    stats.recordConsumeFailure(costTime);
    sendMessageBackToBroker(message, status.getReason());
    // 不 break，继续处理同批后续消息
}
```

新增 `sendMessageBackToBroker(Message msg, String reason)`：构建 ackType=FAILURE 的 ACK 请求，调用已有 `sendToBroker()` 发送。Pull 线程的 offset 推进逻辑保持不变。

### 3. Broker 侧（BrokerRequestHandler）

`handleAckMessage()` 从桩代码改为真正实现：

```java
private ProtocolMessage handleAckMessage(ProtocolMessage request) {
    AckBody body = parseAckBody(request);

    if ("SUCCESS".equals(body.ackType)) {
        ackManager.ackMessage(body.messageId, body.consumerGroup);
    } else {
        ackManager.addToRetryQueue(body.messageId, body.failureReason);
    }
    return successResponse;
}
```

`handlePullMessage()` 在返回消息前注册 Pending Ack（需要 commitLogOffset 和 storeSize 两个值用于重投时读回消息）：

```java
for (Message msg : pullResult.getMessages()) {
    ackManager.addPendingAck(msg.getMessageId(), consumerGroup, topic, queueId,
                              null, msg.getCommitLogOffset(), msg.getStoreSize());
}
```

`AckRecord` 新增 `storeSize` 字段，`addPendingAck()` 方法同步增加参数。

### 4. AckManager 改造

新增 `RetryMessageHandler` 回调接口，由 Broker 在初始化时注入：

```java
public interface RetryMessageHandler {
    void onRetryMessage(RetryRecord retryRecord, AckRecord ackRecord);
}
```

`checkRetryMessages()` 从只打日志改为：

```java
for (RetryRecord record : getRetryMessages()) {
    AckRecord ackRecord = ackRecords.get(record.getMessageId());
    if (record.getRetryCount() > DEFAULT_MAX_RETRY_TIMES) {
        moveToDeadLetterQueue(record.getMessageId(), "超过最大重试次数");
    } else {
        retryHandler.onRetryMessage(record, ackRecord);
    }
}
```

Broker 在实现 `RetryMessageHandler` 时：
1. 从 `ackRecord.getMessageOffset()` + `ackRecord.getStoreSize()` 按 CommitLog 读回原始消息
2. 调用 `DefaultMessageStore.putMessage()` 回投到原 Topic
3. 消息不变，Consumer 下次 Pull 自然拉到

### 5. 重试语义

| 维度 | 行为 |
|------|------|
| 投递语义 | at-least-once（offset 先推进 + 失败重试补偿） |
| 重试延迟 | 指数退避：1s, 2s, 4s, 8s, 16s, 32s, 60s（已有） |
| 最大重试 | 16 次（已有） |
| 死信处理 | 超过最大重试 → moveToDeadLetterQueue（已有） |
| 超时兜底 | PENDING 消息 30s 无 ACK → 自动进重试（已有） |
| 失败处理 | 仅失败消息回传 Broker 重投递，同批其余消息继续处理 |

## 改动文件

| 文件 | 改动 |
|------|------|
| `ruyuan-mq.store.Message` | 新增 `messageId` 字段 + getter/setter |
| `MessageSerializer.java` | 序列化/反序列化 messageId |
| `BrokerRequestHandler.java` | `handleSendMessage`: 设 messageId；`handlePullMessage`: 传 messageId + 注册 Pending Ack；`SimpleMessage`: 加 messageId；`handleAckMessage`: 真正实现 |
| `ConsumerImpl.java` | `SimpleMessageDTO`: 加 messageId；`handlePullResponse`: 解析 messageId；`consumeMessages()` 失败处理；新增 `sendMessageBackToBroker()` |
| `AckManager.java` | 新增 `RetryMessageHandler` 接口；`checkRetryMessages()` 改为回调回投；`addPendingAck()` 利用已有 `messageOffset` 参数 |

## 不变项

- `Consumer.java` 接口不变
- 协议消息类型不变（复用 `ACK_MESSAGE_REQUEST/RESPONSE`）
- Pull 线程逻辑不变（offset 照常推进）
- Pull 调度频率不变（默认 1ms）
- AckManager 的指数退避、死信队列、超时检测逻辑不变
- `MessageType` 枚举、`AckResult`、`RetryRecord`、`AckRecord` 等数据结构不变

## 废弃项

上一版 `2026-06-27-ack-offset-optimization` 的设计与实现计划作废，由本方案替代。
