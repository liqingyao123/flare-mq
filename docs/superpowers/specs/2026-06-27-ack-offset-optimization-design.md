# 消费确认机制优化：Offset 推进修正与批量上报

## 目标

修正消费 Offset 的推进时机（从 at-most-once 提升为 at-least-once），并降低上报频率（从逐条上报改为批次上报 + 定时兜底）。消费失败的场景由业务层自行处理，不引入额外的重试状态机。

## 动机

当前 Offset 在 Pull 拉取消息后立即推进到 `nextBeginOffset`，消费线程异步处理。存在三个问题：

1. **语义错误**：Offset 在消费前推进，消息实际未处理但 offset 已跳过，为 at-most-once 语义
2. **上报过频**：每消费成功一条消息就发送一次 `UPDATE_CONSUMER_OFFSET_REQUEST`，网络开销大
3. **失败静默丢失**：消费失败只打 warn 日志，offset 已推进无法回退，消息永久丢失

## 设计

### 1. Offset 推进时机：从 Pull 时改为消费成功后

**Pull 线程不再推进 offset**

`pullMessageForQueue()` 中删除 `consumeProgress.put(progressKey, pullResult.getNextBeginOffset())`，
改为只将起始 offset 传入消费方法。

**消费线程逐条推进，批次结束统一上报**

```
consumeMessages(messages, subscription, startOffset):
    currentOffset = startOffset
    lastSuccessOffset = -1

    for each message:
        status = listener.consumeMessage(message)
        if success:
            lastSuccessOffset = currentOffset + 1
            currentOffset++
        else:
            break   // 失败停止，剩余消息下次重拉

    if lastSuccessOffset >= 0:
        consumeProgress.put(key, lastSuccessOffset)
        reportOffsetToBroker(topic, queueId, lastSuccessOffset)  // 整批一次
```

### 2. 定时兜底上报

复用已有的 `reportAllOffsets()` 方法，加定时调度：

```java
// ConsumerImpl.start() 中
pullScheduler.scheduleWithFixedDelay(
    this::reportAllOffsets, 10, 10, TimeUnit.SECONDS
);
```

- 幂等：`ConsumerOffsetManager.updateOffset()` 已用 `Math.max` 保证 offset 单调递增
- 最多 10 秒延迟的持久化保障

### 3. 不动项

| 组件 | 处理方式 |
|------|---------|
| AckManager 及 ack 包 | 维持现状，不删除不集成 |
| `ACK_MESSAGE_REQUEST/RESPONSE` | 保留枚举值 |
| `BrokerRequestHandler.handleAckMessage()` | 保留桩代码 |
| `ConsumerOffsetManager` | 无需改动 |
| `Consumer.ackMessage/ackMessages` | 保留接口声明 |

### 4. 语义变化

| 维度 | 改前 | 改后 |
|------|------|------|
| 投递语义 | at-most-once | at-least-once |
| Offset 推进时机 | Pull 后立即 | 消费成功后逐条 |
| 上报频率 | 每条一次 | 每批一次 + 10s 兜底 |
| 失败行为 | 静默丢失 | break，下次 Pull 重拉 |
| 毒消息处理 | — | 业务层返回 SUCCESS 跳过 |

## 改动文件

仅 `ConsumerImpl.java`：

| 位置 | 改动内容 |
|------|---------|
| `pullMessageForQueue()` | 删除 `consumeProgress.put(progressKey, nextBeginOffset)`；将起始 offset 传入 `consumeMessages()` |
| `consumeMessages()` 方法签名 | 增加 `long startOffset` 参数 |
| `consumeMessages()` 方法体 | 逐条推进 offset，批次结束上报一次，失败 break |
| `start()` 或调度初始化处 | 增加 `reportAllOffsets` 定时任务 |
| `consumeMessages()` 中的 `ackMessage()` 调用 | 删除（已有 offset 确认替代） |

## 测试要点

- 消费成功一批后，Broker offset 正确推进到 `起始offset + 成功条数`
- 消费中间失败后 break，后续消息在下次 Pull 时重新拉取
- 定时兜底任务将本地 offset 同步到 Broker
- Rebalance 恢复从 Broker 查到的 offset 即为最后成功的消费位点
- `consumeMessages()` 的调用点同步更新参数（如队列恢复等路径）
