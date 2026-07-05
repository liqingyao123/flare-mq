# Offset 推进修正与批量上报 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修正 consumer Offset 从 at-most-once 为 at-least-once，将 offset 推进时机从 Pull 后改为消费成功后逐条推进，批次结束统一上报。

**Architecture:** 仅改 `ConsumerImpl.java` 中两个方法：`pullMessageForQueue()` 不再提前推进 offset，`consumeMessages()` 改为逐条推进 + 批次结束上报。定时兜底 `reportAllOffsets()` 已存在无需新增。

**Tech Stack:** Java 8, JUnit 5

## Global Constraints

- 不改 AckManager 及 ack 包
- 不改 BrokerRequestHandler 和协议层
- 不改 Consumer 接口
- 不新增文件
- 定时刷盘（`offsetReportScheduler` 每 5s 调 `reportAllOffsets()`）已存在，不重复创建

---

### Task 1: 修正 pullMessageForQueue — 不再提前推进 offset，传递必要参数给 consumeMessages

**Files:**
- Modify: `flare-mq-client/src/main/java/com/ruyuan/mq/client/consumer/ConsumerImpl.java:613-636`

**Interfaces:**
- Consumes: `consumeProgress` (field), `consumeMessages()` (existing private method)
- Produces: `consumeMessages()` 新签名 `(List<Message>, SubscriptionData, String topic, int queueId, long startOffset)`

- [ ] **Step 1: 修改 pullMessageForQueue() 方法**

删除 `consumeProgress.put(progressKey, pullResult.getNextBeginOffset())`，改为将 `topic`、`queueId`、`offset` 传入 `consumeMessages()`。

当前代码（`ConsumerImpl.java:613-636`）：
```java
private void pullMessageForQueue(String topic, int queueId) {
    try {
        SubscriptionData subscription = subscriptions.get(topic);
        if (subscription == null || !subscription.isEnabled()) return;

        String progressKey = topic + "_" + queueId;
        long offset = consumeProgress.getOrDefault(progressKey, 0L);

        PullResult pullResult = pullMessage(topic, queueId, offset, config.getPullBatchSize());

        if (pullResult.hasMessage()) {
            consumeProgress.put(progressKey, pullResult.getNextBeginOffset());
            // Set queueId on each pulled message for offset tracking
            for (Message msg : pullResult.getMessages()) {
                msg.setQueueId(queueId);
            }
            consumeExecutor.submit(() -> consumeMessages(pullResult.getMessages(), subscription));
            logger.debug("Pulled: topic={}, queueId={}, offset={}, count={}, next={}",
                    topic, queueId, offset, pullResult.getMessageCount(), pullResult.getNextBeginOffset());
        }
    } catch (Exception e) {
        logger.error("Pull failed: topic={}, queueId={}", topic, queueId, e);
    }
}
```

改为：
```java
private void pullMessageForQueue(String topic, int queueId) {
    try {
        SubscriptionData subscription = subscriptions.get(topic);
        if (subscription == null || !subscription.isEnabled()) return;

        String progressKey = topic + "_" + queueId;
        long offset = consumeProgress.getOrDefault(progressKey, 0L);

        PullResult pullResult = pullMessage(topic, queueId, offset, config.getPullBatchSize());

        if (pullResult.hasMessage()) {
            // Set queueId on each pulled message for offset tracking
            for (Message msg : pullResult.getMessages()) {
                msg.setQueueId(queueId);
            }
            final long startOffset = offset;
            consumeExecutor.submit(() -> consumeMessages(
                    pullResult.getMessages(), subscription, topic, queueId, startOffset));
            logger.debug("Pulled: topic={}, queueId={}, offset={}, count={}, next={}",
                    topic, queueId, offset, pullResult.getMessageCount(), pullResult.getNextBeginOffset());
        }
    } catch (Exception e) {
        logger.error("Pull failed: topic={}, queueId={}", topic, queueId, e);
    }
}
```

- [ ] **Step 2: 编译验证**

Run: `mvn compile -pl flare-mq-client -am`
Expected: 编译失败（consumeMessages 签名尚未更新，由 Task 2 修复）

---

### Task 2: 重写 consumeMessages — 逐条推进 offset，批次结束上报，失败 break

**Files:**
- Modify: `flare-mq-client/src/main/java/com/ruyuan/mq/client/consumer/ConsumerImpl.java:677-714`

**Interfaces:**
- Consumes: `consumeProgress` (field), `reportOffsetToBroker()` (existing private method), `SubscriptionData.matchTag()`, `MessageListener.consumeMessage()`
- Produces: (none — internal method)

- [ ] **Step 1: 重写 consumeMessages() 方法签名和实现**

当前代码（`ConsumerImpl.java:677-714`）：
```java
    /**
     * Consume messages
     */
    private void consumeMessages(List<Message> messages, SubscriptionData subscription) {
        for (Message message : messages) {
            long startTime = System.currentTimeMillis();

            try {
                // Check tag matching
                if (!subscription.matchTag(message.getTags())) {
                    continue;
                }

                // Call message listener
                ConsumeStatus status = subscription.getMessageListener().consumeMessage(message);

                long costTime = System.currentTimeMillis() - startTime;

                if (status.isSuccess()) {
                    stats.recordConsumeSuccess(costTime, message.getMessageSize());
                    ackMessage(message.getMessageId());
                    // Report offset to broker
                    String topic = message.getTopic();
                    String progressKey = topic + "_" + message.getQueueId();
                    long currentOffset = consumeProgress.getOrDefault(progressKey, 0L);
                    reportOffsetToBroker(topic, message.getQueueId(), currentOffset);
                } else {
                    stats.recordConsumeFailure(costTime);
                    logger.warn("Consume message failed: messageId={}, status={}", message.getMessageId(), status);
                }

            } catch (Exception e) {
                long costTime = System.currentTimeMillis() - startTime;
                stats.recordConsumeFailure(costTime);
                logger.error("Consume message exception: messageId=" + message.getMessageId(), e);
            }
        }
    }
```

改为：
```java
    /**
     * Consume messages — offset advances per-message on success, reported once per batch.
     * On first failure, stops processing the remaining messages in the batch.
     */
    private void consumeMessages(List<Message> messages, SubscriptionData subscription,
                                 String topic, int queueId, long startOffset) {
        long currentOffset = startOffset;
        long lastSuccessOffset = -1;

        for (Message message : messages) {
            long startTime = System.currentTimeMillis();

            try {
                if (!subscription.matchTag(message.getTags())) {
                    continue;
                }

                ConsumeStatus status = subscription.getMessageListener().consumeMessage(message);

                long costTime = System.currentTimeMillis() - startTime;

                if (status.isSuccess()) {
                    stats.recordConsumeSuccess(costTime, message.getMessageSize());
                    lastSuccessOffset = currentOffset + 1;
                    currentOffset++;
                } else {
                    stats.recordConsumeFailure(costTime);
                    logger.warn("Consume message failed, stopping batch: messageId={}, status={}",
                            message.getMessageId(), status);
                    break;
                }

            } catch (Exception e) {
                long costTime = System.currentTimeMillis() - startTime;
                stats.recordConsumeFailure(costTime);
                logger.error("Consume message exception, stopping batch: messageId="
                        + message.getMessageId(), e);
                break;
            }
        }

        // Report offset once after batch
        if (lastSuccessOffset >= 0) {
            String progressKey = topic + "_" + queueId;
            consumeProgress.put(progressKey, lastSuccessOffset);
            reportOffsetToBroker(topic, queueId, lastSuccessOffset);
        }
    }
```

- [ ] **Step 2: 编译验证**

Run: `mvn compile -pl flare-mq-client -am`
Expected: 编译成功

---

### Task 3: 运行现有测试，确保无回归

**Files:**
- 无新建文件

- [ ] **Step 1: 运行 Consumer 相关单元测试**

Run: `mvn test -pl flare-mq-client -Dtest=ConsumerTest`
Expected: 全部 PASS

- [ ] **Step 2: 运行 AckManager 测试（确保不动部分无影响）**

Run: `mvn test -pl flare-mq-broker -Dtest=AckManagerTest`
Expected: 全部 PASS

- [ ] **Step 3: 运行 ConsumerOffsetManager 测试**

Run: `mvn test -pl flare-mq-broker -Dtest=ConsumerOffsetManagerTest`
Expected: 全部 PASS

- [ ] **Step 4: 运行加 Rebalance 测试**

Run: `mvn test -pl flare-mq-test -Dtest=RebalanceTest`
Expected: 全部 PASS

- [ ] **Step 5: 运行全量测试**

Run: `mvn test`
Expected: 全部 PASS

- [ ] **Step 6: Commit**

```bash
git add flare-mq-client/src/main/java/com/ruyuan/mq/client/consumer/ConsumerImpl.java
git commit -m "fix: advance offset after consume success, report once per batch"
```
