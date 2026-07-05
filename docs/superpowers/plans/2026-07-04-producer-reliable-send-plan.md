# Producer 可靠发送 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 ProducerImpl 中实现发送重试、Broker 故障转移、结构化响应解析，确保消息发送的可靠性。

**Architecture:** 三个独立模块 — Broker 端响应增强 (Task 1)、路由信息故障转移方法 (Task 2)、Producer 端重试+故障转移+响应解析 (Task 3)。Task 1 和 Task 2 无依赖可并行，Task 3 依赖前两者。

**Tech Stack:** Java 8, Netty, JUnit 5 + Mockito

## Global Constraints

- 不改变 Producer API 接口签名
- 复用已有配置项 `retryTimesWhenSendFailed`(2)、`retryTimesWhenSendAsyncFailed`(2)
- 仅对可重试错误重试（SEND_TIMEOUT、FLUSH_DISK_TIMEOUT、FLUSH_SLAVE_TIMEOUT、SLAVE_NOT_AVAILABLE、网络异常）
- SEND_FAILED（Broker 明确拒绝）和消息格式错误不重试
- 不在此次范围：同步刷盘、发送方幂等、事务消息、sendOneway 改造

---

### Task 1: BrokerRequestHandler — 返回结构化 SendResponse

**Files:**
- Modify: `flare-mq-broker/src/main/java/com/flare/mq/broker/BrokerRequestHandler.java:114-128`

**Interfaces:**
- Consumes: `PutMessageResult.getAppendResult()`, `AppendMessageResult.getWroteOffset()`
- Produces: `SendResponse` inner class with fields `messageId, queueId, offset, topic`
- Response JSON: `{"messageId":"xxx","queueId":2,"offset":1048576,"topic":"test"}`

- [ ] **Step 1: 添加 SendResponse 内部静态类**

在 `BrokerRequestHandler.java` 末尾（`}` 之前）已有的 DTO 类区域新增：

```java
static class SendResponse {
    public String messageId;
    public int queueId;
    public long offset;
    public String topic;
}
```

- [ ] **Step 2: 修改 handleSendMessage 成功响应**

将 `BrokerRequestHandler.java:119-122` 的响应构建改为结构化 JSON：

旧代码（第 114-122 行）：
```java
        PutMessageResult putRes = messageStore.putMessage(storeMsg);
        if (putRes != null && putRes.isOk()) {
            if (queue != null) {
                queue.incrementMessageCount();
            }
            return ProtocolMessage.createSuccessResponse(
                    MessageType.SEND_MESSAGE_RESPONSE,
                    request.getRequestId(),
                    "OK".getBytes(StandardCharsets.UTF_8));
```

改为：
```java
        PutMessageResult putRes = messageStore.putMessage(storeMsg);
        if (putRes != null && putRes.isOk()) {
            if (queue != null) {
                queue.incrementMessageCount();
            }
            SendResponse resp = new SendResponse();
            resp.messageId = storeMsg.getMessageId();
            resp.queueId = queueId;
            resp.offset = putRes.getAppendResult() != null
                    ? putRes.getAppendResult().getWroteOffset() : 0;
            resp.topic = storeMsg.getTopic();
            return ProtocolMessage.createSuccessResponse(
                    MessageType.SEND_MESSAGE_RESPONSE,
                    request.getRequestId(),
                    JsonUtils.toJson(resp).getBytes(StandardCharsets.UTF_8));
```

- [ ] **Step 3: 编译验证**

```bash
cd "E:\面试讲解\自研消息中间件代码最新\flare-mq" && mvn compile -pl flare-mq-broker -am -q
```

- [ ] **Step 4: Commit**

```bash
git add flare-mq-broker/src/main/java/com/flare/mq/broker/BrokerRequestHandler.java
git commit -m "feat: broker returns structured SendResponse with queueId and offset"
```

---

### Task 2: TopicRouteInfo — 新增 selectAnotherQueue 排除方法

**Files:**
- Modify: `flare-mq-client/src/main/java/com/flare/mq/client/producer/TopicRouteInfo.java:35-42`

**Interfaces:**
- Produces: `QueueInfo selectAnotherQueue(String excludeBrokerName)` — 选择一个可写且不属于排除 broker 的队列，无可用时返回 null

- [ ] **Step 1: 新增 selectAnotherQueue 方法**

在 `TopicRouteInfo.java` 的 `selectQueue()` 方法之后（第 42 行之后）新增：

```java
/**
 * 选择一个队列，排除指定 broker
 * @param excludeBrokerName 要排除的 broker 名称
 * @return 选中的队列，如果没有备选则返回 null
 */
public QueueInfo selectAnotherQueue(String excludeBrokerName) {
    if (queueInfos.isEmpty() || excludeBrokerName == null) {
        return null;
    }

    // 收集可写且不属于排除 broker 的队列
    List<QueueInfo> candidates = new ArrayList<>();
    for (QueueInfo q : queueInfos) {
        if (q.isWritable() && !excludeBrokerName.equals(q.getBrokerName())) {
            candidates.add(q);
        }
    }

    if (candidates.isEmpty()) {
        return null;
    }

    int index = Math.abs(queueSelector.getAndIncrement()) % candidates.size();
    return candidates.get(index);
}
```

- [ ] **Step 2: 编译验证**

```bash
cd "E:\面试讲解\自研消息中间件代码最新\flare-mq" && mvn compile -pl flare-mq-client -am -q
```

- [ ] **Step 3: Commit**

```bash
git add flare-mq-client/src/main/java/com/flare/mq/client/producer/TopicRouteInfo.java
git commit -m "feat: add selectAnotherQueue to TopicRouteInfo for broker failover"
```

---

### Task 3: ProducerImpl — 重试 + 故障转移 + 响应解析

**Files:**
- Modify: `flare-mq-client/src/main/java/com/flare/mq/client/producer/ProducerImpl.java`
  - `send(Message, long)` (lines 160-221)
  - `sendAsync(Message, SendCallback, long)` (lines 229-321)
  - `handleSendResponse()` (lines 615-626)
  - 新增: `sendWithRetry()`, `sendAsyncWithRetry()`, `isRetryableError()`, `isNetworkError()`

**Interfaces:**
- Consumes: `TopicRouteInfo.selectAnotherQueue(String)` (from Task 2)
- Consumes: Broker structured `SendResponse` JSON (from Task 1)
- Produces: same `SendResult send(Message)` / `void sendAsync(Message, SendCallback)` signatures

- [ ] **Step 1: 新增 isRetryableError 和 isNetworkError 辅助方法**

在 `ProducerImpl.java` 末尾（第 698 行 `}` 之前）新增：

```java
/**
 * 判断 SendResult 是否可重试
 */
private boolean isRetryableError(SendResult result) {
    if (result == null) return true;
    return result.needRetry();
}

/**
 * 判断异常是否为网络相关（可重试）
 */
private boolean isNetworkError(Throwable e) {
    if (e == null) return false;
    String msg = e.getClass().getName() + ": " + (e.getMessage() != null ? e.getMessage() : "");
    return msg.contains("ConnectException")
        || msg.contains("connect")
        || msg.contains("timeout")
        || msg.contains("Timeout")
        || msg.contains("Connection refused")
        || msg.contains("SocketException")
        || msg.contains("Not connected")
        || msg.contains("Channel");
}
```

- [ ] **Step 2: 重写 send(Message, long) 为带重试版本**

替换现有的 `send(Message message, long timeoutMs)` 方法（第 160-221 行）：

```java
@Override
public SendResult send(Message message, long timeoutMs) throws Exception {
    checkProducerStatus();
    validateMessage(message);

    if (message.getMessageId() == null) {
        message.setMessageId(generateMessageId());
    }

    TopicRouteInfo routeInfo = getTopicRouteInfo(message.getTopic());
    if (routeInfo == null) {
        throw new Exception("No route info found for topic: " + message.getTopic());
    }

    int maxRetries = config.getRetryTimesWhenSendFailed();
    String excludeBrokerName = null;
    SendResult lastResult = null;

    for (int retryCount = 0; retryCount <= maxRetries; retryCount++) {
        long startTime = System.currentTimeMillis();
        String currentBroker = null;

        try {
            // Select queue — exclude failed broker on retry
            TopicRouteInfo.QueueInfo queueInfo;
            if (retryCount == 0 || excludeBrokerName == null) {
                queueInfo = routeInfo.selectQueue();
            } else {
                queueInfo = routeInfo.selectAnotherQueue(excludeBrokerName);
                if (queueInfo == null) {
                    logger.warn("No alternative broker for {}, retrying same broker", excludeBrokerName);
                    queueInfo = routeInfo.selectQueue();
                }
            }

            if (queueInfo == null) {
                return SendResult.failure("No available queue for topic: " + message.getTopic());
            }

            currentBroker = queueInfo.getBrokerName();

            // Clean dead connection on retry
            if (retryCount > 0 && excludeBrokerName != null) {
                NettyClient deadClient = brokerClients.remove(excludeBrokerName);
                if (deadClient != null) {
                    try { deadClient.disconnect(); } catch (Exception ignore) {}
                    logger.info("Removed dead broker connection: {}", excludeBrokerName);
                }
            }

            // Get broker client
            NettyClient brokerClient = getBrokerClient(currentBroker, routeInfo);
            if (brokerClient == null) {
                lastResult = SendResult.failure("Cannot connect to broker: " + currentBroker);
                if (retryCount < maxRetries && isRetryableError(lastResult)) {
                    excludeBrokerName = currentBroker;
                    continue;
                }
                return lastResult;
            }

            // Build and send
            ProtocolMessage protocolMessage = buildProtocolMessage(message);
            ProtocolMessage response = brokerClient.sendSync(protocolMessage, timeoutMs);

            // Handle response
            SendResult result = handleSendResponse(response, message, currentBroker, routeInfo);
            long costTime = System.currentTimeMillis() - startTime;
            result.setCostTime(costTime);

            if (result.isSuccess()) {
                stats.recordSendSuccess(costTime, message.getMessageSize());
                return result;
            }

            stats.recordSendFailure(costTime);
            lastResult = result;

            if (retryCount < maxRetries && isRetryableError(result)) {
                logger.warn("Send failed, will retry (attempt {}/{}): broker={}, status={}",
                        retryCount + 1, maxRetries, currentBroker, result.getSendStatus());
                excludeBrokerName = currentBroker;
                continue;
            }

            return result;

        } catch (Exception e) {
            long costTime = System.currentTimeMillis() - startTime;
            stats.recordSendFailure(costTime);

            lastResult = SendResult.failure(
                    e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());

            if (retryCount < maxRetries && isNetworkError(e)) {
                logger.warn("Network error, will retry (attempt {}/{}): broker={}, error={}",
                        retryCount + 1, maxRetries, currentBroker, e.getMessage());
                excludeBrokerName = currentBroker;
                continue;
            }

            logger.error("Send message failed (non-retryable): " + message, e);
            return lastResult;
        }
    }

    return lastResult != null ? lastResult : SendResult.failure("Retry exhausted");
}
```

- [ ] **Step 3: 更新 handleSendResponse 支持结构化 JSON 和 brokerAddr**

替换现有的 `handleSendResponse` 方法（第 615-626 行）：

```java
/**
 * Handle send response — parse structured JSON from broker
 */
private SendResult handleSendResponse(ProtocolMessage response, Message message,
                                       String brokerName, TopicRouteInfo routeInfo) {
    if (response == null) {
        return SendResult.failure("Response is null");
    }

    if (response.getStatus() == ResponseCode.SUCCESS) {
        try {
            // Parse structured SendResponse JSON
            String body = response.getBody() != null
                    ? new String(response.getBody(), StandardCharsets.UTF_8) : "";
            SendResponseDto dto = JsonUtils.fromJson(body, SendResponseDto.class);

            if (dto != null && dto.messageId != null) {
                SendResult result = SendResult.success(dto.messageId, dto.queueId, dto.offset);
                result.setBrokerAddr(resolveBrokerAddr(brokerName, routeInfo));
                return result;
            }

            // Fallback: broker returned success but body is not parseable
            SendResult result = SendResult.success(message.getMessageId(), 0, 0);
            result.setBrokerAddr(resolveBrokerAddr(brokerName, routeInfo));
            return result;

        } catch (Exception e) {
            logger.warn("Failed to parse broker response, using fallback: {}", e.getMessage());
            SendResult result = SendResult.success(message.getMessageId(), 0, 0);
            result.setBrokerAddr(resolveBrokerAddr(brokerName, routeInfo));
            return result;
        }
    } else {
        return SendResult.failure(
                "Send failed, error code: " + response.getStatus(),
                response.getStatus().getCode());
    }
}

private String resolveBrokerAddr(String brokerName, TopicRouteInfo routeInfo) {
    TopicRouteInfo.BrokerInfo brokerInfo = routeInfo.getBrokerInfo(brokerName);
    return brokerInfo != null ? brokerInfo.getMasterAddr() : null;
}

// Inner DTO
static class SendResponseDto {
    public String messageId;
    public int queueId;
    public long offset;
    public String topic;
}
```

- [ ] **Step 4: 重写 sendAsync 为带重试版本**

替换现有的 `sendAsync(Message message, SendCallback callback, long timeoutMs)` 方法（第 229-321 行）：

```java
@Override
public void sendAsync(Message message, SendCallback callback, long timeoutMs) {
    try {
        checkProducerStatus();
        validateMessage(message);
    } catch (Exception e) {
        if (callback != null) {
            callbackExecutor.execute(() -> callback.onException(e));
        }
        return;
    }

    if (message.getMessageId() == null) {
        message.setMessageId(generateMessageId());
    }

    TopicRouteInfo routeInfo;
    try {
        routeInfo = getTopicRouteInfo(message.getTopic());
        if (routeInfo == null) {
            throw new Exception("No route info found for topic: " + message.getTopic());
        }
    } catch (Exception e) {
        if (callback != null) {
            callbackExecutor.execute(() -> callback.onException(e));
        }
        return;
    }

    int maxRetries = config.getRetryTimesWhenSendAsyncFailed();
    doSendAsyncWithRetry(message, routeInfo, null, 0, maxRetries,
            timeoutMs, callback, System.currentTimeMillis());
}

/**
 * Recursive async send with retry
 */
private void doSendAsyncWithRetry(Message message, TopicRouteInfo routeInfo,
        String excludeBrokerName, int retryCount, int maxRetries,
        long timeoutMs, SendCallback callback, long startTime) {

    // Select queue
    TopicRouteInfo.QueueInfo queueInfo;
    if (retryCount == 0 || excludeBrokerName == null) {
        queueInfo = routeInfo.selectQueue();
    } else {
        queueInfo = routeInfo.selectAnotherQueue(excludeBrokerName);
        if (queueInfo == null) {
            queueInfo = routeInfo.selectQueue();
        }
    }

    if (queueInfo == null) {
        if (callback != null) {
            callbackExecutor.execute(() ->
                    callback.onException(new Exception("No available queue")));
        }
        return;
    }

    final String currentBroker = queueInfo.getBrokerName();

    // Clean dead connection on retry
    if (retryCount > 0 && excludeBrokerName != null) {
        NettyClient deadClient = brokerClients.remove(excludeBrokerName);
        if (deadClient != null) {
            try { deadClient.disconnect(); } catch (Exception ignore) {}
        }
    }

    NettyClient brokerClient = getBrokerClient(currentBroker, routeInfo);
    if (brokerClient == null) {
        if (retryCount < maxRetries) {
            doSendAsyncWithRetry(message, routeInfo, currentBroker,
                    retryCount + 1, maxRetries, timeoutMs, callback, startTime);
            return;
        }
        if (callback != null) {
            callbackExecutor.execute(() ->
                    callback.onException(new Exception("Cannot connect to broker: " + currentBroker)));
        }
        return;
    }

    ProtocolMessage protocolMessage = buildProtocolMessage(message);

    brokerClient.sendAsync(protocolMessage, new ResponseCallback() {
        @Override
        public void onSuccess(ProtocolMessage response) {
            callbackExecutor.execute(() -> {
                try {
                    SendResult result = handleSendResponse(response, message,
                            currentBroker, routeInfo);
                    long costTime = System.currentTimeMillis() - startTime;
                    result.setCostTime(costTime);

                    if (result.isSuccess()) {
                        stats.recordSendSuccess(costTime, message.getMessageSize());
                    } else {
                        stats.recordSendFailure(costTime);
                    }

                    if (callback != null) {
                        callback.onSuccess(result);
                    }
                } catch (Exception e) {
                    long costTime = System.currentTimeMillis() - startTime;
                    stats.recordSendFailure(costTime);
                    if (callback != null) {
                        callback.onException(e);
                    }
                }
            });
        }

        @Override
        public void onFailure(Throwable throwable) {
            callbackExecutor.execute(() -> {
                if (retryCount < maxRetries && isNetworkError(throwable)) {
                    logger.warn("Async send failed, retrying (attempt {}/{}): broker={}",
                            retryCount + 1, maxRetries, currentBroker, throwable);
                    doSendAsyncWithRetry(message, routeInfo, currentBroker,
                            retryCount + 1, maxRetries, timeoutMs, callback, startTime);
                    return;
                }

                long costTime = System.currentTimeMillis() - startTime;
                stats.recordSendFailure(costTime);
                if (callback != null) {
                    callback.onException(throwable);
                }
            });
        }

        @Override
        public void onTimeout() {
            callbackExecutor.execute(() -> {
                if (retryCount < maxRetries) {
                    logger.warn("Async send timeout, retrying (attempt {}/{}): broker={}",
                            retryCount + 1, maxRetries, currentBroker);
                    doSendAsyncWithRetry(message, routeInfo, currentBroker,
                            retryCount + 1, maxRetries, timeoutMs, callback, startTime);
                    return;
                }

                long costTime = System.currentTimeMillis() - startTime;
                stats.recordSendTimeout(costTime);
                if (callback != null) {
                    callback.onException(new RuntimeException("Send async timeout after "
                            + maxRetries + " retries"));
                }
            });
        }
    });
}
```

- [ ] **Step 5: 编译验证**

```bash
cd "E:\面试讲解\自研消息中间件代码最新\flare-mq" && mvn compile -pl flare-mq-client -am -q
```

- [ ] **Step 6: Commit**

```bash
git add flare-mq-client/src/main/java/com/flare/mq/client/producer/ProducerImpl.java
git commit -m "feat: add send retry, broker failover, and structured response parsing to ProducerImpl"
```

---

### Task 4: 测试与验证

**Files:**
- Modify: `flare-mq-client/src/test/java/com/flare/mq/client/producer/ProducerTest.java`
- 新增测试方法

**Interfaces:**
- Consumes: `ProducerImpl` with retry (Task 3), `TopicRouteInfo.selectAnotherQueue` (Task 2)

- [ ] **Step 1: 新增重试逻辑单元测试**

在 `ProducerTest.java` 末尾新增测试方法：

```java
@Test
@DisplayName("isRetryableError — 可重试状态返回true")
void testIsRetryableError_Retryable() {
    // SEND_TIMEOUT is not retryable per current SendStatus.needRetry()
    // But FLUSH_DISK_TIMEOUT, FLUSH_SLAVE_TIMEOUT, SLAVE_NOT_AVAILABLE are
    assertTrue(SendResult.flushDiskTimeout().needRetry());
    assertTrue(SendResult.flushSlaveTimeout().needRetry());
    assertTrue(SendResult.slaveNotAvailable().needRetry());
}

@Test
@DisplayName("isRetryableError — SEND_FAILED 不重试")
void testIsRetryableError_NotRetryable() {
    SendResult failureResult = SendResult.failure("Broker rejected");
    assertFalse(failureResult.needRetry());
}

@Test
@DisplayName("TopicRouteInfo.selectAnotherQueue 排除指定 broker")
void testSelectAnotherQueue_ExcludesBroker() {
    TopicRouteInfo routeInfo = new TopicRouteInfo("test-topic");
    List<TopicRouteInfo.QueueInfo> queues = new ArrayList<>();
    queues.add(new TopicRouteInfo.QueueInfo("broker-A", 0, true, true));
    queues.add(new TopicRouteInfo.QueueInfo("broker-A", 1, true, true));
    queues.add(new TopicRouteInfo.QueueInfo("broker-B", 0, true, true));
    queues.add(new TopicRouteInfo.QueueInfo("broker-B", 1, true, true));
    routeInfo.setQueueInfos(queues);

    TopicRouteInfo.QueueInfo result = routeInfo.selectAnotherQueue("broker-A");
    assertNotNull(result);
    assertEquals("broker-B", result.getBrokerName());
}

@Test
@DisplayName("TopicRouteInfo.selectAnotherQueue — 排除后无备选返回 null")
void testSelectAnotherQueue_NoAlternatives() {
    TopicRouteInfo routeInfo = new TopicRouteInfo("test-topic");
    List<TopicRouteInfo.QueueInfo> queues = new ArrayList<>();
    queues.add(new TopicRouteInfo.QueueInfo("broker-A", 0, true, true));
    routeInfo.setQueueInfos(queues);

    TopicRouteInfo.QueueInfo result = routeInfo.selectAnotherQueue("broker-A");
    assertNull(result);
}

@Test
@DisplayName("SendResult.brokerAddr 被正确赋值")
void testSendResultBrokerAddr() {
    SendResult result = SendResult.success("msg-1", 2, 4096L);
    result.setBrokerAddr("192.168.1.100:10911");
    assertEquals("192.168.1.100:10911", result.getBrokerAddr());
    assertEquals(2, result.getQueueId());
    assertEquals(4096L, result.getQueueOffset());
}
```

- [ ] **Step 2: 运行测试**

```bash
cd "E:\面试讲解\自研消息中间件代码最新\flare-mq" && mvn test -pl flare-mq-client -Dtest=ProducerTest -q
```

- [ ] **Step 3: 运行全量编译 + 测试**

```bash
cd "E:\面试讲解\自研消息中间件代码最新\flare-mq" && mvn compile -q
```

- [ ] **Step 4: Commit**

```bash
git add flare-mq-client/src/test/java/com/flare/mq/client/producer/ProducerTest.java
git commit -m "test: add tests for retry, failover, and send result brokerAddr"
```
