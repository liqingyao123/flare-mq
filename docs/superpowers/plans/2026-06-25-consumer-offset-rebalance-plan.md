# Consumer Offset 持久化与 Rebalance 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 Consumer offset 持久化到 Broker + Consumer Group 队列分配 + Rebalance 状态机，解决重启丢 offset 和多 consumer 并行消费问题。

**Architecture:** Broker 端新增 ConsumerOffsetManager（内存 + 定时刷 JSON）；NameServer 端新增 consumer 注册/心跳管理；Consumer 端新增 QueueAllocationManager（状态机 + coalesce + 确定性分配），ConsumerImpl 改造为多 queue 并发拉取 + offset 上报/恢复 + rebalance 回调。

**Tech Stack:** Java 8, Netty, JUnit 5 + Mockito, Jackson (JsonUtils), JDK ConcurrentHashMap/Atomic/ScheduledExecutor

## Global Constraints

- Java 8 兼容
- 不引入新的外部依赖
- Topic 仅存内存，不持久化
- Offset 持久化为 Broker 本地 JSON 文件（consumerOffset.json），每 5s 刷盘
- Rebalance 状态机：IDLE → REBALANCE_WAIT (3~10s coalesce) → REBALANCE_IN_PROGRESS → IDLE
- 队列分配为确定性算法（consumerId 排序 + 平均分）
- Consumer 启动随机 sleep 0~3s 错开注册
- Offset 只随 ACK 上报，没 ACK 不算已消费
- 最终一致性，不引入分布式事务

---

### Task 1: MessageType — 新增 8 个协议类型

**Files:**
- Modify: `flare-mq-protocol/src/main/java/com/ruyuan/mq/protocol/MessageType.java`

**Interfaces:**
- Produces: 8 个新枚举值供 NameServer、Broker、Consumer 的 switch 使用

- [ ] **Step 1: 添加枚举值**

在 `REGISTER_BROKER_RESPONSE((short) 39);` 之后（当前最后一个），`;` 之前添加：

```java
    // ========== 消费者相关 ==========
    /**
     * 消费者注册请求
     */
    CONSUMER_REGISTER_REQUEST((short) 50),

    /**
     * 消费者注册响应
     */
    CONSUMER_REGISTER_RESPONSE((short) 51),

    /**
     * 消费者心跳请求
     */
    CONSUMER_HEARTBEAT_REQUEST((short) 52),

    /**
     * 消费者心跳响应
     */
    CONSUMER_HEARTBEAT_RESPONSE((short) 53),

    // ========== Offset管理 ==========
    /**
     * 更新消费偏移量请求
     */
    UPDATE_CONSUMER_OFFSET_REQUEST((short) 44),

    /**
     * 更新消费偏移量响应
     */
    UPDATE_CONSUMER_OFFSET_RESPONSE((short) 45),

    /**
     * 查询消费偏移量请求
     */
    QUERY_CONSUMER_OFFSET_REQUEST((short) 46),

    /**
     * 查询消费偏移量响应
     */
    QUERY_CONSUMER_OFFSET_RESPONSE((short) 47);
```

- [ ] **Step 2: 更新 `isRequest()` 方法**

在现有的 `isRequest()` 返回表达式中，加入新的 4 个请求类型：

```java
               this == CONSUMER_REGISTER_REQUEST || this == CONSUMER_HEARTBEAT_REQUEST ||
               this == UPDATE_CONSUMER_OFFSET_REQUEST || this == QUERY_CONSUMER_OFFSET_REQUEST;
```

- [ ] **Step 3: 验证编译**

```bash
mvn compile -pl flare-mq-protocol -am -q
```
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add flare-mq-protocol/src/main/java/com/ruyuan/mq/protocol/MessageType.java
git commit -m "feat: add consumer register/heartbeat and offset management message types"
```

---

### Task 2: ServiceRegistry — 新增 Consumer 注册管理

**Files:**
- Modify: `flare-mq-nameserver/src/main/java/com/ruyuan/mq/nameserver/registry/ServiceRegistry.java`

**Interfaces:**
- Produces: `registerConsumer(group, consumerId, topics)` → `List<String>`; `heartbeatConsumer(group, consumerId)`; `unregisterConsumer(group, consumerId)`; `getConsumerIds(group)` → `List<String>`
- Produces inner class: `ConsumerHeartbeatData { String consumerId; String consumerGroup; long lastHeartbeatTime; List<String> topics; }`

- [ ] **Step 1: 添加字段和数据结构**

在 ServiceRegistry 字段区域（`topicRouteTable` 之后）添加：

```java
    // Consumer 注册信息: consumerGroup → (consumerId → heartbeatData)
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, ConsumerHeartbeatData>> consumerGroupTable;

    /**
     * Consumer 心跳数据
     */
    public static class ConsumerHeartbeatData {
        private final String consumerId;
        private final String consumerGroup;
        private volatile long lastHeartbeatTime;
        private final List<String> topics;

        public ConsumerHeartbeatData(String consumerId, String consumerGroup, List<String> topics) {
            this.consumerId = consumerId;
            this.consumerGroup = consumerGroup;
            this.topics = topics != null ? new ArrayList<>(topics) : new ArrayList<>();
            this.lastHeartbeatTime = System.currentTimeMillis();
        }

        public String getConsumerId() { return consumerId; }
        public String getConsumerGroup() { return consumerGroup; }
        public long getLastHeartbeatTime() { return lastHeartbeatTime; }
        public void setLastHeartbeatTime(long t) { this.lastHeartbeatTime = t; }
        public List<String> getTopics() { return topics; }
    }
```

在构造函数中初始化：

```java
        this.consumerGroupTable = new ConcurrentHashMap<>();
```

- [ ] **Step 2: 添加 `registerConsumer()` 方法**

```java
    /**
     * 注册 Consumer，返回同组所有 consumerId 列表（排序后）
     */
    public java.util.List<String> registerConsumer(String consumerGroup, String consumerId,
                                                    java.util.List<String> topics) {
        lock.writeLock().lock();
        try {
            consumerGroupTable.putIfAbsent(consumerGroup, new ConcurrentHashMap<>());
            ConcurrentHashMap<String, ConsumerHeartbeatData> group = consumerGroupTable.get(consumerGroup);
            boolean isNew = !group.containsKey(consumerId);
            group.put(consumerId, new ConsumerHeartbeatData(consumerId, consumerGroup, topics));

            // 返回排序后的 consumerId 列表（供确定性分配使用）
            java.util.List<String> ids = new java.util.ArrayList<>(group.keySet());
            java.util.Collections.sort(ids);

            logger.info("Consumer registered: group={}, consumerId={}, isNew={}, totalInGroup={}",
                       consumerGroup, consumerId, isNew, ids.size());
            return ids;
        } finally {
            lock.writeLock().unlock();
        }
    }
```

- [ ] **Step 3: 添加 `heartbeatConsumer()` 方法**

```java
    /**
     * Consumer 心跳
     */
    public boolean heartbeatConsumer(String consumerGroup, String consumerId) {
        ConcurrentHashMap<String, ConsumerHeartbeatData> group = consumerGroupTable.get(consumerGroup);
        if (group == null) return false;
        ConsumerHeartbeatData data = group.get(consumerId);
        if (data == null) return false;
        data.setLastHeartbeatTime(System.currentTimeMillis());
        return true;
    }
```

- [ ] **Step 4: 添加 `unregisterConsumer()` 和 `getConsumerIds()` 方法**

```java
    /**
     * 注销 Consumer
     */
    public void unregisterConsumer(String consumerGroup, String consumerId) {
        lock.writeLock().lock();
        try {
            ConcurrentHashMap<String, ConsumerHeartbeatData> group = consumerGroupTable.get(consumerGroup);
            if (group != null) {
                group.remove(consumerId);
                if (group.isEmpty()) {
                    consumerGroupTable.remove(consumerGroup);
                }
                logger.info("Consumer unregistered: group={}, consumerId={}", consumerGroup, consumerId);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取消费者组内所有 consumerId 列表（排序后，供 rebalance 使用）
     */
    public java.util.List<String> getConsumerIds(String consumerGroup) {
        ConcurrentHashMap<String, ConsumerHeartbeatData> group = consumerGroupTable.get(consumerGroup);
        if (group == null) return java.util.Collections.emptyList();
        java.util.List<String> ids = new java.util.ArrayList<>(group.keySet());
        java.util.Collections.sort(ids);
        return ids;
    }

    /**
     * 获取所有 consumerGroup，供 HealthChecker 扫描超时用
     */
    public java.util.Set<String> getAllConsumerGroups() {
        return new java.util.LinkedHashSet<>(consumerGroupTable.keySet());
    }

    /**
     * 获取指定 consumerGroup 内所有 Consumer 的心跳数据
     */
    public java.util.Map<String, ConsumerHeartbeatData> getConsumerHeartbeatData(String consumerGroup) {
        ConcurrentHashMap<String, ConsumerHeartbeatData> group = consumerGroupTable.get(consumerGroup);
        if (group == null) return java.util.Collections.emptyMap();
        return new java.util.concurrent.ConcurrentHashMap<>(group);
    }
```

- [ ] **Step 5: 在 `shutdown()` 中清理**

```java
            consumerGroupTable.clear();
```

- [ ] **Step 6: 验证编译**

```bash
mvn compile -pl flare-mq-nameserver -am -q
```
Expected: BUILD SUCCESS

- [ ] **Step 7: Commit**

```bash
git add flare-mq-nameserver/src/main/java/com/ruyuan/mq/nameserver/registry/ServiceRegistry.java
git commit -m "feat: add consumer registration and heartbeat management to ServiceRegistry"
```

---

### Task 3: NameServerRequestHandler — Consumer 注册/心跳 handler

**Files:**
- Modify: `flare-mq-nameserver/src/main/java/com/ruyuan/mq/nameserver/NameServerRequestHandler.java`

**Interfaces:**
- Consumes: `MessageType.CONSUMER_REGISTER_REQUEST/RESPONSE`, `MessageType.CONSUMER_HEARTBEAT_REQUEST/RESPONSE` (Task 1)
- Consumes: `serviceRegistry.registerConsumer()`, `heartbeatConsumer()`, `unregisterConsumer()` (Task 2)

- [ ] **Step 1: 在 switch 中添加两个新 case**

在 `handleRequest()` 的 switch 中，`REGISTER_BROKER_REQUEST` case 之后添加：

```java
                case CONSUMER_REGISTER_REQUEST:
                    return handleConsumerRegister(request);
                case CONSUMER_HEARTBEAT_REQUEST:
                    return handleConsumerHeartbeat(request);
```

- [ ] **Step 2: 实现 `handleConsumerRegister()`**

在 `handleRegisterBroker()` 方法之后添加：

```java
    /**
     * 处理 Consumer 注册请求
     */
    private ProtocolMessage handleConsumerRegister(ProtocolMessage request) {
        byte[] body = request.getBody();
        if (body == null || body.length == 0) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.CONSUMER_REGISTER_RESPONSE,
                    request.getRequestId(), ResponseCode.BAD_REQUEST);
        }

        String json = new String(body, StandardCharsets.UTF_8);
        ConsumerRegisterRequest req = JsonUtils.fromJson(json, ConsumerRegisterRequest.class);
        if (req == null || req.consumerGroup == null || req.consumerId == null) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.CONSUMER_REGISTER_RESPONSE,
                    request.getRequestId(), ResponseCode.BAD_REQUEST);
        }

        logger.info("Consumer registering: group={}, consumerId={}", req.consumerGroup, req.consumerId);

        java.util.List<String> topics = req.topics != null ? req.topics : java.util.Collections.emptyList();
        java.util.List<String> consumerIds = serviceRegistry.registerConsumer(
                req.consumerGroup, req.consumerId, topics);

        ConsumerRegisterResponse resp = new ConsumerRegisterResponse();
        resp.consumerIdList = consumerIds;

        String respJson = JsonUtils.toJson(resp);
        return ProtocolMessage.createSuccessResponse(
                MessageType.CONSUMER_REGISTER_RESPONSE,
                request.getRequestId(),
                respJson != null ? respJson.getBytes(StandardCharsets.UTF_8) : null);
    }
```

- [ ] **Step 3: 实现 `handleConsumerHeartbeat()`**

```java
    /**
     * 处理 Consumer 心跳请求
     */
    private ProtocolMessage handleConsumerHeartbeat(ProtocolMessage request) {
        byte[] body = request.getBody();
        if (body == null || body.length == 0) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.CONSUMER_HEARTBEAT_RESPONSE,
                    request.getRequestId(), ResponseCode.BAD_REQUEST);
        }

        String json = new String(body, StandardCharsets.UTF_8);
        ConsumerHeartbeatRequest req = JsonUtils.fromJson(json, ConsumerHeartbeatRequest.class);
        if (req == null || req.consumerGroup == null || req.consumerId == null) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.CONSUMER_HEARTBEAT_RESPONSE,
                    request.getRequestId(), ResponseCode.BAD_REQUEST);
        }

        serviceRegistry.heartbeatConsumer(req.consumerGroup, req.consumerId);
        return ProtocolMessage.createSuccessResponse(
                MessageType.CONSUMER_HEARTBEAT_RESPONSE,
                request.getRequestId(),
                "OK".getBytes(StandardCharsets.UTF_8));
    }
```

- [ ] **Step 4: 添加请求/响应 DTO**

在 DTO 区域（`RegisterBrokerResponse` 之后）添加：

```java
    static class ConsumerRegisterRequest {
        public String consumerGroup;
        public String consumerId;
        public java.util.List<String> topics;
    }

    static class ConsumerRegisterResponse {
        public java.util.List<String> consumerIdList;
    }

    static class ConsumerHeartbeatRequest {
        public String consumerGroup;
        public String consumerId;
    }
```

- [ ] **Step 5: 验证编译**

```bash
mvn compile -pl flare-mq-nameserver -am -q
```
Expected: BUILD SUCCESS

- [ ] **Step 6: Commit**

```bash
git add flare-mq-nameserver/src/main/java/com/ruyuan/mq/nameserver/NameServerRequestHandler.java
git commit -m "feat: add consumer register and heartbeat handlers to NameServer"
```

---

### Task 4: HealthChecker — Consumer 心跳超时清理

**Files:**
- Modify: `flare-mq-nameserver/src/main/java/com/ruyuan/mq/nameserver/health/HealthChecker.java`

**Interfaces:**
- Consumes: `serviceRegistry.getAllConsumerGroups()`, `getConsumerHeartbeatData()`, `unregisterConsumer()` (Task 2)

- [ ] **Step 1: 添加常量**

在现有常量区域：

```java
    private static final long CONSUMER_HEARTBEAT_TIMEOUT = 1000 * 60; // 60s Consumer 心跳超时
```

- [ ] **Step 2: 在 `scanNotActiveBroker()` 末尾添加 consumer 超时清理**

在 `scanNotActiveBroker()` 方法中，现有 broker 清理逻辑之后、`logger.debug` 之前添加：

```java
        // 清理心跳超时的 Consumer
        scanNotActiveConsumers(currentTime);
```

- [ ] **Step 3: 添加 `scanNotActiveConsumers()` 方法**

```java
    /**
     * 扫描并清理心跳超时的 Consumer
     */
    private void scanNotActiveConsumers(long currentTime) {
        try {
            java.util.Set<String> groups = serviceRegistry.getAllConsumerGroups();
            for (String group : groups) {
                java.util.Map<String, ServiceRegistry.ConsumerHeartbeatData> consumers =
                        serviceRegistry.getConsumerHeartbeatData(group);
                for (java.util.Map.Entry<String, ServiceRegistry.ConsumerHeartbeatData> entry : consumers.entrySet()) {
                    String consumerId = entry.getKey();
                    ServiceRegistry.ConsumerHeartbeatData data = entry.getValue();
                    if ((currentTime - data.getLastHeartbeatTime()) > CONSUMER_HEARTBEAT_TIMEOUT) {
                        logger.warn("Consumer heartbeat timeout, removing: group={}, consumerId={}", group, consumerId);
                        serviceRegistry.unregisterConsumer(group, consumerId);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error scanning not active consumers", e);
        }
    }
```

- [ ] **Step 4: 验证编译**

```bash
mvn compile -pl flare-mq-nameserver -am -q
```
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add flare-mq-nameserver/src/main/java/com/ruyuan/mq/nameserver/health/HealthChecker.java
git commit -m "feat: add consumer heartbeat timeout cleanup to HealthChecker"
```

---

### Task 5: ConsumerOffsetManager — Broker 端 Offset 管理

**Files:**
- Create: `flare-mq-broker/src/main/java/com/ruyuan/mq/broker/offset/ConsumerOffsetManager.java`

**Interfaces:**
- Produces: `updateOffset(group, topic, queueId, offset)`; `getOffset(group, topic, queueId)` → long; `persistOffsets()`; `loadOffsets()`
- Produces key format: `"{group}@{topic}@{queueId}"`

- [ ] **Step 1: 创建目录结构**

```bash
mkdir -p flare-mq-broker/src/main/java/com/ruyuan/mq/broker/offset
```

- [ ] **Step 2: 创建 ConsumerOffsetManager.java**

```java
package com.flare.mq.broker.offset;

import com.flare.mq.common.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.Map;

/**
 * Consumer Offset 管理器 — 按 (consumerGroup, topic, queueId) 存储消费偏移量
 * 定时刷盘到 consumerOffset.json
 */
public class ConsumerOffsetManager {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerOffsetManager.class);

    private final ConcurrentHashMap<String, Long> offsetTable;
    private final ScheduledExecutorService persistScheduler;
    private final File offsetFile;

    public ConsumerOffsetManager(String persistDir) {
        this.offsetTable = new ConcurrentHashMap<>();
        this.offsetFile = new File(persistDir, "consumerOffset.json");
        this.persistScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "OffsetPersist");
            t.setDaemon(true);
            return t;
        });

        loadOffsets();
        startPersistTask();
        logger.info("ConsumerOffsetManager initialized, offsetFile={}", offsetFile.getAbsolutePath());
    }

    /**
     * 更新 offset，只向前推进
     */
    public void updateOffset(String consumerGroup, String topic, int queueId, long offset) {
        String key = buildKey(consumerGroup, topic, queueId);
        offsetTable.merge(key, offset, Math::max);
    }

    /**
     * 查询 offset
     */
    public long getOffset(String consumerGroup, String topic, int queueId) {
        String key = buildKey(consumerGroup, topic, queueId);
        return offsetTable.getOrDefault(key, 0L);
    }

    /**
     * 获取所有 offset（供测试和监控使用）
     */
    public Map<String, Long> getAllOffsets() {
        return new ConcurrentHashMap<>(offsetTable);
    }

    // ===== 持久化 =====

    private void startPersistTask() {
        persistScheduler.scheduleWithFixedDelay(this::persistOffsets, 5, 5, TimeUnit.SECONDS);
    }

    private void persistOffsets() {
        try {
            if (offsetTable.isEmpty()) return;

            String json = JsonUtils.toJson(new ConcurrentHashMap<>(offsetTable));
            if (json == null) return;

            // 先写临时文件再 rename（原子写入）
            File tmpFile = new File(offsetFile.getParentFile(), "consumerOffset.json.tmp");
            try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
                fos.write(json.getBytes(StandardCharsets.UTF_8));
                fos.flush();
            }
            if (!tmpFile.renameTo(offsetFile)) {
                // rename 失败时直接 copy
                Files.move(tmpFile.toPath(), offsetFile.toPath(),
                        StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            }
            logger.debug("Persisted {} offset entries", offsetTable.size());
        } catch (Exception e) {
            logger.error("Failed to persist offsets", e);
        }
    }

    @SuppressWarnings("unchecked")
    private void loadOffsets() {
        if (!offsetFile.exists()) {
            logger.info("No existing offset file, starting fresh");
            return;
        }
        try {
            String json = new String(Files.readAllBytes(offsetFile.toPath()), StandardCharsets.UTF_8);
            Map<String, Object> loaded = JsonUtils.fromJson(json, Map.class);
            if (loaded != null) {
                for (Map.Entry<String, Object> entry : loaded.entrySet()) {
                    long value = entry.getValue() instanceof Number
                            ? ((Number) entry.getValue()).longValue() : 0L;
                    offsetTable.put(entry.getKey(), value);
                }
                logger.info("Loaded {} offset entries from {}", offsetTable.size(), offsetFile.getAbsolutePath());
            }
        } catch (Exception e) {
            logger.error("Failed to load offset file, starting fresh", e);
        }
    }

    // ===== 工具方法 =====

    private String buildKey(String consumerGroup, String topic, int queueId) {
        return consumerGroup + "@" + topic + "@" + queueId;
    }

    public void shutdown() {
        persistOffsets(); // 关闭前最后一次刷盘
        persistScheduler.shutdown();
        try {
            if (!persistScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                persistScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            persistScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("ConsumerOffsetManager shutdown complete");
    }
}
```

- [ ] **Step 3: 验证编译**

```bash
mvn compile -pl flare-mq-broker -am -q
```
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add flare-mq-broker/src/main/java/com/ruyuan/mq/broker/offset/ConsumerOffsetManager.java
git commit -m "feat: add ConsumerOffsetManager for broker-side offset persistence"
```

---

### Task 6: BrokerRequestHandler — Offset 处理 handler

**Files:**
- Modify: `flare-mq-broker/src/main/java/com/ruyuan/mq/broker/BrokerRequestHandler.java`

**Interfaces:**
- Consumes: `MessageType.UPDATE_CONSUMER_OFFSET_REQUEST/RESPONSE`, `MessageType.QUERY_CONSUMER_OFFSET_REQUEST/RESPONSE` (Task 1)
- Consumes: `ConsumerOffsetManager.updateOffset()`, `getOffset()` (Task 5)
- Modifies: 构造函数加入 `ConsumerOffsetManager` 参数

- [ ] **Step 1: 添加字段和修改构造函数**

添加字段：

```java
    private final com.flare.mq.broker.offset.ConsumerOffsetManager offsetManager;
```

修改构造函数，加入最后一个参数：

```java
    public BrokerRequestHandler(TopicManager topicManager,
                                QueueManager queueManager,
                                DefaultMessageStore messageStore,
                                com.flare.mq.broker.offset.ConsumerOffsetManager offsetManager) {
        this.topicManager = topicManager;
        this.queueManager = queueManager;
        this.messageStore = messageStore;
        this.offsetManager = offsetManager;
    }
```

- [ ] **Step 2: 在 switch 中添加两个新 case**

```java
                case UPDATE_CONSUMER_OFFSET_REQUEST:
                    return handleUpdateConsumerOffset(request);
                case QUERY_CONSUMER_OFFSET_REQUEST:
                    return handleQueryConsumerOffset(request);
```

- [ ] **Step 3: 实现两个 handler 方法**

在 `handleQueryTopic()` 之后添加：

```java
    private ProtocolMessage handleUpdateConsumerOffset(ProtocolMessage request) {
        String json = request.getBody() != null
                ? new String(request.getBody(), StandardCharsets.UTF_8) : null;
        UpdateOffsetRequest req = json != null
                ? JsonUtils.fromJson(json, UpdateOffsetRequest.class) : null;
        if (req == null || req.consumerGroup == null || req.topic == null) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.UPDATE_CONSUMER_OFFSET_RESPONSE,
                    request.getRequestId(), ResponseCode.BAD_REQUEST);
        }

        offsetManager.updateOffset(req.consumerGroup, req.topic, req.queueId, req.offset);
        return ProtocolMessage.createSuccessResponse(
                MessageType.UPDATE_CONSUMER_OFFSET_RESPONSE,
                request.getRequestId(),
                "OK".getBytes(StandardCharsets.UTF_8));
    }

    private ProtocolMessage handleQueryConsumerOffset(ProtocolMessage request) {
        String json = request.getBody() != null
                ? new String(request.getBody(), StandardCharsets.UTF_8) : null;
        QueryOffsetRequest req = json != null
                ? JsonUtils.fromJson(json, QueryOffsetRequest.class) : null;
        if (req == null || req.consumerGroup == null || req.topic == null) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.QUERY_CONSUMER_OFFSET_RESPONSE,
                    request.getRequestId(), ResponseCode.BAD_REQUEST);
        }

        long offset = offsetManager.getOffset(req.consumerGroup, req.topic, req.queueId);
        String payload = "{\"offset\":" + offset + "}";
        return ProtocolMessage.createSuccessResponse(
                MessageType.QUERY_CONSUMER_OFFSET_RESPONSE,
                request.getRequestId(),
                payload.getBytes(StandardCharsets.UTF_8));
    }
```

- [ ] **Step 4: 添加 DTO**

```java
    static class UpdateOffsetRequest { public String consumerGroup; public String topic; public int queueId; public long offset; }
    static class QueryOffsetRequest { public String consumerGroup; public String topic; public int queueId; }
```

- [ ] **Step 5: 验证编译**

```bash
mvn compile -pl flare-mq-broker -am -q
```
Expected: BUILD SUCCESS（注意：ClusterManager 构造 BrokerRequestHandler 处会报编译错误，需要 Task 7 修复）

- [ ] **Step 6: Commit**

```bash
git add flare-mq-broker/src/main/java/com/ruyuan/mq/broker/BrokerRequestHandler.java
git commit -m "feat: add offset update and query handlers to BrokerRequestHandler"
```

---

### Task 7: ClusterManager — 集成 ConsumerOffsetManager

**Files:**
- Modify: `flare-mq-broker/src/main/java/com/ruyuan/mq/broker/cluster/ClusterManager.java`

**Interfaces:**
- Consumes: `ConsumerOffsetManager` (Task 5), 修改后的 `BrokerRequestHandler` 构造函数 (Task 6)

- [ ] **Step 1: 创建 ConsumerOffsetManager 并传入 BrokerRequestHandler**

修改构造函数中的 BrokerRequestHandler 创建部分。将：

```java
        this.nettyServer = new NettyServer(port, new BrokerRequestHandler(topicManager, queueManager, messageStore));
```

替换为：

```java
        // 创建 Offset 管理器并注入 BrokerRequestHandler
        String persistDir = System.getProperty("user.dir") + "/data";
        new java.io.File(persistDir).mkdirs();
        com.flare.mq.broker.offset.ConsumerOffsetManager offsetManager =
                new com.flare.mq.broker.offset.ConsumerOffsetManager(persistDir);
        this.nettyServer = new NettyServer(port,
                new BrokerRequestHandler(topicManager, queueManager, messageStore, offsetManager));
```

- [ ] **Step 2: 在 shutdown() 中关闭 offsetManager**

在 `shutdown()` 方法中，在关闭 scheduledExecutor 之后、关闭 loadBalancer 之前：

```java
            // 关闭 offset 管理器
            if (offsetManager != null) {
                offsetManager.shutdown();
            }
```

但 offsetManager 是局部变量，需要改成字段。添加字段：

```java
    private com.flare.mq.broker.offset.ConsumerOffsetManager offsetManager;
```

修改构造函数中的创建行：

```java
        this.offsetManager = new com.flare.mq.broker.offset.ConsumerOffsetManager(persistDir);
```

- [ ] **Step 3: 验证编译**

```bash
mvn compile -pl flare-mq-broker -am -q
```
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add flare-mq-broker/src/main/java/com/ruyuan/mq/broker/cluster/ClusterManager.java
git commit -m "feat: integrate ConsumerOffsetManager into ClusterManager"
```

---

### Task 8: QueueAllocationManager — Consumer 端分配管理

**Files:**
- Create: `flare-mq-client/src/main/java/com/ruyuan/mq/client/consumer/QueueAllocationManager.java`

**Interfaces:**
- Consumes: `MessageType.CONSUMER_REGISTER_REQUEST/RESPONSE`, `MessageType.CONSUMER_HEARTBEAT_REQUEST/RESPONSE` (Task 1)
- Produces: `initialize()`, `getAllocatedQueueIds(topic)` → `List<Integer>`, `shutdown()`
- Produces callback: `void onRebalance(String topic, List<Integer> oldQueues, List<Integer> newQueues)`
- State machine: IDLE → REBALANCE_WAIT → REBALANCE_IN_PROGRESS → IDLE

- [ ] **Step 1: 创建 QueueAllocationManager.java**

```java
package com.flare.mq.client.consumer;

import com.flare.mq.protocol.client.NettyClient;
import com.flare.mq.protocol.ProtocolMessage;
import com.flare.mq.protocol.MessageType;
import com.flare.mq.protocol.ResponseCode;
import com.flare.mq.common.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 队列分配管理器 — Consumer 注册、心跳、Rebalance 状态机
 */
public class QueueAllocationManager {

    private static final Logger logger = LoggerFactory.getLogger(QueueAllocationManager.class);

    // ===== Rebalance 状态机状态 =====
    private enum State { IDLE, REBALANCE_WAIT, REBALANCE_IN_PROGRESS }

    private final String nameServerHost;
    private final int nameServerPort;
    private final String consumerGroup;
    private final String consumerId;
    private final List<String> topics;

    private NettyClient nameServerClient;
    private final ScheduledExecutorService scheduler;

    // 当前状态
    private final AtomicReference<State> state = new AtomicReference<>(State.IDLE);
    private volatile ScheduledFuture<?> rebalanceTimer;

    // 本地缓存：topic → 当前分配的 queueId 列表
    private final ConcurrentHashMap<String, List<Integer>> allocatedQueues;

    // Rebalance 回调（由 ConsumerImpl 设置）
    private volatile RebalanceListener rebalanceListener;

    public interface RebalanceListener {
        void onRebalance(String topic, List<Integer> oldQueues, List<Integer> newQueues);
    }

    public QueueAllocationManager(String nameServerHost, int nameServerPort,
                                   String consumerGroup, String consumerId, List<String> topics) {
        this.nameServerHost = nameServerHost;
        this.nameServerPort = nameServerPort;
        this.consumerGroup = consumerGroup;
        this.consumerId = consumerId;
        this.topics = new ArrayList<>(topics);
        this.allocatedQueues = new ConcurrentHashMap<>();
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "QueueAllocator-" + consumerId);
            t.setDaemon(false);
            return t;
        });
    }

    public void setRebalanceListener(RebalanceListener listener) {
        this.rebalanceListener = listener;
    }

    /**
     * 初始化：连接 NameServer → 随机 sleep → 注册 → 计算初始分配
     */
    public void initialize() throws Exception {
        nameServerClient = new NettyClient(nameServerHost, nameServerPort);
        nameServerClient.connect();

        // 随机 sleep 0~3s 错开多 consumer 同时启动
        int delay = new Random().nextInt(3000);
        logger.info("Random startup delay: {}ms", delay);
        Thread.sleep(delay);

        // 注册到 NameServer
        List<String> consumerIds = registerToNameServer();
        logger.info("Registered, group '{}' has {} consumers: {}", consumerGroup, consumerIds.size(), consumerIds);

        // 获取路由信息，计算初始分配
        for (String topic : topics) {
            int queueCount = fetchQueueCount(topic);
            if (queueCount > 0) {
                List<Integer> queues = calculateAllocation(topic, queueCount, consumerIds);
                allocatedQueues.put(topic, queues);
                logger.info("Initial allocation for topic '{}': queues={}", topic, queues);
            }
        }

        // 启动心跳（30s 间隔）
        scheduler.scheduleWithFixedDelay(this::sendHeartbeat, 30, 30, TimeUnit.SECONDS);

        // 启动 rebalance 检查（30s 间隔）
        scheduler.scheduleWithFixedDelay(this::checkRebalance, 30, 30, TimeUnit.SECONDS);
    }

    /**
     * 获取某 topic 当前分配的 queueId 列表
     */
    public List<Integer> getAllocatedQueueIds(String topic) {
        List<Integer> queues = allocatedQueues.get(topic);
        return queues != null ? new ArrayList<>(queues) : Collections.emptyList();
    }

    // ===== Rebalance 状态机 =====

    private void checkRebalance() {
        try {
            // 拉取最新 consumer 列表
            List<String> latestIds = fetchConsumerIds();
            if (latestIds.isEmpty()) return;

            // 与本地缓存对比（用 allocatedQueues 的 consumer 数推断）
            for (String topic : topics) {
                int queueCount = fetchQueueCount(topic);
                if (queueCount <= 0) continue;

                List<Integer> currentAllocation = allocatedQueues.get(topic);
                List<Integer> newAllocation = calculateMyAllocation(topic, queueCount, latestIds);

                if (!Objects.equals(currentAllocation, newAllocation)) {
                    logger.info("Rebalance needed for topic '{}': current={}, new={}, group={}",
                            topic, currentAllocation, newAllocation, latestIds);
                    triggerRebalance(topic, newAllocation);
                }
            }
        } catch (Exception e) {
            logger.error("Error in rebalance check", e);
        }
    }

    private void triggerRebalance(String topic, List<Integer> newQueues) {
        State currentState = state.get();
        if (currentState == State.REBALANCE_WAIT) {
            // Coalesce: 重置 timer
            cancelTimer();
        } else if (currentState == State.IDLE) {
            // 首次触发
            state.set(State.REBALANCE_WAIT);
        } else {
            // IN_PROGRESS: 当前正在执行，不打断（下次 check 会覆盖）
            return;
        }

        // 启动随机等待 timer (3~10s)
        int waitMs = 3000 + new Random().nextInt(7000);
        rebalanceTimer = scheduler.schedule(() -> executeRebalance(topic, newQueues),
                waitMs, TimeUnit.MILLISECONDS);
        logger.debug("Rebalance wait started for topic '{}': {}ms", topic, waitMs);
    }

    private void executeRebalance(String topic, List<Integer> newQueues) {
        state.set(State.REBALANCE_IN_PROGRESS);
        try {
            List<Integer> oldQueues = allocatedQueues.getOrDefault(topic, Collections.emptyList());
            allocatedQueues.put(topic, newQueues);

            logger.info("Rebalance executing for topic '{}': old={} → new={}", topic, oldQueues, newQueues);

            if (rebalanceListener != null) {
                rebalanceListener.onRebalance(topic, oldQueues, newQueues);
            }
        } finally {
            state.set(State.IDLE);
        }
    }

    private void cancelTimer() {
        ScheduledFuture<?> timer = rebalanceTimer;
        if (timer != null && !timer.isDone()) {
            timer.cancel(false);
        }
    }

    // ===== 确定性分配算法 =====

    /**
     * 按 consumerId 排序后平均分配 queueId，返回当前 consumer 负责的 queue 列表
     */
    public List<Integer> calculateMyAllocation(String topic, int queueCount,
                                                List<String> sortedConsumerIds) {
        int index = sortedConsumerIds.indexOf(this.consumerId);
        if (index < 0) return Collections.emptyList();

        int consumerCount = sortedConsumerIds.size();
        int base = queueCount / consumerCount;
        int remainder = queueCount % consumerCount;

        int start = index * base + Math.min(index, remainder);
        int count = base + (index < remainder ? 1 : 0);

        List<Integer> result = new ArrayList<>();
        for (int q = start; q < start + count; q++) {
            result.add(q);
        }
        return result;
    }

    // ===== NameServer 通信 =====

    private List<String> registerToNameServer() {
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("consumerGroup", consumerGroup);
        req.put("consumerId", consumerId);
        req.put("topics", topics);

        ProtocolMessage response = sendToNameServer(
                MessageType.CONSUMER_REGISTER_REQUEST, JsonUtils.toJson(req));
        if (response == null || response.getStatus() != ResponseCode.SUCCESS) return Collections.emptyList();

        String body = new String(response.getBody(), StandardCharsets.UTF_8);
        Map<String, Object> respMap = JsonUtils.fromJson(body, Map.class);
        if (respMap == null || !respMap.containsKey("consumerIdList")) return Collections.emptyList();

        @SuppressWarnings("unchecked")
        List<String> ids = (List<String>) respMap.get("consumerIdList");
        return ids != null ? ids : Collections.emptyList();
    }

    private List<String> fetchConsumerIds() {
        // 重新注册（轻量级，NameServer 返回当前组列表）
        return registerToNameServer();
    }

    private int fetchQueueCount(String topic) {
        try {
            String reqJson = "{\"topic\":\"" + topic + "\"}";
            ProtocolMessage response = sendToNameServer(
                    MessageType.GET_ROUTEINFO_BY_TOPIC_REQUEST, reqJson);
            if (response == null || response.getStatus() != ResponseCode.SUCCESS) return 0;

            String body = new String(response.getBody(), StandardCharsets.UTF_8);
            Map<String, Object> respMap = JsonUtils.fromJson(body, Map.class);
            if (respMap == null) return 0;

            @SuppressWarnings("unchecked")
            Map<String, Object> routeData = (Map<String, Object>) respMap.get("topicRouteData");
            if (routeData == null) return 0;

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> queueDatas = (List<Map<String, Object>>) routeData.get("queueDatas");
            if (queueDatas == null || queueDatas.isEmpty()) return 0;

            // 取第一个 broker 的 writeQueueNums
            Object nums = queueDatas.get(0).get("writeQueueNums");
            return nums instanceof Number ? ((Number) nums).intValue() : 0;
        } catch (Exception e) {
            logger.error("Failed to fetch queue count for topic: " + topic, e);
            return 0;
        }
    }

    private void sendHeartbeat() {
        try {
            Map<String, Object> req = new LinkedHashMap<>();
            req.put("consumerGroup", consumerGroup);
            req.put("consumerId", consumerId);
            sendToNameServer(MessageType.CONSUMER_HEARTBEAT_REQUEST, JsonUtils.toJson(req));
        } catch (Exception e) {
            logger.warn("Failed to send heartbeat for consumer: {}", consumerId, e);
        }
    }

    private ProtocolMessage sendToNameServer(MessageType type, String bodyJson) {
        try {
            if (nameServerClient == null || !nameServerClient.isConnected()) {
                nameServerClient = new NettyClient(nameServerHost, nameServerPort);
                nameServerClient.connect();
            }
            ProtocolMessage msg = new ProtocolMessage(type,
                    bodyJson != null ? bodyJson.getBytes(StandardCharsets.UTF_8) : null);
            return nameServerClient.sendSync(msg, 5000);
        } catch (Exception e) {
            logger.error("Failed to communicate with NameServer: type={}", type, e);
            return null;
        }
    }

    public void shutdown() {
        cancelTimer();
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        if (nameServerClient != null) {
            nameServerClient.disconnect();
        }
        logger.info("QueueAllocationManager shutdown for consumer: {}", consumerId);
    }
}
```

- [ ] **Step 2: 验证编译**

```bash
mvn compile -pl flare-mq-client -am -q
```
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flare-mq-client/src/main/java/com/ruyuan/mq/client/consumer/QueueAllocationManager.java
git commit -m "feat: add QueueAllocationManager with rebalance state machine"
```

---

### Task 9: ConsumerImpl 改造 — 多 queue 并发 + offset 上报恢复 + rebalance

**Files:**
- Modify: `flare-mq-client/src/main/java/com/ruyuan/mq/client/consumer/ConsumerImpl.java`

**Interfaces:**
- Consumes: `QueueAllocationManager` (Task 8), `MessageType.UPDATE_CONSUMER_OFFSET_REQUEST/RESPONSE`, `MessageType.QUERY_CONSUMER_OFFSET_REQUEST/RESPONSE` (Task 1)

- [ ] **Step 1: 添加 QueueAllocationManager 字段和 offset report scheduler**

在字段区域添加：

```java
    private QueueAllocationManager allocationManager;
    private ScheduledExecutorService offsetReportScheduler;
    // 每个 queue 的 pull 任务 handle，用于 rebalance 时取消
    private final ConcurrentHashMap<String, ScheduledFuture<?>> pullTasks = new ConcurrentHashMap<>();
```

- [ ] **Step 2: 重写 `start()` 方法**

替换现有的 `start()` 方法：

```java
    @Override
    public void start() throws Exception {
        if (status != ConsumerStatus.CREATE_JUST) {
            logger.warn("Consumer already started or closed, current status: {}", status);
            return;
        }
        try {
            logger.info("Starting Consumer: {}", config.getConsumerGroup());

            initNameServerClient();
            initConsumeExecutor();
            initPullScheduler();

            // 创建 QueueAllocationManager
            String[] nsParts = config.getNameServerAddr().split(":");
            String nsHost = nsParts[0];
            int nsPort = nsParts.length > 1 ? Integer.parseInt(nsParts[1]) : 9876;

            List<String> topicList = new ArrayList<>(subscriptions.keySet());
            String consumerId = config.getConsumerGroup() + "-" + UUID.randomUUID().toString().substring(0, 8);
            allocationManager = new QueueAllocationManager(nsHost, nsPort,
                    config.getConsumerGroup(), consumerId, topicList);
            allocationManager.setRebalanceListener(this::onRebalance);
            allocationManager.initialize();

            // 为初始分配的每个 queue 恢复 offset 并启动 pull
            for (String topic : topicList) {
                List<Integer> queues = allocationManager.getAllocatedQueueIds(topic);
                for (int queueId : queues) {
                    restoreAndStartPull(topic, queueId);
                }
            }

            // 启动 offset 上报定时任务（5s 间隔）
            offsetReportScheduler = Executors.newSingleThreadScheduledExecutor(r ->
                    new Thread(r, "OffsetReporter-" + consumerId));
            offsetReportScheduler.scheduleWithFixedDelay(
                    this::reportAllOffsets, 5, 5, TimeUnit.SECONDS);

            status = ConsumerStatus.RUNNING;
            logger.info("Consumer started: group={}, id={}, topics={}",
                    config.getConsumerGroup(), consumerId, topicList);
        } catch (Exception e) {
            status = ConsumerStatus.START_FAILED;
            logger.error("Consumer startup failed: " + config.getConsumerGroup(), e);
            throw e;
        }
    }
```

- [ ] **Step 3: 修改 `pullMessageForTopic()` — 改为 `pullMessageForQueue()`**

删除 `pullMessageForTopic(String topic)` 和 `startPullTaskForTopic(String topic)` 方法，替换为：

```java
    private void restoreAndStartPull(String topic, int queueId) {
        String progressKey = topic + "_" + queueId;
        long savedOffset = queryOffsetFromBroker(topic, queueId);
        consumeProgress.put(progressKey, savedOffset);
        startPullTaskForQueue(topic, queueId);
    }

    private void startPullTaskForQueue(String topic, int queueId) {
        String taskKey = topic + "_" + queueId;
        if (pullTasks.containsKey(taskKey)) return;

        ScheduledFuture<?> task = pullScheduler.scheduleWithFixedDelay(
                () -> pullMessageForQueue(topic, queueId),
                0,
                Math.max(1, config.getPullInterval()),
                TimeUnit.MILLISECONDS);
        pullTasks.put(taskKey, task);
        logger.info("Started pull task: topic={}, queueId={}", topic, queueId);
    }

    private void stopPullTaskForQueue(String topic, int queueId) {
        String taskKey = topic + "_" + queueId;
        ScheduledFuture<?> task = pullTasks.remove(taskKey);
        if (task != null) {
            task.cancel(false);
        }
    }

    private void pullMessageForQueue(String topic, int queueId) {
        try {
            SubscriptionData subscription = subscriptions.get(topic);
            if (subscription == null || !subscription.isEnabled()) return;

            String progressKey = topic + "_" + queueId;
            long offset = consumeProgress.getOrDefault(progressKey, 0L);

            PullResult pullResult = pullMessage(topic, queueId, offset, config.getPullBatchSize());

            if (pullResult.hasMessage()) {
                consumeProgress.put(progressKey, pullResult.getNextBeginOffset());
                consumeExecutor.submit(() -> consumeMessages(pullResult.getMessages(), subscription));
                logger.debug("Pulled: topic={}, queueId={}, offset={}, count={}, next={}",
                        topic, queueId, offset, pullResult.getMessageCount(), pullResult.getNextBeginOffset());
            }
        } catch (Exception e) {
            logger.error("Pull failed: topic={}, queueId={}", topic, queueId, e);
        }
    }
```

- [ ] **Step 4: 实现 Rebalance 回调**

```java
    /**
     * Rebalance 回调 — 限时等待旧 queue 处理完 → 释放 → 启动新 queue
     */
    private void onRebalance(String topic, List<Integer> oldQueues, List<Integer> newQueues) {
        logger.info("Rebalance for topic '{}': old={}, new={}", topic, oldQueues, newQueues);

        // 1. 停止旧 queue 的 pull 任务
        for (int qid : oldQueues) {
            if (!newQueues.contains(qid)) {
                stopPullTaskForQueue(topic, qid);
            }
        }

        // 2. 等待消费线程池处理完在途消息（最多 10s）
        try {
            consumeExecutor.shutdown();
            if (!consumeExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                consumeExecutor.shutdownNow();
                logger.warn("Rebalance timeout, forcing shutdown of in-flight messages for topic '{}'", topic);
            }
        } catch (InterruptedException e) {
            consumeExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // 3. 重新创建消费线程池
        initConsumeExecutor();

        // 4. 启动新 queue
        for (int qid : newQueues) {
            if (!oldQueues.contains(qid)) {
                restoreAndStartPull(topic, qid);
            }
        }

        logger.info("Rebalance complete for topic '{}': now consuming queues={}", topic, newQueues);
    }
```

- [ ] **Step 5: 实现 offset 上报和查询方法**

```java
    private long queryOffsetFromBroker(String topic, int queueId) {
        try {
            TopicRouteInfo routeInfo = getTopicRouteInfo(topic);
            if (routeInfo == null) return 0L;

            TopicRouteInfo.QueueInfo qi = routeInfo.getQueueInfos().stream()
                    .filter(q -> q.getQueueId() == queueId).findFirst().orElse(null);
            if (qi == null) return 0L;

            NettyClient brokerClient = getBrokerClient(qi.getBrokerName(), routeInfo);
            if (brokerClient == null) return 0L;

            String reqJson = String.format(
                    "{\"consumerGroup\":\"%s\",\"topic\":\"%s\",\"queueId\":%d}",
                    config.getConsumerGroup(), topic, queueId);
            ProtocolMessage request = new ProtocolMessage(
                    MessageType.QUERY_CONSUMER_OFFSET_REQUEST,
                    reqJson.getBytes(StandardCharsets.UTF_8));
            ProtocolMessage response = brokerClient.sendSync(request, 5000);

            if (response != null && response.getStatus() == ResponseCode.SUCCESS) {
                String body = new String(response.getBody(), StandardCharsets.UTF_8);
                Map<String, Object> respMap = JsonUtils.fromJson(body, Map.class);
                if (respMap != null && respMap.get("offset") instanceof Number) {
                    return ((Number) respMap.get("offset")).longValue();
                }
            }
        } catch (Exception e) {
            logger.error("Failed to query offset: topic={}, queueId={}", topic, queueId, e);
        }
        return 0L;
    }

    private void reportOffsetToBroker(String topic, int queueId, long offset) {
        try {
            TopicRouteInfo routeInfo = getTopicRouteInfo(topic);
            if (routeInfo == null) return;

            TopicRouteInfo.QueueInfo qi = routeInfo.getQueueInfos().stream()
                    .filter(q -> q.getQueueId() == queueId).findFirst().orElse(null);
            if (qi == null) return;

            NettyClient brokerClient = getBrokerClient(qi.getBrokerName(), routeInfo);
            if (brokerClient == null) return;

            String reqJson = String.format(
                    "{\"consumerGroup\":\"%s\",\"topic\":\"%s\",\"queueId\":%d,\"offset\":%d}",
                    config.getConsumerGroup(), topic, queueId, offset);
            ProtocolMessage request = new ProtocolMessage(
                    MessageType.UPDATE_CONSUMER_OFFSET_REQUEST,
                    reqJson.getBytes(StandardCharsets.UTF_8));
            brokerClient.sendSync(request, 3000);
        } catch (Exception e) {
            logger.warn("Failed to report offset: topic={}, queueId={}", topic, queueId, e);
        }
    }

    private void reportAllOffsets() {
        for (Map.Entry<String, Long> entry : consumeProgress.entrySet()) {
            String key = entry.getKey();
            int lastUnderscore = key.lastIndexOf('_');
            if (lastUnderscore < 0) continue;
            String topic = key.substring(0, lastUnderscore);
            int queueId = Integer.parseInt(key.substring(lastUnderscore + 1));
            reportOffsetToBroker(topic, queueId, entry.getValue());
        }
    }
```

- [ ] **Step 6: 修改 `consumeMessages()` — ACK 后上报 offset**

在 `consumeMessages()` 方法中，ACK 成功后：

```java
                if (status.isSuccess()) {
                    stats.recordConsumeSuccess(costTime, message.getMessageSize());
                    ackMessage(message.getMessageId());
                    // 上报 offset 到 Broker
                    String topic = message.getTopic();
                    String progressKey = topic + "_" + message.getQueueId();
                    long currentOffset = consumeProgress.getOrDefault(progressKey, 0L);
                    reportOffsetToBroker(topic, message.getQueueId(), currentOffset);
                }
```

- [ ] **Step 7: `shutdown()` 中加入 alloc 相关清理**

在 `shutdown()` 方法中，`pullScheduler.shutdown()` 之前：

```java
        if (offsetReportScheduler != null) {
            reportAllOffsets(); // 最后上报一次
            offsetReportScheduler.shutdown();
        }
        if (allocationManager != null) {
            allocationManager.shutdown();
        }
```

- [ ] **Step 8: 验证编译**

```bash
mvn compile -pl flare-mq-client -am -q
```
Expected: BUILD SUCCESS

- [ ] **Step 9: Commit**

```bash
git add flare-mq-client/src/main/java/com/ruyuan/mq/client/consumer/ConsumerImpl.java
git commit -m "feat: refactor ConsumerImpl for multi-queue pull, offset report/restore, and rebalance"
```

---

### Task 10: 集成测试

**Files:**
- Create: `flare-mq-test/src/test/java/com/ruyuan/mq/test/offset/ConsumerOffsetTest.java`
- Create: `flare-mq-test/src/test/java/com/ruyuan/mq/test/rebalance/RebalanceTest.java`

**Interfaces:**
- Consumes: 所有之前 Task 的实现

- [ ] **Step 1: 创建 ConsumerOffsetTest.java**

```java
package com.flare.mq.test.offset;

import com.flare.mq.broker.offset.ConsumerOffsetManager;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import java.io.File;

public class ConsumerOffsetTest {

    private ConsumerOffsetManager manager;
    private File testDir;

    @BeforeEach
    public void setUp() throws Exception {
        testDir = new File(System.getProperty("java.io.tmpdir"), "offset-test-" + System.nanoTime());
        testDir.mkdirs();
        manager = new ConsumerOffsetManager(testDir.getAbsolutePath());
    }

    @AfterEach
    public void tearDown() {
        manager.shutdown();
        for (File f : testDir.listFiles()) f.delete();
        testDir.delete();
    }

    @Test
    public void testUpdateAndGetOffset() {
        manager.updateOffset("group1", "topic1", 0, 100L);
        assertEquals(100L, manager.getOffset("group1", "topic1", 0));
    }

    @Test
    public void testOffsetOnlyMovesForward() {
        manager.updateOffset("group1", "topic1", 0, 100L);
        manager.updateOffset("group1", "topic1", 0, 50L);  // 后退
        assertEquals(100L, manager.getOffset("group1", "topic1", 0));
    }

    @Test
    public void testDefaultOffsetIsZero() {
        assertEquals(0L, manager.getOffset("nonexistent", "topic", 0));
    }

    @Test
    public void testMultipleQueues() {
        manager.updateOffset("g1", "t1", 0, 100L);
        manager.updateOffset("g1", "t1", 1, 200L);
        manager.updateOffset("g2", "t1", 0, 50L);
        assertEquals(100L, manager.getOffset("g1", "t1", 0));
        assertEquals(200L, manager.getOffset("g1", "t1", 1));
        assertEquals(50L, manager.getOffset("g2", "t1", 0));
    }

    @Test
    public void testPersistAndLoad() throws Exception {
        manager.updateOffset("g1", "t1", 0, 42L);
        manager.persistOffsets(); // 刷盘
        manager.shutdown();

        // 重新加载
        ConsumerOffsetManager manager2 = new ConsumerOffsetManager(testDir.getAbsolutePath());
        assertEquals(42L, manager2.getOffset("g1", "t1", 0));
        manager2.shutdown();
    }
}
```

- [ ] **Step 2: 创建 RebalanceTest.java**

```java
package com.flare.mq.test.rebalance;

import com.flare.mq.client.consumer.QueueAllocationManager;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.util.Arrays;
import java.util.List;

public class RebalanceTest {

    @Test
    public void testEqualDistribution() {
        // 8 queues, 2 consumers
        QueueAllocationManager mgr = new QueueAllocationManager("localhost", 9876, "g", "A", Arrays.asList("t"));
        List<Integer> queues = mgr.calculateMyAllocation("t", 8, Arrays.asList("A", "B"));
        assertEquals(Arrays.asList(0, 1, 2, 3), queues);
    }

    @Test
    public void testUnequalDistribution() {
        // 8 queues, 3 consumers
        QueueAllocationManager mgr = new QueueAllocationManager("localhost", 9876, "g", "A", Arrays.asList("t"));
        List<Integer> a = mgr.calculateMyAllocation("t", 8, Arrays.asList("A", "B", "C"));
        assertEquals(Arrays.asList(0, 1, 2), a); // 8/3 = 2余2, A拿3个

        mgr = new QueueAllocationManager("localhost", 9876, "g", "B", Arrays.asList("t"));
        List<Integer> b = mgr.calculateMyAllocation("t", 8, Arrays.asList("A", "B", "C"));
        assertEquals(Arrays.asList(3, 4, 5), b); // B拿3个

        mgr = new QueueAllocationManager("localhost", 9876, "g", "C", Arrays.asList("t"));
        List<Integer> c = mgr.calculateMyAllocation("t", 8, Arrays.asList("A", "B", "C"));
        assertEquals(Arrays.asList(6, 7), c); // C拿2个
    }

    @Test
    public void testConsumerMoreThanQueues() {
        // 2 queues, 5 consumers
        QueueAllocationManager mgr = new QueueAllocationManager("localhost", 9876, "g", "E", Arrays.asList("t"));
        List<Integer> e = mgr.calculateMyAllocation("t", 2, Arrays.asList("A", "B", "C", "D", "E"));
        assertTrue(e.isEmpty()); // 多出来的 consumer 拿不到 queue
    }

    @Test
    public void testSingleConsumer() {
        QueueAllocationManager mgr = new QueueAllocationManager("localhost", 9876, "g", "A", Arrays.asList("t"));
        List<Integer> queues = mgr.calculateMyAllocation("t", 8, Arrays.asList("A"));
        assertEquals(8, queues.size());
        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7), queues);
    }
}
```

- [ ] **Step 3: 运行测试**

```bash
mvn test -pl flare-mq-test -am -Dtest="ConsumerOffsetTest,RebalanceTest" -Dsurefire.failIfNoSpecifiedTests=false
```
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add flare-mq-test/src/test/java/com/ruyuan/mq/test/offset/ConsumerOffsetTest.java
git add flare-mq-test/src/test/java/com/ruyuan/mq/test/rebalance/RebalanceTest.java
git commit -m "test: add ConsumerOffsetManager and Rebalance tests"
```

---

### Task 11: 最终验证

- [ ] **Step 1: 运行全量编译**

```bash
mvn compile -q
```
Expected: BUILD SUCCESS, no warnings

- [ ] **Step 2: 运行全量测试**

```bash
mvn test -q
```
Expected: 所有新增测试 PASS；已有的 ClusterTest 6 个 pre-existing 错误不受影响

- [ ] **Step 3: 确认改动范围**

```bash
git diff --stat master...HEAD
```
