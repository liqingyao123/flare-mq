# ConsumerGroup 页面及消费进度监控 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Console 新增消费者组管理页面，展示每组的消费进度、队列堆积、消费者实例状态

**Architecture:** Broker 从 ConsumerOffsetManager + DefaultMessageStore 采集每队列消费进度 (30s) → NameServer 存储并合并心跳数据 → Console 通过 GET_CONSUMER_GROUPS_REQUEST 定时拉取 (5s) → Vue UI 展示

**Tech Stack:** Java 8, Netty, Spring Boot 3.2.5, Vue 3 CDN

## Global Constraints

- ConsumerOffsetManager key 格式: `consumerGroup@topic@queueId`
- Broker 每 30s 上报消费组统计，Console 每 5s 拉取
- `activeConsumers > 0` → ACTIVE，否则 INACTIVE
- NameServer 响应 JSON 字段命名使用 camelCase
- 前端使用 Vue 3 CDN，无构建步骤，单文件 HTML

---

### Task 1: 协议层 — MessageType 新增枚举值

**Files:**
- Modify: `ruyuan-mq-protocol/src/main/java/com/ruyuan/mq/protocol/MessageType.java`

**Interfaces:**
- Produces: `REPORT_CONSUMER_GROUP_STATS_REQUEST(72)`, `REPORT_CONSUMER_GROUP_STATS_RESPONSE(73)`, `GET_CONSUMER_GROUPS_REQUEST(74)`, `GET_CONSUMER_GROUPS_RESPONSE(75)`

- [ ] **Step 1: 在 GET_CLUSTER_STATS_RESPONSE 之后新增 4 个枚举值**

在 `GET_CLUSTER_STATS_RESPONSE((short) 71);` 之后、`;` 之前插入：

```java
    /**
     * 上报消费组统计请求
     */
    REPORT_CONSUMER_GROUP_STATS_REQUEST((short) 72),

    /**
     * 上报消费组统计响应
     */
    REPORT_CONSUMER_GROUP_STATS_RESPONSE((short) 73),

    /**
     * 获取消费组列表请求
     */
    GET_CONSUMER_GROUPS_REQUEST((short) 74),

    /**
     * 获取消费组列表响应
     */
    GET_CONSUMER_GROUPS_RESPONSE((short) 75);
```

- [ ] **Step 2: 在 isRequest() 方法中追加新枚举**

在 `isRequest()` 的 return 语句中，`this == GET_CLUSTER_STATS_REQUEST` 之后追加：

```java
               this == GET_CLUSTER_STATS_REQUEST ||
               this == REPORT_CONSUMER_GROUP_STATS_REQUEST ||
               this == GET_CONSUMER_GROUPS_REQUEST;
```

- [ ] **Step 3: 编译验证**

```bash
mvn compile -pl ruyuan-mq-protocol -q
```

- [ ] **Step 4: Commit**

```bash
git add ruyuan-mq-protocol/src/main/java/com/ruyuan/mq/protocol/MessageType.java
git commit -m "feat: add consumer group stats message types"
```

---

### Task 2: NameServer — ServiceRegistry 新增消费组统计存储

**Files:**
- Modify: `ruyuan-mq-nameserver/src/main/java/com/ruyuan/mq/nameserver/registry/ServiceRegistry.java`

**Interfaces:**
- Consumes: Broker 上报的 ConsumerGroupStats (from Task 4)
- Produces: `updateConsumerGroupStats(String brokerName, ConsumerGroupStats stats)`, `getAllConsumerGroupStats()`, `ConsumerGroupStats` inner class

- [ ] **Step 1: 新增 ConsumerGroupStats 内部类**

在 `ConsumerHeartbeatData` 内部类之后（约 line 58）新增：

```java
    /**
     * Broker 上报的消费组统计
     */
    public static class ConsumerGroupStats {
        private String groupName;
        private String topic;
        private long lastUpdateTimestamp;
        private double consumeTps;
        private java.util.List<QueueStat> queueStats = new java.util.ArrayList<>();

        public String getGroupName() { return groupName; }
        public void setGroupName(String v) { this.groupName = v; }
        public String getTopic() { return topic; }
        public void setTopic(String v) { this.topic = v; }
        public long getLastUpdateTimestamp() { return lastUpdateTimestamp; }
        public void setLastUpdateTimestamp(long v) { this.lastUpdateTimestamp = v; }
        public double getConsumeTps() { return consumeTps; }
        public void setConsumeTps(double v) { this.consumeTps = v; }
        public java.util.List<QueueStat> getQueueStats() { return queueStats; }
        public void setQueueStats(java.util.List<QueueStat> v) { this.queueStats = v; }
    }

    public static class QueueStat {
        private int queueId;
        private long maxOffset;
        private long consumedOffset;

        public int getQueueId() { return queueId; }
        public void setQueueId(int v) { this.queueId = v; }
        public long getMaxOffset() { return maxOffset; }
        public void setMaxOffset(long v) { this.maxOffset = v; }
        public long getConsumedOffset() { return consumedOffset; }
        public void setConsumedOffset(long v) { this.consumedOffset = v; }
    }
```

- [ ] **Step 2: 新增 consumerGroupStatsTable 字段及存取方法**

在构造函数 `consumerGroupTable` 初始化之后新增：

```java
// Broker 上报的消费组统计: brokerName → groupName → ConsumerGroupStats
private final ConcurrentHashMap<String, ConcurrentHashMap<String, ConsumerGroupStats>> consumerGroupStatsTable
        = new ConcurrentHashMap<>();
```

新增两个方法（在 `getAllConsumerGroups()` 之后）：

```java
    /**
     * 更新消费组统计（由 Broker 上报触发）
     */
    public void updateConsumerGroupStats(String brokerName, ConsumerGroupStats stats) {
        consumerGroupStatsTable.putIfAbsent(brokerName, new ConcurrentHashMap<>());
        stats.setLastUpdateTimestamp(System.currentTimeMillis());
        consumerGroupStatsTable.get(brokerName).put(stats.getGroupName(), stats);
        logger.debug("Updated consumer group stats: broker={}, group={}, topic={}, queues={}",
                brokerName, stats.getGroupName(), stats.getTopic(), stats.getQueueStats().size());
    }

    /**
     * 获取所有消费组统计（合并所有 Broker 的数据）
     */
    public java.util.List<ConsumerGroupStats> getAllConsumerGroupStats() {
        java.util.List<ConsumerGroupStats> result = new java.util.ArrayList<>();
        for (ConcurrentHashMap<String, ConsumerGroupStats> brokerStats : consumerGroupStatsTable.values()) {
            result.addAll(brokerStats.values());
        }
        return result;
    }
```

- [ ] **Step 3: shutdown 中清理新表**

在 `shutdown()` 方法中，`consumerGroupTable.clear()` 之后新增：

```java
            consumerGroupStatsTable.clear();
```

- [ ] **Step 4: 编译验证**

```bash
mvn compile -pl ruyuan-mq-nameserver -am -q
```

- [ ] **Step 5: Commit**

```bash
git add ruyuan-mq-nameserver/src/main/java/com/ruyuan/mq/nameserver/registry/ServiceRegistry.java
git commit -m "feat: add consumer group stats storage to ServiceRegistry"
```

---

### Task 3: NameServer — 新增两个请求处理器

**Files:**
- Modify: `ruyuan-mq-nameserver/src/main/java/com/ruyuan/mq/nameserver/NameServerRequestHandler.java`

**Interfaces:**
- Consumes: `ServiceRegistry.updateConsumerGroupStats()`, `ServiceRegistry.getAllConsumerGroupStats()`, `ServiceRegistry.getConsumerHeartbeatData()`, `ServiceRegistry.getAllConsumerGroups()`
- Produces: `handleReportConsumerGroupStats()`, `handleGetConsumerGroups()`

- [ ] **Step 1: 在 switch 中新增两个 case**

在 `case GET_CLUSTER_STATS_REQUEST:` 之后新增：

```java
                case REPORT_CONSUMER_GROUP_STATS_REQUEST:
                    return handleReportConsumerGroupStats(request);
                case GET_CONSUMER_GROUPS_REQUEST:
                    return handleGetConsumerGroups(request);
```

- [ ] **Step 2: 新增 handleReportConsumerGroupStats 方法**

在 `handleGetClusterStats()` 方法之后新增：

```java
    /**
     * 处理 Broker 上报的消费组统计信息
     */
    private ProtocolMessage handleReportConsumerGroupStats(ProtocolMessage request) {
        byte[] body = request.getBody();
        if (body == null || body.length == 0) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.REPORT_CONSUMER_GROUP_STATS_RESPONSE,
                    request.getRequestId(), ResponseCode.BAD_REQUEST);
        }

        String json = new String(body, StandardCharsets.UTF_8);
        @SuppressWarnings("unchecked")
        Map<String, Object> data = JsonUtils.fromJson(json, Map.class);
        if (data == null) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.REPORT_CONSUMER_GROUP_STATS_RESPONSE,
                    request.getRequestId(), ResponseCode.BAD_REQUEST);
        }

        String groupName = (String) data.get("groupName");
        String topic = (String) data.get("topic");
        String brokerName = (String) data.get("brokerName");

        if (groupName == null || topic == null || brokerName == null) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.REPORT_CONSUMER_GROUP_STATS_RESPONSE,
                    request.getRequestId(), ResponseCode.BAD_REQUEST);
        }

        ServiceRegistry.ConsumerGroupStats stats = new ServiceRegistry.ConsumerGroupStats();
        stats.setGroupName(groupName);
        stats.setTopic(topic);
        stats.setConsumeTps(data.get("consumeTps") instanceof Number
                ? ((Number) data.get("consumeTps")).doubleValue() : 0.0);

        @SuppressWarnings("unchecked")
        java.util.List<Map<String, Object>> rawQueueStats =
                (java.util.List<Map<String, Object>>) data.get("queueStats");
        if (rawQueueStats != null) {
            java.util.List<ServiceRegistry.QueueStat> queueStats = new java.util.ArrayList<>();
            for (Map<String, Object> qs : rawQueueStats) {
                ServiceRegistry.QueueStat q = new ServiceRegistry.QueueStat();
                q.setQueueId(qs.get("queueId") instanceof Number
                        ? ((Number) qs.get("queueId")).intValue() : 0);
                q.setMaxOffset(qs.get("maxOffset") instanceof Number
                        ? ((Number) qs.get("maxOffset")).longValue() : 0L);
                q.setConsumedOffset(qs.get("consumedOffset") instanceof Number
                        ? ((Number) qs.get("consumedOffset")).longValue() : 0L);
                queueStats.add(q);
            }
            stats.setQueueStats(queueStats);
        }

        serviceRegistry.updateConsumerGroupStats(brokerName, stats);

        return ProtocolMessage.createSuccessResponse(
                MessageType.REPORT_CONSUMER_GROUP_STATS_RESPONSE,
                request.getRequestId(),
                "OK".getBytes(StandardCharsets.UTF_8));
    }
```

- [ ] **Step 3: 新增 handleGetConsumerGroups 方法**

```java
    /**
     * 处理查询消费组列表请求（供 Console 使用）
     */
    private ProtocolMessage handleGetConsumerGroups(ProtocolMessage request) {
        try {
            java.util.List<ServiceRegistry.ConsumerGroupStats> allStats =
                    serviceRegistry.getAllConsumerGroupStats();

            java.util.List<Map<String, Object>> groupList = new java.util.ArrayList<>();
            java.util.Set<String> allGroupNames = new java.util.LinkedHashSet<>();
            allGroupNames.addAll(serviceRegistry.getAllConsumerGroups());

            java.util.Map<String, ServiceRegistry.ConsumerGroupStats> statsByGroup = new java.util.LinkedHashMap<>();
            for (ServiceRegistry.ConsumerGroupStats s : allStats) {
                statsByGroup.put(s.getGroupName(), s);
                allGroupNames.add(s.getGroupName());
            }

            for (String groupName : allGroupNames) {
                Map<String, Object> g = new LinkedHashMap<>();
                g.put("groupName", groupName);

                ServiceRegistry.ConsumerGroupStats brokerStats = statsByGroup.get(groupName);

                // 合并 Broker 上报的消费进度
                if (brokerStats != null) {
                    g.put("topic", brokerStats.getTopic());
                    g.put("consumeTps", brokerStats.getConsumeTps());
                    java.util.List<Map<String, Object>> qs = new java.util.ArrayList<>();
                    long totalConsumed = 0;
                    long totalLag = 0;
                    for (ServiceRegistry.QueueStat q : brokerStats.getQueueStats()) {
                        Map<String, Object> qm = new LinkedHashMap<>();
                        qm.put("queueId", q.getQueueId());
                        qm.put("maxOffset", q.getMaxOffset());
                        qm.put("consumedOffset", q.getConsumedOffset());
                        qm.put("lag", q.getMaxOffset() - q.getConsumedOffset());
                        qs.add(qm);
                        totalConsumed += q.getConsumedOffset();
                        totalLag += (q.getMaxOffset() - q.getConsumedOffset());
                    }
                    g.put("queueStats", qs);
                    g.put("totalConsumed", totalConsumed);
                    g.put("totalLag", totalLag);
                } else {
                    g.put("topic", "");
                    g.put("consumeTps", 0.0);
                    g.put("queueStats", java.util.Collections.emptyList());
                    g.put("totalConsumed", 0L);
                    g.put("totalLag", 0L);
                }

                // 合并消费者心跳数据
                Map<String, ConsumerHeartbeatData> consumers =
                        serviceRegistry.getConsumerHeartbeatData(groupName);
                int activeCount = 0;
                java.util.List<Map<String, Object>> consumerList = new java.util.ArrayList<>();
                for (ConsumerHeartbeatData hb : consumers.values()) {
                    Map<String, Object> cm = new LinkedHashMap<>();
                    cm.put("consumerId", hb.getConsumerId());
                    cm.put("lastHeartbeat", hb.getLastHeartbeatTime());
                    boolean alive = (System.currentTimeMillis() - hb.getLastHeartbeatTime()) < 30000L;
                    if (alive) activeCount++;
                    cm.put("alive", alive);
                    consumerList.add(cm);
                }
                g.put("consumerCount", consumers.size());
                g.put("activeConsumers", activeCount);
                g.put("consumers", consumerList);
                g.put("status", activeCount > 0 ? "ACTIVE" : "INACTIVE");

                groupList.add(g);
            }

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("consumerGroups", groupList);

            String json = JsonUtils.toJson(result);
            return ProtocolMessage.createSuccessResponse(
                    MessageType.GET_CONSUMER_GROUPS_RESPONSE,
                    request.getRequestId(),
                    json.getBytes(StandardCharsets.UTF_8));

        } catch (Exception e) {
            logger.error("Error handling get consumer groups request", e);
            return ProtocolMessage.createErrorResponse(
                    MessageType.GET_CONSUMER_GROUPS_RESPONSE,
                    request.getRequestId(), ResponseCode.INTERNAL_ERROR);
        }
    }
```

- [ ] **Step 4: 编译验证**

```bash
mvn compile -pl ruyuan-mq-nameserver -am -q
```

- [ ] **Step 5: Commit**

```bash
git add ruyuan-mq-nameserver/src/main/java/com/ruyuan/mq/nameserver/NameServerRequestHandler.java
git commit -m "feat: add consumer group stats report and query handlers to NameServer"
```

---

### Task 4: Broker — BrokerRegistration 注入 offsetManager 并上报消费组统计

**Files:**
- Modify: `ruyuan-mq-broker/src/main/java/com/ruyuan/mq/broker/registry/BrokerRegistration.java`
- Modify: `ruyuan-mq-broker/src/main/java/com/ruyuan/mq/broker/cluster/ClusterManager.java`
- Modify: `ruyuan-mq-store/src/main/java/com/ruyuan/mq/store/DefaultMessageStore.java`

**Interfaces:**
- Consumes: `ConsumerOffsetManager.getAllOffsets()`, `DefaultMessageStore.getMaxOffset(topic, queueId)`
- Produces: `setConsumerOffsetManager(ConsumerOffsetManager m)`, `reportConsumerGroupStats()` → sends REPORT_CONSUMER_GROUP_STATS_REQUEST

- [ ] **Step 1: DefaultMessageStore 新增 getAllQueueKeys 方法**

在 `ruyuan-mq-store/src/main/java/com/ruyuan/mq/store/DefaultMessageStore.java` 的 `getTotalMessageCount()` 之后新增：

```java
    /**
     * 获取所有已知队列的 (topic, queueId) 列表（供消费组统计采集）
     */
    public java.util.List<String[]> getAllQueueKeys() {
        return consumeQueueManager.getAllQueueKeys();
    }
```

- [ ] **Step 1b: ConsumeQueueManager 新增 getAllQueueKeys 方法**

在 `ruyuan-mq-store/src/main/java/com/ruyuan/mq/store/ConsumeQueueManager.java` 的 `getConsumeQueueCount()` 之后新增：

```java
    /**
     * 获取所有已知队列的 (topic, queueId) 列表
     */
    public java.util.List<String[]> getAllQueueKeys() {
        java.util.List<String[]> keys = new java.util.ArrayList<>();
        for (ConsumeQueue cq : consumeQueueTable.values()) {
            keys.add(new String[] { cq.getTopic(), String.valueOf(cq.getQueueId()) });
        }
        return keys;
    }
```

- [ ] **Step 2: BrokerRegistration 新增字段和 setter**

在 `private DefaultMessageStore messageStore;` 之后新增：

```java
    private com.ruyuan.mq.broker.offset.ConsumerOffsetManager offsetManager;
    private long lastReportedConsumed;
```

在 `setMessageStore()` 之后新增：

```java
    public void setConsumerOffsetManager(com.ruyuan.mq.broker.offset.ConsumerOffsetManager offsetManager) {
        this.offsetManager = offsetManager;
    }
```

- [ ] **Step 3: 在 registerBroker() 末尾新增消费组统计上报**

在 `registerBroker()` 方法中，metrics 设置代码块之后、`String requestJson` 之前新增：

```java
            // 上报消费组统计
            if (offsetManager != null && messageStore != null) {
                reportConsumerGroupStats();
            }
```

- [ ] **Step 4: 新增 reportConsumerGroupStats 方法**

在 `sendHeartbeat()` 方法之后新增：

```java
    /**
     * 采集消费组统计并上报到 NameServer
     */
    private void reportConsumerGroupStats() {
        if (offsetManager == null || messageStore == null) return;
        try {
            java.util.Map<String, Long> allOffsets = offsetManager.getAllOffsets();
            if (allOffsets.isEmpty()) return;

            // groupName → topic → queueId → consumedOffset
            java.util.Map<String, java.util.Map<String, java.util.Map<Integer, Long>>> grouped
                    = new java.util.LinkedHashMap<>();

            for (java.util.Map.Entry<String, Long> entry : allOffsets.entrySet()) {
                String key = entry.getKey();  // "group@topic@queueId"
                String[] parts = key.split("@", 3);
                if (parts.length != 3) continue;
                String groupName = parts[0];
                String topic = parts[1];
                int queueId;
                try { queueId = Integer.parseInt(parts[2]); } catch (NumberFormatException e) { continue; }
                long consumedOffset = entry.getValue();

                grouped.computeIfAbsent(groupName, g -> new java.util.LinkedHashMap<>())
                       .computeIfAbsent(topic, t -> new java.util.LinkedHashMap<>())
                       .put(queueId, consumedOffset);
            }

            long totalConsumed = 0;
            for (java.util.Map.Entry<String, java.util.Map<String, java.util.Map<Integer, Long>>> ge : grouped.entrySet()) {
                String groupName = ge.getKey();
                java.util.Map<String, java.util.Map<Integer, Long>> topicMap = ge.getValue();

                // 一组一个 topic（简化）
                for (java.util.Map.Entry<String, java.util.Map<Integer, Long>> te : topicMap.entrySet()) {
                    String topic = te.getKey();
                    java.util.Map<Integer, Long> queueMap = te.getValue();

                    java.util.List<java.util.Map<String, Object>> queueStats = new java.util.ArrayList<>();
                    long groupConsumed = 0;
                    for (java.util.Map.Entry<Integer, Long> qe : queueMap.entrySet()) {
                        int qid = qe.getKey();
                        long consumed = qe.getValue();
                        long maxOffset = messageStore.getMaxOffset(topic, qid);
                        groupConsumed += consumed;

                        java.util.Map<String, Object> qs = new java.util.LinkedHashMap<>();
                        qs.put("queueId", qid);
                        qs.put("maxOffset", maxOffset);
                        qs.put("consumedOffset", consumed);
                        queueStats.add(qs);
                    }
                    totalConsumed += groupConsumed;

                    java.util.Map<String, Object> report = new java.util.LinkedHashMap<>();
                    report.put("brokerName", this.brokerName);
                    report.put("groupName", groupName);
                    report.put("topic", topic);
                    report.put("consumeTps", calcConsumeTps(totalConsumed));
                    report.put("queueStats", queueStats);

                    String json = JsonUtils.toJson(report);
                    ProtocolMessage msg = new ProtocolMessage(
                            MessageType.REPORT_CONSUMER_GROUP_STATS_REQUEST,
                            json.getBytes(StandardCharsets.UTF_8));
                    nameServerClient.sendAsync(msg);
                }
            }

            lastReportedConsumed = totalConsumed;

        } catch (Exception e) {
            logger.warn("Failed to report consumer group stats: {}", e.getMessage());
        }
    }

    private double calcConsumeTps(long totalConsumed) {
        if (lastReportedConsumed <= 0 || lastRegisterTimestamp <= 0) return 0.0;
        double elapsed = (System.currentTimeMillis() - lastRegisterTimestamp) / 1000.0;
        long delta = totalConsumed - lastReportedConsumed;
        return elapsed > 0 ? Math.max(0, delta) / elapsed : 0.0;
    }
```

- [ ] **Step 5: ClusterManager 传递 offsetManager**

在 `ClusterManager.java` 的 `start()` 方法中，`brokerRegistration.setMessageStore(messageStore);` 之后新增：

```java
            brokerRegistration.setConsumerOffsetManager(offsetManager);
```

- [ ] **Step 6: 编译验证**

```bash
mvn compile -pl ruyuan-mq-broker -am -q
```

- [ ] **Step 7: Commit**

```bash
git add ruyuan-mq-broker/src/main/java/com/ruyuan/mq/broker/registry/BrokerRegistration.java
git add ruyuan-mq-broker/src/main/java/com/ruyuan/mq/broker/cluster/ClusterManager.java
git add ruyuan-mq-store/src/main/java/com/ruyuan/mq/store/DefaultMessageStore.java
git add ruyuan-mq-store/src/main/java/com/ruyuan/mq/store/ConsumeQueueManager.java
git commit -m "feat: report consumer group stats from broker to nameserver"
```

---

### Task 5: Console 模型 — 扩展 ConsumerGroupStatus

**Files:**
- Modify: `ruyuan-mq-console/src/main/java/com/ruyuan/mq/console/model/ConsumerGroupStatus.java`

**Interfaces:**
- Produces: `ConsumerGroupStatus` with new fields: `queues`, `consumers`, `activeConsumers`; new inner classes `QueueInfo`, `ConsumerInfo`

- [ ] **Step 1: 重写 ConsumerGroupStatus，新增内部类**

用以下内容替换整个文件：

```java
package com.ruyuan.mq.console.model;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 消费者组状态
 */
public class ConsumerGroupStatus {
    private String groupName;
    private String topic;
    private int consumerCount;
    private int activeConsumers;
    private long totalConsumed;
    private double consumeTps;
    private long totalLag;
    private String status;
    private LocalDateTime lastUpdateTime;
    private List<QueueInfo> queues;
    private List<ConsumerInfo> consumers;

    public ConsumerGroupStatus() {}

    // Getters and Setters
    public String getGroupName() { return groupName; }
    public void setGroupName(String v) { this.groupName = v; }
    public String getTopic() { return topic; }
    public void setTopic(String v) { this.topic = v; }
    public int getConsumerCount() { return consumerCount; }
    public void setConsumerCount(int v) { this.consumerCount = v; }
    public int getActiveConsumers() { return activeConsumers; }
    public void setActiveConsumers(int v) { this.activeConsumers = v; }
    public long getTotalConsumed() { return totalConsumed; }
    public void setTotalConsumed(long v) { this.totalConsumed = v; }
    public double getConsumeTps() { return consumeTps; }
    public void setConsumeTps(double v) { this.consumeTps = v; }
    public long getTotalLag() { return totalLag; }
    public void setTotalLag(long v) { this.totalLag = v; }
    public String getStatus() { return status; }
    public void setStatus(String v) { this.status = v; }
    public LocalDateTime getLastUpdateTime() { return lastUpdateTime; }
    public void setLastUpdateTime(LocalDateTime v) { this.lastUpdateTime = v; }
    public List<QueueInfo> getQueues() { return queues; }
    public void setQueues(List<QueueInfo> v) { this.queues = v; }
    public List<ConsumerInfo> getConsumers() { return consumers; }
    public void setConsumers(List<ConsumerInfo> v) { this.consumers = v; }

    public static class QueueInfo {
        private int queueId;
        private long maxOffset;
        private long consumedOffset;
        private long lag;

        public int getQueueId() { return queueId; }
        public void setQueueId(int v) { this.queueId = v; }
        public long getMaxOffset() { return maxOffset; }
        public void setMaxOffset(long v) { this.maxOffset = v; }
        public long getConsumedOffset() { return consumedOffset; }
        public void setConsumedOffset(long v) { this.consumedOffset = v; }
        public long getLag() { return lag; }
        public void setLag(long v) { this.lag = v; }
    }

    public static class ConsumerInfo {
        private String consumerId;
        private long lastHeartbeat;
        private boolean alive;

        public String getConsumerId() { return consumerId; }
        public void setConsumerId(String v) { this.consumerId = v; }
        public long getLastHeartbeat() { return lastHeartbeat; }
        public void setLastHeartbeat(long v) { this.lastHeartbeat = v; }
        public boolean isAlive() { return alive; }
        public void setAlive(boolean v) { this.alive = v; }
    }
}
```

- [ ] **Step 2: 编译验证**

```bash
mvn compile -pl ruyuan-mq-console -am -q
```

- [ ] **Step 3: Commit**

```bash
git add ruyuan-mq-console/src/main/java/com/ruyuan/mq/console/model/ConsumerGroupStatus.java
git commit -m "feat: extend ConsumerGroupStatus model with queue and consumer info"
```

---

### Task 6: Console 后端 — MonitorServiceImpl + MonitorController

**Files:**
- Modify: `ruyuan-mq-console/src/main/java/com/ruyuan/mq/console/service/impl/MonitorServiceImpl.java`
- Modify: `ruyuan-mq-console/src/main/java/com/ruyuan/mq/console/controller/MonitorController.java`

**Interfaces:**
- Consumes: NameServer GET_CONSUMER_GROUPS_RESPONSE JSON
- Produces: `List<ConsumerGroupStatus>` via `/api/consumers`

- [ ] **Step 1: MonitorServiceImpl 新增缓存字段**

在 `cachedConsumerGroupCount` 之后新增：

```java
    private List<Map<String, Object>> cachedConsumerGroups = Collections.emptyList();
```

- [ ] **Step 2: refreshSystemMetrics 中新增消费组拉取**

在 `refreshSystemMetrics()` 方法的 try 块中，`fetchClusterStats()` 之后新增：

```java
            fetchConsumerGroups();
```

- [ ] **Step 3: 新增 fetchConsumerGroups 方法**

在 `fetchClusterStats()` 方法之后新增：

```java
    private void fetchConsumerGroups() throws Exception {
        if (!connected || nettyClient == null || !nettyClient.isConnected()) {
            connectToNameServer();
            if (!connected) return;
        }

        ProtocolMessage request = new ProtocolMessage(
                MessageType.GET_CONSUMER_GROUPS_REQUEST, null);
        ProtocolMessage response = nettyClient.sendSync(request, 5000);

        if (response == null || response.getBody() == null) return;

        String json = new String(response.getBody(), StandardCharsets.UTF_8);
        @SuppressWarnings("unchecked")
        Map<String, Object> data = JsonUtils.fromJson(json, Map.class);
        if (data == null) return;

        synchronized (cacheLock) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> groups = (List<Map<String, Object>>) data.get("consumerGroups");
            cachedConsumerGroups = groups != null ? new ArrayList<>(groups) : Collections.emptyList();
        }
    }
```

- [ ] **Step 4: 重写 getConsumerGroupStatusList 方法**

替换现有的空返回实现：

```java
    @Override
    public List<ConsumerGroupStatus> getConsumerGroupStatusList() {
        List<Map<String, Object>> groups;
        synchronized (cacheLock) {
            groups = this.cachedConsumerGroups;
        }

        List<ConsumerGroupStatus> result = new ArrayList<>();
        for (Map<String, Object> g : groups) {
            ConsumerGroupStatus s = new ConsumerGroupStatus();
            s.setGroupName(g.get("groupName") != null ? (String) g.get("groupName") : "");
            s.setTopic(g.get("topic") != null ? (String) g.get("topic") : "");
            s.setConsumerCount(g.get("consumerCount") instanceof Number
                    ? ((Number) g.get("consumerCount")).intValue() : 0);
            s.setActiveConsumers(g.get("activeConsumers") instanceof Number
                    ? ((Number) g.get("activeConsumers")).intValue() : 0);
            s.setTotalConsumed(g.get("totalConsumed") instanceof Number
                    ? ((Number) g.get("totalConsumed")).longValue() : 0L);
            s.setConsumeTps(g.get("consumeTps") instanceof Number
                    ? ((Number) g.get("consumeTps")).doubleValue() : 0.0);
            s.setTotalLag(g.get("totalLag") instanceof Number
                    ? ((Number) g.get("totalLag")).longValue() : 0L);
            s.setStatus(g.get("status") != null ? (String) g.get("status") : "");

            @SuppressWarnings("unchecked")
            java.util.List<Map<String, Object>> rawQueues =
                    (java.util.List<Map<String, Object>>) g.get("queueStats");
            if (rawQueues != null) {
                java.util.List<ConsumerGroupStatus.QueueInfo> queues = new java.util.ArrayList<>();
                for (Map<String, Object> q : rawQueues) {
                    ConsumerGroupStatus.QueueInfo qi = new ConsumerGroupStatus.QueueInfo();
                    qi.setQueueId(q.get("queueId") instanceof Number
                            ? ((Number) q.get("queueId")).intValue() : 0);
                    qi.setMaxOffset(q.get("maxOffset") instanceof Number
                            ? ((Number) q.get("maxOffset")).longValue() : 0L);
                    qi.setConsumedOffset(q.get("consumedOffset") instanceof Number
                            ? ((Number) q.get("consumedOffset")).longValue() : 0L);
                    qi.setLag(q.get("lag") instanceof Number
                            ? ((Number) q.get("lag")).longValue() : 0L);
                    queues.add(qi);
                }
                s.setQueues(queues);
            }

            @SuppressWarnings("unchecked")
            java.util.List<Map<String, Object>> rawConsumers =
                    (java.util.List<Map<String, Object>>) g.get("consumers");
            if (rawConsumers != null) {
                java.util.List<ConsumerGroupStatus.ConsumerInfo> consumers = new java.util.ArrayList<>();
                for (Map<String, Object> c : rawConsumers) {
                    ConsumerGroupStatus.ConsumerInfo ci = new ConsumerGroupStatus.ConsumerInfo();
                    ci.setConsumerId(c.get("consumerId") != null ? (String) c.get("consumerId") : "");
                    ci.setLastHeartbeat(c.get("lastHeartbeat") instanceof Number
                            ? ((Number) c.get("lastHeartbeat")).longValue() : 0L);
                    ci.setAlive(c.get("alive") instanceof Boolean && (Boolean) c.get("alive"));
                    consumers.add(ci);
                }
                s.setConsumers(consumers);
            }

            result.add(s);
        }
        return result;
    }
```

- [ ] **Step 5: MonitorController 新增端点**

在 `MonitorController.java` 中，`getHealth()` 方法之后新增：

```java
    /** GET /api/consumers — 消费者组状态列表 */
    @GetMapping("/consumers")
    public List<ConsumerGroupStatus> getConsumerGroups() {
        return monitorService.getConsumerGroupStatusList();
    }
```

并在 import 中加入：

```java
import com.ruyuan.mq.console.model.ConsumerGroupStatus;
```

- [ ] **Step 6: 编译验证**

```bash
mvn compile -pl ruyuan-mq-console -am -q
```

- [ ] **Step 7: Commit**

```bash
git add ruyuan-mq-console/src/main/java/com/ruyuan/mq/console/service/impl/MonitorServiceImpl.java
git add ruyuan-mq-console/src/main/java/com/ruyuan/mq/console/controller/MonitorController.java
git commit -m "feat: add consumer group data fetching and API endpoint"
```

---

### Task 7: Console 前端 — Vue 消费者标签页

**Files:**
- Modify: `ruyuan-mq-console/src/main/resources/static/index.html`

**Interfaces:**
- Consumes: `GET /api/consumers` → `List<ConsumerGroupStatus>` JSON

- [ ] **Step 1: tabs 区域新增「消费者」按钮**

在 Topic 按钮之后新增：

```html
      <button class="tab" :class="{active: tab==='consumers'}" @click="switchTab('consumers')">消费者</button>
```

- [ ] **Step 2: content 区域新增消费者标签页 HTML**

在 Topic Tab 的 `</div>` 结束标签之后、`</div>` (content 结束) 之前新增：

```html
    <!-- ===== 消费者 Tab ===== -->
    <div v-if="tab==='consumers'">
      <table>
        <thead><tr><th>消费者组</th><th>Topic</th><th>消费者</th><th>堆积</th><th>消费 TPS</th><th>状态</th></tr></thead>
        <tbody>
          <tr v-for="(g,i) in consumerGroups" :key="i" @click="selectedConsumerGroup = g.groupName" style="cursor:pointer">
            <td><strong>{{g.groupName}}</strong></td>
            <td>{{g.topic||'-'}}</td>
            <td>{{g.activeConsumers}}/{{g.consumerCount}}</td>
            <td :style="{color: g.totalLag>1000?'#f5222d':'inherit'}">{{formatNum(g.totalLag)}}</td>
            <td>{{g.consumeTps?.toFixed(1)}}/s</td>
            <td><span class="status-dot" :class="g.status==='ACTIVE'?'online':'offline'"></span>{{g.status==='ACTIVE'?'活跃':'不活跃'}}</td>
          </tr>
        </tbody>
      </table>
      <div v-if="selectedConsumerGroupData" class="detail-bar">
        <div style="width:100%">
          <div style="font-weight:600;margin-bottom:8px;font-size:14px">队列消费进度</div>
          <table style="box-shadow:none;margin-bottom:12px">
            <thead><tr><th>Queue</th><th>MaxOffset</th><th>Consumed</th><th>Lag</th><th>进度</th></tr></thead>
            <tbody>
              <tr v-for="q in selectedConsumerGroupData.queues" :key="q.queueId">
                <td>{{q.queueId}}</td>
                <td>{{formatNum(q.maxOffset)}}</td>
                <td>{{formatNum(q.consumedOffset)}}</td>
                <td :style="{color: q.lag>500?'#f5222d':'inherit'}">{{formatNum(q.lag)}}</td>
                <td>{{q.maxOffset>0?(q.consumedOffset/q.maxOffset*100).toFixed(1)+'%':'0%'}}</td>
              </tr>
            </tbody>
          </table>
          <div style="font-weight:600;margin-bottom:8px;font-size:14px">消费者实例</div>
          <table style="box-shadow:none">
            <thead><tr><th>Consumer ID</th><th>最近心跳</th><th>状态</th></tr></thead>
            <tbody>
              <tr v-for="c in selectedConsumerGroupData.consumers" :key="c.consumerId">
                <td>{{c.consumerId}}</td>
                <td>{{c.lastHeartbeat>0?new Date(c.lastHeartbeat).toLocaleString():'-'}}</td>
                <td><span class="status-dot" :class="c.alive?'online':'offline'"></span>{{c.alive?'在线':'离线'}}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
```

- [ ] **Step 3: Vue data 中新增 consumerGroups**

在 `topics: []` 之后新增：

```javascript
      consumerGroups: [],
      selectedConsumerGroup: null,
```

- [ ] **Step 4: computed 中新增 selectedConsumerGroupData**

在 `selectedBrokerData()` 之后新增：

```javascript
    selectedConsumerGroupData() {
      return this.consumerGroups.find(g => g.groupName === this.selectedConsumerGroup);
    }
```

- [ ] **Step 5: loadTab 中新增 consumers 分支**

在 `else if (t==='topics')` 分支之后新增：

```javascript
      else if (t==='consumers') fetch('/api/consumers').then(r=>r.json()).then(d=>this.consumerGroups=d).catch(e=>console.error('Failed to load consumers:',e));
```

- [ ] **Step 6: switchTab 重置 selectedConsumerGroup**

在 `switchTab()` 方法中，`this.selectedBrokerName=null; this.selectedTopicName=null;` 之后新增：

```javascript
      this.selectedConsumerGroup=null;
```

- [ ] **Step 7: 编译打包验证**

```bash
mvn compile -pl ruyuan-mq-console -am -q
```

- [ ] **Step 8: Commit**

```bash
git add ruyuan-mq-console/src/main/resources/static/index.html
git commit -m "feat: add consumer group tab to console UI"
```
