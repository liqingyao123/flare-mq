# Console 真实监控数据 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 替换 Console 所有 mock 数据为来自 NameServer 的真实集群指标。

**Architecture:** Broker 注册时上报 CPU/内存/磁盘/消息数/TPS → NameServer 存储 → Console 通过 NettyClient 发送 GET_CLUSTER_STATS_REQUEST 定时获取。

**Tech Stack:** Java 17, Netty, Maven

## Global Constraints

- 不改变现有已有接口签名（MonitorService 接口保持不变）
- 降级：NameServer 不可达时所有指标返回 0，不抛异常
- Broker 采集指标用 JDK 自带 API（ManagementFactory, Runtime, File），无外部依赖
- NameServer 响应 JSON 格式必须与前端期望的字段名一致

---

### Task 1: 协议层 — MessageType + BrokerData

**Files:**
- Modify: `flare-mq-protocol/src/main/java/com/flare/mq/protocol/MessageType.java`
- Modify: `flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/registry/BrokerData.java`

**Interfaces:**
- Produces: `GET_CLUSTER_STATS_REQUEST(70)`, `GET_CLUSTER_STATS_RESPONSE(71)`
- Produces: BrokerData with cpuUsage, memoryUsage, diskUsage, totalMessages, currentTps fields + getter/setter

- [ ] **Step 1: MessageType 新增枚举值**

在 `MessageType.java` 枚举末尾 (`;` 之前) 新增：
```java
GET_CLUSTER_STATS_REQUEST(70),
GET_CLUSTER_STATS_RESPONSE(71);
```

- [ ] **Step 2: BrokerData 新增指标字段**

在 `BrokerData.java` 的 `lastUpdateTimestamp` 之后新增：
```java
private double cpuUsage;
private double memoryUsage;
private double diskUsage;
private long totalMessages;
private double currentTps;
```
以及对应的 getter/setter 方法。

- [ ] **Step 3: 编译验证**

```bash
mvn compile -pl flare-mq-nameserver -am -q
```

- [ ] **Step 4: Commit**

```bash
git add ... && git commit -m "feat: add cluster stats message type and broker metrics fields"
```

---

### Task 2: NameServer — ServiceRegistry + NameServerRequestHandler

**Files:**
- Modify: `flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/registry/ServiceRegistry.java:75-126`
- Modify: `flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/NameServerRequestHandler.java` (新增 handleGetClusterStats)

**Interfaces:**
- Consumes: BrokerData with metrics (from Task 1)
- Produces: NameServer handles GET_CLUSTER_STATS_REQUEST → returns JSON with brokers/topicCount/queueCount/consumerGroupCount

- [ ] **Step 1: ServiceRegistry.registerBroker() 解析并存储指标**

在 `registerBroker()` 中，`brokerData.setLastUpdateTimestamp()` 后新增指标更新逻辑。RegisterBrokerRequest 已有 cpuUsage 等字段（Task 3 新增上报），此处解析并存储到 BrokerData。

- [ ] **Step 2: NameServerRequestHandler 新增 handleGetClusterStats()**

在 switch 中新增 `case GET_CLUSTER_STATS_REQUEST: return handleGetClusterStats(request);`

实现 `handleGetClusterStats()`:
- 调用 `serviceRegistry.getAllBrokerData()` 获取所有 broker 数据
- 调用 `routeInfoManager.getStatistics()` 获取 topicCount/queueCount
- 调用 `serviceRegistry.getAllConsumerGroups().size()` 获取 consumerGroupCount
- 组装 JSON 返回

- [ ] **Step 3: 编译 + 测试验证**

```bash
mvn compile -pl flare-mq-nameserver -am -q
```

- [ ] **Step 4: Commit**

---

### Task 3: Broker 上报 — BrokerRegistration

**Files:**
- Modify: `flare-mq-broker/src/main/java/com/flare/mq/broker/registry/BrokerRegistration.java` (RegisterBrokerRequest DTO + registerBroker())

**Interfaces:**
- Consumes: Runtime/ManagementFactory metrics
- Produces: RegisterBrokerRequest with cpuUsage, memoryUsage, diskUsage, totalMessages, currentTps

- [ ] **Step 1: RegisterBrokerRequest DTO 新增字段**

```java
public double cpuUsage;
public double memoryUsage;
public double diskUsage;
public long totalMessages;
public double currentTps;
```

- [ ] **Step 2: registerBroker() 中采集指标并设置到 request**

```java
request.cpuUsage = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage()
        / Runtime.getRuntime().availableProcessors();
request.memoryUsage = 1.0 - (double)Runtime.getRuntime().freeMemory() / Runtime.getRuntime().totalMemory();
// disk: commitLog directory
java.io.File storeDir = new java.io.File(System.getProperty("user.home") + "/flare-mq-store");
request.diskUsage = 1.0 - (double)storeDir.getUsableSpace() / storeDir.getTotalSpace();
// totalMessages and currentTps kept as 0 for now
```

- [ ] **Step 3: 编译验证**

- [ ] **Step 4: Commit**

---

### Task 4: Console 重写 — MonitorServiceImpl + ConsoleApplication

**Files:**
- Rewrite: `flare-mq-console/src/main/java/com/flare/mq/console/service/impl/MonitorServiceImpl.java`
- Modify: `flare-mq-console/src/main/java/com/flare/mq/console/ConsoleApplication.java`

**Interfaces:**
- Consumes: NameServer GET_CLUSTER_STATS_RESPONSE JSON (via NettyClient)
- Produces: Real SystemOverview, List<BrokerStatus>, List<TopicStats>, ClusterHealth (via existing MonitorService interface)

- [ ] **Step 1: 重写 MonitorServiceImpl**

替换为：启动时连接 NameServer，定时每 5 秒发送 GET_CLUSTER_STATS_REQUEST，解析 JSON 更新内部缓存数据，getXxx() 从缓存返回。

核心逻辑：
- `start()`: 连接 NameServer + 首次拉取
- `refreshSystemMetrics()`: 发送 GET_CLUSTER_STATS_REQUEST，解析 JSON，更新 overview/brokers/topics
- `getSystemOverview()`: 从缓存转换
- `getBrokerStatusList()`: 从缓存转换
- `getTopicStatsList()`: 从缓存转换
- `getClusterHealth()`: 从缓存转换

- [ ] **Step 2: 修改 ConsoleApplication**

注入 NameServer 地址配置（默认 localhost:9876），传给 MonitorServiceImpl。

- [ ] **Step 3: 编译 + 打包 + 端到端验证**

```bash
mvn clean package -pl flare-mq-console -am -q -DskipTests
java -jar ... &
curl http://localhost:8080/api/overview
```

- [ ] **Step 4: Commit**
