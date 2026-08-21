# Broker 集群：命名、租约选举与故障转移 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让多 Broker 无需显式配置即自动组成一主多从集群；master 故障后 NameServer 自动按"消息 offset 最大者"选出新 master，并通过租约 + epoch 栅栏保证全程单主。

**Architecture:** NameServer 作为单一决策权威：`ServiceRegistry` 持有全局注册表（brokerId 0 槽位 = master 指针）与每个 broker 的 `totalMessages`（offset 代理）；新增 `MasterElectionManager` 负责租约检测、确定性选举、epoch 递增与 `BECOME_MASTER`/`STAND_DOWN` RPC 下发；broker 是被指挥的执行者，收到 `BECOME_MASTER` 后翻转本地角色并启动 Master 职能，心跳连续失败即主动停写。

**Tech Stack:** Java 8，Netty（协议通信），JUnit 5 + Mockito，Maven。

**Spec:** `docs/superpowers/specs/2026-08-22-broker-cluster-failover-design.md`

## Global Constraints

- 项目基于 **Java 8**，测试框架 **JUnit 5 + Mockito**，源码 **UTF-8**。
- 命名推导（可被 `-b / -i / -a` 覆盖）：`brokerName = host:port`（如 `127.0.0.1-10911`）；`brokerId = port - 10911`。
- **brokerId 0 = master**（`BrokerData.getMasterAddr()` = `brokerAddrs.get(0L)`，客户端据此解析 master，零改动）。
- 选举规则：候选里 `totalMessages` **降序**，平局 `brokerId` **升序**，取第一名。
- 租约：`masterLeaseDurationMs` 默认 **20000**；broker 心跳沿用 **10s**；心跳**连续 3 次失败** → 主动停写。
- epoch 从 **0** 起单调递增；携带旧 epoch 的 id0 注册被拒（栅栏）。
- 单一决策权威 = NameServer；failover 只保证"写路径可用 + 单主"，**不承诺数据零丢失**（复制是模拟的）。
- 复现/测试时命令：`mvn test -pl <module> -Dtest=<TestClass>`。

---

### Task 1: Broker 身份推导 + NameServer 地址传递

**Files:**
- Create: `flare-mq-broker/src/main/java/com/flare/mq/broker/cluster/BrokerIdentity.java`
- Modify: `flare-mq-broker/src/main/java/com/flare/mq/broker/BrokerStartup.java`（parseArgs 记录显式标记；main 用 BrokerIdentity 解析；createClusterConfig 传 nameServerAddr）
- Modify: `flare-mq-broker/src/main/java/com/flare/mq/broker/cluster/ClusterConfig.java`（新增 `nameServerAddr`、`dataDir` 字段）
- Modify: `flare-mq-broker/src/main/java/com/flare/mq/broker/cluster/ClusterManager.java:155`（去掉硬编码 `localhost:9876`；构造函数里 `persistDir` 改用 `config.getDataDir()`）
- Test: `flare-mq-broker/src/test/java/com/flare/mq/broker/cluster/BrokerIdentityTest.java`

**Interfaces:**
- Produces: `BrokerIdentity.resolve(String brokerAddr, String explicitName, boolean nameExplicit, Long explicitId, boolean idExplicit)` → 返回 `brokerName` 与 `brokerId`。
- Produces: `ClusterConfig.getNameServerAddr()`、`getDataDir()`、`setNameServerAddr(String)`、`setDataDir(String)`。

- [ ] **Step 1: 写失败测试 `BrokerIdentityTest`**

```java
package com.flare.mq.broker.cluster;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class BrokerIdentityTest {

    @Test
    public void testDeriveFromAddress() {
        BrokerIdentity id = BrokerIdentity.resolve("127.0.0.1:10911", null, false, null, false);
        assertEquals("127.0.0.1-10911", id.brokerName);
        assertEquals(0L, id.brokerId);
    }

    @Test
    public void testDeriveIdFromPort() {
        BrokerIdentity id = BrokerIdentity.resolve("127.0.0.1:10913", null, false, null, false);
        assertEquals(2L, id.brokerId);
    }

    @Test
    public void testExplicitOverrides() {
        BrokerIdentity id = BrokerIdentity.resolve("127.0.0.1:10912", "broker-x", true, 7L, true);
        assertEquals("broker-x", id.brokerName);
        assertEquals(7L, id.brokerId);
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `mvn test -pl flare-mq-broker -Dtest=BrokerIdentityTest`
Expected: FAIL —— `BrokerIdentity` 类不存在，编译错误。

- [ ] **Step 3: 创建 `BrokerIdentity`**

```java
package com.flare.mq.broker.cluster;

/**
 * Broker 身份解析：brokerName 默认 host-port，brokerId 默认 port-10911。
 * 两者均可被显式配置覆盖，保证多 broker 默认启动即唯一。
 */
public class BrokerIdentity {

    public final String brokerName;
    public final long brokerId;

    private BrokerIdentity(String brokerName, long brokerId) {
        this.brokerName = brokerName;
        this.brokerId = brokerId;
    }

    public static BrokerIdentity resolve(String brokerAddr, String explicitName,
                                         boolean nameExplicit, Long explicitId, boolean idExplicit) {
        String host = brokerAddr.split(":")[0];
        int port = Integer.parseInt(brokerAddr.split(":")[1]);
        String name = nameExplicit ? explicitName : host + "-" + port;
        long id = idExplicit ? explicitId : (long) port - 10911;
        return new BrokerIdentity(name, id);
    }
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `mvn test -pl flare-mq-broker -Dtest=BrokerIdentityTest`
Expected: PASS。

- [ ] **Step 5: 修改 `BrokerStartup`（记录显式标记并解析身份）**

在 `BrokerConfig` 类中新增两个布尔标记：

```java
private boolean brokerNameExplicit = false;
private boolean brokerIdExplicit = false;
public boolean isBrokerNameExplicit() { return brokerNameExplicit; }
public boolean isBrokerIdExplicit() { return brokerIdExplicit; }
```

在 `parseArgs` 的 `-b` / `-i` 分支中置位：

```java
case "-b":
case "--broker":
    if (i + 1 < args.length) {
        config.setBrokerName(args[++i]);
        config.brokerNameExplicit = true;   // 同文件内可访问私有字段
    }
    break;
case "-i":
case "--id":
    if (i + 1 < args.length) {
        config.setBrokerId(Long.parseLong(args[++i]));
        config.brokerIdExplicit = true;
    }
    break;
```

在 `main` 中创建 `ClusterManager` 前解析身份：

```java
BrokerIdentity identity = BrokerIdentity.resolve(
        config.getBrokerAddr(),
        config.getBrokerName(), config.isBrokerNameExplicit(),
        config.getBrokerId(), config.isBrokerIdExplicit());
config.setBrokerName(identity.brokerName);
config.setBrokerId(identity.brokerId);
clusterManager = new ClusterManager(
        config.getClusterName(), config.getBrokerName(), clusterConfig);
```

- [ ] **Step 6: 修改 `ClusterConfig`（新增两个字段）**

在字段区新增：

```java
private String nameServerAddr = "localhost:9876";
private String dataDir = System.getProperty("user.dir") + "/data";

public String getNameServerAddr() { return nameServerAddr; }
public void setNameServerAddr(String nameServerAddr) { this.nameServerAddr = nameServerAddr; }
public String getDataDir() { return dataDir; }
public void setDataDir(String dataDir) { this.dataDir = dataDir; }
```

- [ ] **Step 7: 修改 `ClusterManager`（用配置而非硬编码）**

`ClusterManager.start()` 中替换：

```java
// 之前
String nameServerAddr = "localhost:9876"; // TODO: 从配置中获取
// 之后
String nameServerAddr = clusterConfig.getNameServerAddr();
```

构造函数中替换：

```java
// 之前
String persistDir = System.getProperty("user.dir") + "/data";
// 之后
String persistDir = clusterConfig.getDataDir();
```

并在 `createClusterConfig`（BrokerStartup 中）补传：

```java
clusterConfig.setNameServerAddr(brokerConfig.getNameServerAddr());
```

- [ ] **Step 8: 编译并跑既有测试**

Run: `mvn test -pl flare-mq-broker -Dtest=BrokerIdentityTest,ClusterTest,BrokerSendMessageTest`
Expected: PASS。

- [ ] **Step 9: 提交**

```bash
git add flare-mq-broker/src/main/java/com/flare/mq/broker/cluster/BrokerIdentity.java \
        flare-mq-broker/src/test/java/com/flare/mq/broker/cluster/BrokerIdentityTest.java \
        flare-mq-broker/src/main/java/com/flare/mq/broker/BrokerStartup.java \
        flare-mq-broker/src/main/java/com/flare/mq/broker/cluster/ClusterConfig.java \
        flare-mq-broker/src/main/java/com/flare/mq/broker/cluster/ClusterManager.java
git commit -m "feat(broker): 自动推导 brokerName/brokerId 并透传 nameServerAddr"
```

---

### Task 2: 心跳携带身份，NameServer 刷新存活

**Files:**
- Modify: `flare-mq-broker/src/main/java/com/flare/mq/broker/registry/BrokerRegistration.java`（`sendHeartbeat` 携带身份 JSON）
- Modify: `flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/NameServerRequestHandler.java`（构造函数新增 `HealthChecker`；HEARTBEAT 分支解析身份并刷新存活）
- Modify: `flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/NameServerController.java:47`（构造 handler 时传入 `healthChecker`）
- Test: `flare-mq-nameserver/src/test/java/com/flare/mq/nameserver/NameServerHeartbeatTest.java`

**Interfaces:**
- Consumes: `HealthChecker.processBrokerHeartbeat(String clusterName, String brokerAddr, String brokerName, long brokerId, long timeoutMillis)`（已存在）。
- Produces: `NameServerRequestHandler(ServiceDiscovery, ServiceRegistry, RouteInfoManager, HealthChecker)` 新构造函数。

- [ ] **Step 1: 写失败测试 `NameServerHeartbeatTest`**

```java
package com.flare.mq.nameserver;

import com.flare.mq.nameserver.health.HealthChecker;
import com.flare.mq.nameserver.registry.ServiceDiscovery;
import com.flare.mq.nameserver.registry.ServiceRegistry;
import com.flare.mq.nameserver.route.RouteInfoManager;
import com.flare.mq.protocol.MessageType;
import com.flare.mq.protocol.ProtocolMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class NameServerHeartbeatTest {

    @Test
    public void testHeartbeatRefreshesBrokerLiveness() {
        ServiceRegistry registry = new ServiceRegistry();
        HealthChecker checker = new HealthChecker(registry);
        NameServerRequestHandler handler = new NameServerRequestHandler(
                new ServiceDiscovery(registry), registry, new RouteInfoManager(), checker);

        // 先注册 broker，使其存在于注册表
        registry.registerBroker("DefaultCluster", "127.0.0.1:20911", "127.0.0.1-20911", 0L,
                "127.0.0.1:20912", null, null, false);
        registry.getBrokerData("127.0.0.1-20911").setLastUpdateTimestamp(0L); // 故意置旧

        // 携带身份的 broker 心跳
        String body = "{\"clusterName\":\"DefaultCluster\",\"brokerName\":\"127.0.0.1-20911\","
                + "\"brokerAddr\":\"127.0.0.1:20911\",\"brokerId\":0}";
        ProtocolMessage hb = new ProtocolMessage(MessageType.HEARTBEAT_REQUEST,
                body.getBytes(StandardCharsets.UTF_8));

        handler.handleRequest(null, hb);

        assertTrue(registry.getBrokerData("127.0.0.1-20911").getLastUpdateTimestamp() > 0L);
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `mvn test -pl flare-mq-nameserver -Dtest=NameServerHeartbeatTest`
Expected: FAIL —— 构造函数签名不匹配（无 HealthChecker 参数）或 lastUpdate 未被刷新。

- [ ] **Step 3: 修改 `NameServerRequestHandler`**

构造函数与字段：

```java
private final HealthChecker healthChecker;

public NameServerRequestHandler(ServiceDiscovery serviceDiscovery, ServiceRegistry serviceRegistry,
                                RouteInfoManager routeInfoManager, HealthChecker healthChecker) {
    this.serviceDiscovery = serviceDiscovery;
    this.serviceRegistry = serviceRegistry;
    this.routeInfoManager = routeInfoManager;
    this.healthChecker = healthChecker;
}
```

HEARTBEAT 分支替换（原来只回空响应）：

```java
if (request.getType() == MessageType.HEARTBEAT_REQUEST) {
    handleBrokerHeartbeat(request);
    return ProtocolMessage.createHeartbeatResponse(request.getRequestId());
}
```

新增方法：

```java
private void handleBrokerHeartbeat(ProtocolMessage request) {
    byte[] body = request.getBody();
    if (body == null || body.length == 0) return;   // 空心跳仅保活
    try {
        String json = new String(body, StandardCharsets.UTF_8);
        Map<String, Object> map = JsonUtils.fromJson(json, Map.class);
        if (map == null) return;
        Object cluster = map.get("clusterName");
        Object brokerAddr = map.get("brokerAddr");
        Object brokerName = map.get("brokerName");
        Object brokerId = map.get("brokerId");
        if (cluster == null || brokerAddr == null || brokerName == null || brokerId == null) return;
        healthChecker.processBrokerHeartbeat(
                String.valueOf(cluster),
                String.valueOf(brokerAddr),
                String.valueOf(brokerName),
                ((Number) brokerId).longValue(),
                1000 * 30);
    } catch (Exception e) {
        logger.warn("Failed to parse broker heartbeat: {}", e.getMessage());
    }
}
```

（`Map` / `JsonUtils` 已在文件顶部 import；确认 `java.util.Map` 已引入，未引入则补。）

- [ ] **Step 4: 修改 `NameServerController` 构造 handler**

```java
NameServerRequestHandler requestHandler = new NameServerRequestHandler(
        serviceDiscovery, serviceRegistry, routeInfoManager, healthChecker);
```

- [ ] **Step 5: 修改 `BrokerRegistration.sendHeartbeat` 携带身份**

```java
private void sendHeartbeat() {
    if (!running || nameServerClient == null || !nameServerClient.isConnected()) {
        return;
    }
    try {
        Map<String, Object> hb = new LinkedHashMap<>();
        hb.put("clusterName", clusterName);
        hb.put("brokerName", brokerName);
        hb.put("brokerAddr", brokerAddr);
        hb.put("brokerId", brokerId);
        ProtocolMessage heartbeat = new ProtocolMessage(
                MessageType.HEARTBEAT_REQUEST,
                JsonUtils.toJson(hb).getBytes(StandardCharsets.UTF_8));
        ProtocolMessage response = nameServerClient.sendSync(heartbeat, 3000);
        if (response == null || response.getStatus().getCode() != 0) {
            logger.warn("Heartbeat failed to NameServer: brokerName={}", brokerName);
        }
    } catch (Exception e) {
        logger.warn("Error sending heartbeat to NameServer: brokerName=" + brokerName, e);
    }
}
```

- [ ] **Step 6: 运行测试确认通过**

Run: `mvn test -pl flare-mq-nameserver -Dtest=NameServerHeartbeatTest`
Expected: PASS。

- [ ] **Step 7: 提交**

```bash
git add flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/NameServerRequestHandler.java \
        flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/NameServerController.java \
        flare-mq-nameserver/src/test/java/com/flare/mq/nameserver/NameServerHeartbeatTest.java \
        flare-mq-broker/src/main/java/com/flare/mq/broker/registry/BrokerRegistration.java
git commit -m "feat(cluster): broker 心跳携带身份，NameServer 刷新存活"
```

---

### Task 3: MasterElectionManager 核心（选举 + epoch + 提升）

**Files:**
- Create: `flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/cluster/MasterElectionManager.java`
- Test: `flare-mq-nameserver/src/test/java/com/flare/mq/nameserver/cluster/MasterElectionManagerTest.java`

**Interfaces:**
- Produces: `BrokerData electNewMaster(List<BrokerData> candidates)`（纯函数）
- Produces: `long getEpoch()`
- Produces: `long promoteToMaster(BrokerData newMaster)`（把新 master 地址写到 id0 槽位并 `epoch++`，返回新 epoch）
- Produces: `long getLeaseDurationMs()`

- [ ] **Step 1: 写失败测试 `MasterElectionManagerTest`**

```java
package com.flare.mq.nameserver.cluster;

import com.flare.mq.nameserver.registry.BrokerData;
import com.flare.mq.nameserver.registry.ServiceRegistry;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class MasterElectionManagerTest {

    private BrokerData broker(String name, long totalMessages, long brokerId) {
        BrokerData d = new BrokerData("DefaultCluster", name);
        d.setTotalMessages(totalMessages);
        d.getBrokerAddrs().put(brokerId, "127.0.0.1:" + (10911 + brokerId));
        return d;
    }

    @Test
    public void testLargestOffsetWins() {
        BrokerData b = broker("b", 100, 1);
        BrokerData c = broker("c", 50, 2);
        BrokerData winner = MasterElectionManager.electNewMaster(Arrays.asList(c, b));
        assertEquals("b", winner.getBrokerName());
    }

    @Test
    public void testTieBreakBySmallestBrokerId() {
        BrokerData b = broker("b", 100, 2);
        BrokerData c = broker("c", 100, 1);
        BrokerData winner = MasterElectionManager.electNewMaster(Arrays.asList(b, c));
        assertEquals("c", winner.getBrokerName());
    }

    @Test
    public void testPromoteSetsIdZeroSlotAndBumpsEpoch() {
        MasterElectionManager mgr = new MasterElectionManager(new ServiceRegistry(), 20000);
        BrokerData b = broker("b", 100, 1);
        long epoch = mgr.promoteToMaster(b);
        assertEquals(1L, epoch);
        assertEquals(1L, mgr.getEpoch());
        assertTrue(b.getBrokerAddrs().containsKey(0L));   // id0 槽位指向新 master
        assertTrue(b.hasMaster());
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `mvn test -pl flare-mq-nameserver -Dtest=MasterElectionManagerTest`
Expected: FAIL —— 类不存在。

- [ ] **Step 3: 创建 `MasterElectionManager`**

```java
package com.flare.mq.nameserver.cluster;

import com.flare.mq.nameserver.registry.BrokerData;
import com.flare.mq.nameserver.registry.ServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Master 选举管理器（NameServer 单一决策权威）。
 * 选举规则：totalMessages 降序 → brokerId 升序；promote 把新 master 地址写入 brokerId 0 槽位，
 * 使客户端 getMasterAddr()（读 id0）零改动切换到新 master。epoch 单调递增，用作旧主栅栏。
 */
public class MasterElectionManager {

    private static final Logger logger = LoggerFactory.getLogger(MasterElectionManager.class);

    private final ServiceRegistry serviceRegistry;
    private final long leaseDurationMs;
    private final AtomicLong epoch = new AtomicLong(0);

    public MasterElectionManager(ServiceRegistry serviceRegistry, long leaseDurationMs) {
        this.serviceRegistry = serviceRegistry;
        this.leaseDurationMs = leaseDurationMs;
    }

    /** 确定性选主：offset 降序，平局 brokerId 升序。 */
    static BrokerData electNewMaster(List<BrokerData> candidates) {
        return candidates.stream()
                .sorted(Comparator
                        .comparingLong(BrokerData::getTotalMessages).reversed()
                        .thenComparingLong(MasterElectionManager::minBrokerId))
                .findFirst()
                .orElse(null);
    }

    private static long minBrokerId(BrokerData d) {
        return d.getBrokerAddrs().keySet().stream()
                .mapToLong(Long::longValue)
                .min()
                .orElse(Long.MAX_VALUE);
    }

    public long getEpoch() {
        return epoch.get();
    }

    public long getLeaseDurationMs() {
        return leaseDurationMs;
    }

    /** 提升：把新 master 的地址写入 brokerId 0 槽位，epoch+1，返回新 epoch。 */
    public long promoteToMaster(BrokerData newMaster) {
        long id = minBrokerId(newMaster);
        String addr = newMaster.getBrokerAddrs().get(id);
        newMaster.getBrokerAddrs().put(0L, addr);
        long e = epoch.incrementAndGet();
        logger.info("Promoted master: broker={}, addr={}, epoch={}", newMaster.getBrokerName(), addr, e);
        return e;
    }
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `mvn test -pl flare-mq-nameserver -Dtest=MasterElectionManagerTest`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/cluster/MasterElectionManager.java \
        flare-mq-nameserver/src/test/java/com/flare/mq/nameserver/cluster/MasterElectionManagerTest.java
git commit -m "feat(nameserver): MasterElectionManager 确定性选举 + epoch"
```

---

### Task 4: 故障转移编排 + BECOME_MASTER / STAND_DOWN RPC

**Files:**
- Modify: `flare-mq-protocol/src/main/java/com/flare/mq/protocol/MessageType.java`（新增 4 个类型并加入 `isRequest()`）
- Modify: `flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/cluster/MasterElectionManager.java`（`checkAndFailover` + RPC 发送 + `now()`）
- Modify: `flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/NameServerConfig.java`（新增 `masterLeaseDurationMs`、`failoverScanIntervalMs`）
- Modify: `flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/NameServerController.java`（构造 MasterElectionManager + 定时 `checkAndFailover`）
- Test: `flare-mq-nameserver/src/test/java/com/flare/mq/nameserver/cluster/MasterElectionManagerTest.java`（追加 `checkAndFailover` 用例）

**Interfaces:**
- Consumes: `ServiceRegistry.getAllBrokerData()`；`NettyClient(host, port).connect(); sendSync(ProtocolMessage, timeout)`。
- Produces: `MasterElectionManager(ServiceRegistry, long leaseDurationMs)`；`void checkAndFailover()`。

- [ ] **Step 1: 改协议——新增 MessageType**

在 `MessageType` 枚举末尾（`GET_CONSUMER_GROUPS_RESPONSE((short) 75);` 前）插入：

```java
// ========== 集群故障转移 ==========
BECOME_MASTER_REQUEST((short) 76),
BECOME_MASTER_RESPONSE((short) 77),
STAND_DOWN_REQUEST((short) 78),
STAND_DOWN_RESPONSE((short) 79),
```

并把 4 个 REQUEST 加入 `isRequest()`：

```java
this == BECOME_MASTER_REQUEST || this == STAND_DOWN_REQUEST ||
```

- [ ] **Step 2: 写失败测试（checkAndFailover）**

在 `MasterElectionManagerTest` 追加：

```java
@Test
public void testCheckAndFailoverElectsBestSlave() throws Exception {
    ServiceRegistry registry = new ServiceRegistry();
    MasterElectionManager mgr = new MasterElectionManager(registry, 5000) {
        @Override
        protected long now() { return 100_000L; }   // 固定时钟
    };

    // master 已过期：注册为 id0 后把 lastUpdate 置旧（距 now=100s 超过 lease 5s）
    registry.registerBroker("DefaultCluster", "addr-m", "m", 0L, null, null, null, false);
    registry.getBrokerData("m").setLastUpdateTimestamp(10_000L);

    // 两个存活 slave，offset 不同（在注册对象上设置，模拟 broker 上报）
    registry.registerBroker("DefaultCluster", "addr-b", "b", 1L, null, null, null, false);
    registry.registerBroker("DefaultCluster", "addr-c", "c", 2L, null, null, null, false);
    registry.getBrokerData("b").setTotalMessages(100);
    registry.getBrokerData("c").setTotalMessages(50);
    registry.getBrokerData("b").setLastUpdateTimestamp(95_000L);
    registry.getBrokerData("c").setLastUpdateTimestamp(95_000L);

    mgr.checkAndFailover();

    // RPC 发给不存在的 broker 会失败，但被吞掉；注册表与 epoch 应已更新
    assertEquals(1L, mgr.getEpoch());
    assertTrue(registry.getBrokerData("b").getBrokerAddrs().containsKey(0L));   // offset 大者当选
    assertFalse(registry.getBrokerData("c").getBrokerAddrs().containsKey(0L));
}
```

- [ ] **Step 3: 运行测试确认失败**

Run: `mvn test -pl flare-mq-nameserver -Dtest=MasterElectionManagerTest`
Expected: FAIL —— 构造函数签名、`checkAndFailover` 不存在。

- [ ] **Step 4: 实现 `checkAndFailover` 与 RPC 发送**

`serviceRegistry` 字段与双参构造函数已在 Task 3 定义，无需改动。新增方法：

```java
protected long now() { return System.currentTimeMillis(); }

/** 定期调用：master 租约过期则选新主并下发 BECOME_MASTER。 */
public void checkAndFailover() {
    try {
        BrokerData master = findAliveMaster();
        if (master != null) {
            return;   // 当前主仍存活，无需切换
        }
        List<BrokerData> slaves = aliveSlaves();
        BrokerData winner = electNewMaster(slaves);
        if (winner == null) {
            logger.warn("No alive slave candidate for master failover");
            return;
        }
        sendStandDown(master);
        long e = promoteToMaster(winner);
        sendBecomeMaster(winner, e);
    } catch (Exception ex) {
        logger.error("Error in checkAndFailover", ex);
    }
}

/** 当前 master = 拥有 brokerId 0 槽位且租约未过期的节点。 */
private BrokerData findAliveMaster() {
    for (BrokerData d : serviceRegistry.getAllBrokerData().values()) {
        if (d.getBrokerAddrs().containsKey(0L) && isAlive(d)) {
            return d;
        }
    }
    return null;
}

/** 全部存活节点（排除自身 = 选举候选）。 */
private List<BrokerData> aliveSlaves() {
    List<BrokerData> alive = new ArrayList<>();
    for (BrokerData d : serviceRegistry.getAllBrokerData().values()) {
        if (isAlive(d)) {
            alive.add(d);
        }
    }
    return alive;
}

private boolean isAlive(BrokerData d) {
    return (now() - d.getLastUpdateTimestamp()) <= leaseDurationMs;
}

/** 尽力通知旧主让位（连不上/失败不影响主流程）。 */
private void sendStandDown(BrokerData master) {
    if (master == null) return;
    String addr = master.getBrokerAddrs().get(0L);
    sendToBroker(addr, new ProtocolMessage(MessageType.STAND_DOWN_REQUEST, null));
}

/** 通知新主上任（携带新 epoch）。 */
private void sendBecomeMaster(BrokerData newMaster, long e) {
    String addr = newMaster.getBrokerAddrs().get(0L);
    String json = "{\"epoch\":" + e + "}";
    sendToBroker(addr, new ProtocolMessage(MessageType.BECOME_MASTER_REQUEST,
            json.getBytes(StandardCharsets.UTF_8)));
}

/** 短超时、异常吞掉：RPC 是尽力而为的通知。 */
private void sendToBroker(String addr, ProtocolMessage msg) {
    if (addr == null) return;
    try {
        String[] parts = addr.split(":");
        NettyClient client = new NettyClient(parts[0], Integer.parseInt(parts[1]));
        client.connect();
        try {
            client.sendSync(msg, 1000);
        } finally {
            client.disconnect();
        }
    } catch (Exception e) {
        logger.warn("RPC to broker {} failed (best-effort): {}", addr, e.getMessage());
    }
}
```

新增 import：`com.flare.mq.protocol.MessageType`、`com.flare.mq.protocol.ProtocolMessage`、`com.flare.mq.protocol.client.NettyClient`、`com.flare.mq.nameserver.registry.ServiceRegistry`、`java.nio.charset.StandardCharsets`、`java.util.ArrayList`、`java.util.List`。

- [ ] **Step 5: 改 `NameServerConfig` 与 `NameServerController`**

`NameServerConfig` 新增：

```java
private long masterLeaseDurationMs = 1000 * 20;   // master 租约 20s
private long failoverScanIntervalMs = 1000 * 3;   // failover 扫描 3s

public long getMasterLeaseDurationMs() { return masterLeaseDurationMs; }
public void setMasterLeaseDurationMs(long v) { this.masterLeaseDurationMs = v; }
public long getFailoverScanIntervalMs() { return failoverScanIntervalMs; }
public void setFailoverScanIntervalMs(long v) { this.failoverScanIntervalMs = v; }
```

`NameServerController` 字段与构造：

```java
private final MasterElectionManager masterElectionManager;
// 构造函数内：
this.masterElectionManager = new MasterElectionManager(
        serviceRegistry, nameServerConfig.getMasterLeaseDurationMs());
```

`startScheduledTasks()` 末尾追加：

```java
scheduledExecutorService.scheduleAtFixedRate(() -> {
    try {
        masterElectionManager.checkAndFailover();
    } catch (Exception e) {
        logger.error("Error in master failover check", e);
    }
}, 3, nameServerConfig.getFailoverScanIntervalMs(), TimeUnit.SECONDS);
```

新增 getter：`public MasterElectionManager getMasterElectionManager() { return masterElectionManager; }`

- [ ] **Step 6: 运行测试确认通过**

Run: `mvn test -pl flare-mq-nameserver -Dtest=MasterElectionManagerTest`
Expected: PASS。

- [ ] **Step 7: 提交**

```bash
git add flare-mq-protocol/src/main/java/com/flare/mq/protocol/MessageType.java \
        flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/cluster/MasterElectionManager.java \
        flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/NameServerConfig.java \
        flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/NameServerController.java \
        flare-mq-nameserver/src/test/java/com/flare/mq/nameserver/cluster/MasterElectionManagerTest.java
git commit -m "feat(nameserver): 租约过期检测 + 选主编排 + BECOME_MASTER/STAND_DOWN RPC"
```

---

### Task 5: Broker 响应 BECOME_MASTER / STAND_DOWN + 失联停写

**Files:**
- Modify: `flare-mq-broker/src/main/java/com/flare/mq/broker/BrokerRequestHandler.java`（新增 `ClusterRoleListener` + switch 分支 + 写守卫）
- Modify: `flare-mq-broker/src/main/java/com/flare/mq/broker/cluster/ClusterManager.java`（实现 listener：`becomeMaster`/`standDown`/`isAcceptingWrites`）
- Modify: `flare-mq-broker/src/main/java/com/flare/mq/broker/registry/BrokerRegistration.java`（心跳失败计数 → `onHeartbeatLost`）
- Test: `flare-mq-broker/src/test/java/com/flare/mq/broker/BrokerRoleHandlerTest.java`

**Interfaces:**
- Consumes: `MessageType.BECOME_MASTER_REQUEST` / `STAND_DOWN_REQUEST`（Task 4 新增）。
- Produces: `interface ClusterRoleListener { void onBecomeMaster(long epoch); void onStandDown(); boolean isAcceptingWrites(); }`；`BrokerRequestHandler.setClusterRoleListener(ClusterRoleListener)`。
- Produces: `ClusterManager.becomeMaster(long epoch)`、`standDown()`、`isAcceptingWrites()`。
- Produces: `BrokerRegistration.setHeartbeatLossListener(Runnable)`。

- [ ] **Step 1: 写失败测试 `BrokerRoleHandlerTest`**

```java
package com.flare.mq.broker;

import com.flare.mq.broker.BrokerRequestHandler.ClusterRoleListener;
import com.flare.mq.protocol.MessageType;
import com.flare.mq.protocol.ProtocolMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

public class BrokerRoleHandlerTest {

    @Test
    public void testBecomeMasterInvokesListener() {
        AtomicLong receivedEpoch = new AtomicLong(-1);
        ClusterRoleListener listener = new ClusterRoleListener() {
            @Override public void onBecomeMaster(long epoch) { receivedEpoch.set(epoch); }
            @Override public void onStandDown() { }
            @Override public boolean isAcceptingWrites() { return true; }
        };

        BrokerRequestHandler handler = new BrokerRequestHandler(null, null, null, null, null);
        handler.setClusterRoleListener(listener);

        ProtocolMessage msg = new ProtocolMessage(MessageType.BECOME_MASTER_REQUEST,
                "{\"epoch\":7}".getBytes(StandardCharsets.UTF_8));
        ProtocolMessage resp = handler.handleRequest(null, msg);

        assertTrue(resp.isSuccess());
        assertEquals(7L, receivedEpoch.get());
    }

    @Test
    public void testSendRejectedWhenNotAcceptingWrites() {
        ClusterRoleListener listener = new ClusterRoleListener() {
            @Override public void onBecomeMaster(long epoch) { }
            @Override public void onStandDown() { }
            @Override public boolean isAcceptingWrites() { return false; }
        };

        BrokerRequestHandler handler = new BrokerRequestHandler(null, null, null, null, null);
        handler.setClusterRoleListener(listener);

        String body = "{\"topic\":\"t\",\"body\":\"hello\",\"messageId\":\"m1\"}";
        ProtocolMessage msg = new ProtocolMessage(MessageType.SEND_MESSAGE_REQUEST,
                body.getBytes(StandardCharsets.UTF_8));
        ProtocolMessage resp = handler.handleRequest(null, msg);

        assertFalse(resp.isSuccess());   // 停写时拒收新消息
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `mvn test -pl flare-mq-broker -Dtest=BrokerRoleHandlerTest`
Expected: FAIL —— `ClusterRoleListener`、`setClusterRoleListener` 不存在。

- [ ] **Step 3: 改 `BrokerRequestHandler`**

新增接口（放在类内）：

```java
public interface ClusterRoleListener {
    void onBecomeMaster(long epoch);
    void onStandDown();
    boolean isAcceptingWrites();
}
```

字段与 setter：

```java
private volatile ClusterRoleListener clusterRoleListener;

public void setClusterRoleListener(ClusterRoleListener clusterRoleListener) {
    this.clusterRoleListener = clusterRoleListener;
}
```

`handleRequest` switch 追加分支：

```java
case BECOME_MASTER_REQUEST:
    return handleBecomeMaster(request);
case STAND_DOWN_REQUEST:
    return handleStandDown(request);
```

新增方法：

```java
private ProtocolMessage handleBecomeMaster(ProtocolMessage request) {
    long epoch = 0L;
    try {
        String json = new String(request.getBody(), StandardCharsets.UTF_8);
        Map<?, ?> map = JsonUtils.fromJson(json, Map.class);
        if (map != null && map.get("epoch") instanceof Number) {
            epoch = ((Number) map.get("epoch")).longValue();
        }
    } catch (Exception ignored) { }
    if (clusterRoleListener != null) {
        clusterRoleListener.onBecomeMaster(epoch);
    }
    return ProtocolMessage.createSuccessResponse(
            MessageType.BECOME_MASTER_RESPONSE, request.getRequestId(), null);
}

private ProtocolMessage handleStandDown(ProtocolMessage request) {
    if (clusterRoleListener != null) {
        clusterRoleListener.onStandDown();
    }
    return ProtocolMessage.createSuccessResponse(
            MessageType.STAND_DOWN_RESPONSE, request.getRequestId(), null);
}
```

`handleSendMessage` 开头（解析 body 之前）加写守卫：

```java
if (clusterRoleListener != null && !clusterRoleListener.isAcceptingWrites()) {
    return ProtocolMessage.createErrorResponse(
            MessageType.SEND_MESSAGE_RESPONSE, request.getRequestId(), ResponseCode.SERVICE_UNAVAILABLE);
}
```

- [ ] **Step 4: 改 `ClusterManager` 实现 listener**

`ClusterManager` 在 `com.flare.mq.broker.cluster` 包，需 import 嵌套接口：`import com.flare.mq.broker.BrokerRequestHandler.ClusterRoleListener;`。类声明加 `implements ClusterRoleListener`，新增字段：

```java
private volatile long currentEpoch = 0L;
private volatile boolean acceptingWrites = true;
```

实现方法：

```java
@Override
public void onBecomeMaster(long epoch) {
    becomeMaster(epoch);
}

@Override
public void onStandDown() {
    BrokerNode node = clusterNodes.get(brokerName);
    if (node != null) {
        node.setRole(BrokerRole.SLAVE);
    }
    acceptingWrites = false;
    logger.warn("Broker stood down from master role: {}", brokerName);
}

@Override
public boolean isAcceptingWrites() {
    return acceptingWrites;
}

public void becomeMaster(long epoch) {
    BrokerNode node = clusterNodes.get(brokerName);
    if (node != null) {
        node.setRole(BrokerRole.MASTER);
        node.setLastUpdateTime(System.currentTimeMillis());
    }
    this.currentEpoch = epoch;
    this.acceptingWrites = true;
    replicationManager.startAsmaster();
    if (brokerRegistration != null) {
        brokerRegistration.setCurrentEpoch(epoch);
    }
    logger.info("Broker became master via BECOME_MASTER: {}, epoch={}", brokerName, epoch);
}
```

构造函数中改为"先建 handler、挂 listener、再建 server"（`NettyServer` 不暴露 handler getter，只能先持有引用）：

```java
BrokerRequestHandler requestHandler = new BrokerRequestHandler(
        topicManager, queueManager, messageStore, offsetManager, this.ackManager);
requestHandler.setClusterRoleListener(this);
this.nettyServer = new NettyServer(port, requestHandler);
```

**限制自选主：仅 brokerId 0 节点在启动时自选 master**，其余节点保持 SLAVE，等 NameServer 下发 `BECOME_MASTER`。修改 `tryBecomeMaster()` 开头：

```java
private void tryBecomeMaster() {
    if (clusterConfig.getBrokerId() != 0L) {
        return;   // 非 id0 节点不自选 master，等 NameServer BECOME_MASTER 指令
    }
    BrokerNode currentNode = clusterNodes.get(brokerName);
    if (currentNode == null) {
        return;
    }
    // ... 原有 hasMaster 判断与角色翻转保持不变
}
```

并在 `joinCluster` 后把 `brokerRegistration` 挂上心跳丢失监听：

```java
brokerRegistration.setHeartbeatLossListener(this::onHeartbeatLost);
```

新增：

```java
private void onHeartbeatLost() {
    acceptingWrites = false;
    logger.error("Heartbeat lost to NameServer repeatedly, stopping writes: {}", brokerName);
}
```

- [ ] **Step 5: 改 `BrokerRegistration` 心跳失败计数**

字段：

```java
private volatile int consecutiveHeartbeatFailures = 0;
private volatile Runnable heartbeatLossListener;

public void setHeartbeatLossListener(Runnable listener) {
    this.heartbeatLossListener = listener;
}

public void setCurrentEpoch(long epoch) {
    this.currentEpoch = epoch;
}
private volatile long currentEpoch = 0L;   // 与上述字段放一起
```

`sendHeartbeat` 内：

```java
if (response == null || response.getStatus().getCode() != 0) {
    consecutiveHeartbeatFailures++;
    logger.warn("Heartbeat failed to NameServer: brokerName={}, consecutiveFailures={}",
            brokerName, consecutiveHeartbeatFailures);
    if (consecutiveHeartbeatFailures >= 3 && heartbeatLossListener != null) {
        heartbeatLossListener.run();
    }
} else {
    consecutiveHeartbeatFailures = 0;
}
```

- [ ] **Step 6: 运行测试确认通过**

Run: `mvn test -pl flare-mq-broker -Dtest=BrokerRoleHandlerTest,BrokerSendMessageTest,ClusterTest`
Expected: PASS（handler 无 listener 时 SEND 行为不变，既有测试不回归）。

- [ ] **Step 7: 提交**

```bash
git add flare-mq-broker/src/main/java/com/flare/mq/broker/BrokerRequestHandler.java \
        flare-mq-broker/src/main/java/com/flare/mq/broker/cluster/ClusterManager.java \
        flare-mq-broker/src/main/java/com/flare/mq/broker/registry/BrokerRegistration.java \
        flare-mq-broker/src/test/java/com/flare/mq/broker/BrokerRoleHandlerTest.java
git commit -m "feat(broker): 响应 BECOME_MASTER/STAND_DOWN，心跳连续失败即停写"
```

---

### Task 6: epoch 栅栏——拒绝旧 epoch 的 id0 注册

**Files:**
- Modify: `flare-mq-broker/src/main/java/com/flare/mq/broker/registry/BrokerRegistration.java`（`RegisterBrokerRequest` 加 `epoch`；`registerBroker` 填充）
- Modify: `flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/NameServerRequestHandler.java`（构造函数加 `MasterElectionManager`；`handleRegisterBroker` 拒旧 epoch）
- Modify: `flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/NameServerController.java`（传 `masterElectionManager` 给 handler）
- Modify: `flare-mq-protocol/src/main/java/com/flare/mq/protocol/ResponseCode.java`（新增 `STALE_EPOCH`）
- Test: `flare-mq-nameserver/src/test/java/com/flare/mq/nameserver/NameServerEpochFenceTest.java`

**Interfaces:**
- Consumes: `MasterElectionManager.getEpoch()`（Task 3 产出）。
- Produces: `NameServerRequestHandler(..., HealthChecker, MasterElectionManager)` 新构造函数。

- [ ] **Step 1: 写失败测试 `NameServerEpochFenceTest`**

```java
package com.flare.mq.nameserver;

import com.flare.mq.nameserver.cluster.MasterElectionManager;
import com.flare.mq.nameserver.health.HealthChecker;
import com.flare.mq.nameserver.registry.ServiceDiscovery;
import com.flare.mq.nameserver.registry.ServiceRegistry;
import com.flare.mq.nameserver.route.RouteInfoManager;
import com.flare.mq.protocol.MessageType;
import com.flare.mq.protocol.ProtocolMessage;
import com.flare.mq.protocol.ResponseCode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class NameServerEpochFenceTest {

    @Test
    public void testStaleEpochMasterRegistrationRejected() {
        ServiceRegistry registry = new ServiceRegistry();
        MasterElectionManager election = new MasterElectionManager(registry, 20000);
        // 先把 epoch 抬到 1（模拟发生过一次选举）
        election.promoteToMaster(makeBroker("b", 1));
        assertEquals(1L, election.getEpoch());

        NameServerRequestHandler handler = new NameServerRequestHandler(
                new ServiceDiscovery(registry), registry, new RouteInfoManager(),
                new HealthChecker(registry), election);

        // 旧的 id0 master 用 epoch=0 重新注册 → 应被拒
        String body = "{\"clusterName\":\"DefaultCluster\",\"brokerName\":\"m\",\"brokerAddr\":\"addr-m\","
                + "\"brokerId\":0,\"epoch\":0}";
        ProtocolMessage resp = handler.handleRequest(null,
                new ProtocolMessage(MessageType.REGISTER_BROKER_REQUEST,
                        body.getBytes(StandardCharsets.UTF_8)));

        assertFalse(resp.isSuccess());
        assertEquals(ResponseCode.STALE_EPOCH, resp.getStatus());
    }

    private com.flare.mq.nameserver.registry.BrokerData makeBroker(String name, long id) {
        com.flare.mq.nameserver.registry.BrokerData d =
                new com.flare.mq.nameserver.registry.BrokerData("DefaultCluster", name);
        d.getBrokerAddrs().put(id, "127.0.0.1:" + (10911 + id));
        return d;
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `mvn test -pl flare-mq-nameserver -Dtest=NameServerEpochFenceTest`
Expected: FAIL —— 构造函数签名不匹配 / `STALE_EPOCH` 不存在。

- [ ] **Step 3: 改 `ResponseCode` 新增枚举**

```java
// ========== 集群错误 6xx ==========
/**
 * epoch 过时，拒绝注册（防双主栅栏）
 */
STALE_EPOCH((short) 604, "epoch 过时"),
```

- [ ] **Step 4: 改 `NameServerRequestHandler` 构造函数与注册处理**

构造函数加参数并保存：

```java
private final MasterElectionManager masterElectionManager;

public NameServerRequestHandler(ServiceDiscovery serviceDiscovery, ServiceRegistry serviceRegistry,
                                RouteInfoManager routeInfoManager, HealthChecker healthChecker,
                                MasterElectionManager masterElectionManager) {
    ...
    this.masterElectionManager = masterElectionManager;
}
```

`handleRegisterBroker` 解析 DTO 后加栅栏：

```java
if (brokerRequest.brokerId == 0L && brokerRequest.epoch < masterElectionManager.getEpoch()) {
    logger.warn("Rejected stale-epoch master registration: brokerName={}, epoch={}, current={}",
            brokerRequest.brokerName, brokerRequest.epoch, masterElectionManager.getEpoch());
    return ProtocolMessage.createErrorResponse(
            MessageType.REGISTER_BROKER_RESPONSE, request.getRequestId(), ResponseCode.STALE_EPOCH);
}
```

- [ ] **Step 5: 改 `NameServerController` 传参**

```java
NameServerRequestHandler requestHandler = new NameServerRequestHandler(
        serviceDiscovery, serviceRegistry, routeInfoManager, healthChecker, masterElectionManager);
```

- [ ] **Step 6: 同步更新 Task 2 的 `NameServerHeartbeatTest`（构造函数已变 5 参）**

`NameServerHeartbeatTest` 中 handler 构造改为：

```java
NameServerRequestHandler handler = new NameServerRequestHandler(
        new ServiceDiscovery(registry), registry, new RouteInfoManager(),
        checker, new MasterElectionManager(registry, 20000));
```

（`MasterElectionManager` 用同一 `registry`，lease 默认值即可，本测试不触发选举。）

- [ ] **Step 7: 改 `BrokerRegistration` 上报 epoch**

`RegisterBrokerRequest` DTO 加字段：

```java
public long epoch;
```

`registerBroker` 填充：

```java
request.epoch = this.currentEpoch;
```

（`currentEpoch` 已在 Task 5 加入，默认 0。）

- [ ] **Step 8: 运行测试确认通过**

Run: `mvn test -pl flare-mq-nameserver -Dtest=NameServerEpochFenceTest,NameServerHeartbeatTest`
Expected: PASS。

- [ ] **Step 9: 提交**

```bash
git add flare-mq-protocol/src/main/java/com/flare/mq/protocol/ResponseCode.java \
        flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/NameServerRequestHandler.java \
        flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/NameServerController.java \
        flare-mq-nameserver/src/test/java/com/flare/mq/nameserver/NameServerEpochFenceTest.java \
        flare-mq-broker/src/main/java/com/flare/mq/broker/registry/BrokerRegistration.java
git commit -m "feat(cluster): epoch 栅栏——拒绝旧 epoch 的 master 注册"
```

---

### Task 7: 死代码清理——删除 broker 侧 FailoverManager

**Files:**
- Delete: `flare-mq-broker/src/main/java/com/flare/mq/broker/cluster/FailoverManager.java`
- Modify: `flare-mq-broker/src/main/java/com/flare/mq/broker/cluster/ClusterManager.java`（移除 `failoverManager` 字段/构造/start/shutdown、`handleClusterDegradation` 调用）
- Test: 既有 `ClusterTest`、`BrokerSendMessageTest` 回归

> **范围说明**：spec §6 提到删除 nameserver 侧 `routing/cluster/FailoverManager` + `ClusterRouter`，但 `SmartRoutingEngine` 依赖 `ClusterRouter`（`SmartRoutingEngine.java:53`），而 `SmartRoutingEngine` 仅被测试/demo 使用、不在 NameServer 运行时路径。删除会级联破坏 `SmartRoutingEngine`，故本次只删 **broker 侧**死代码；nameserver 侧休眠路由子系统保持不动（不在运行时路径上，不影响本特性）。

- [ ] **Step 1: 删除文件并编译确认断点**

Run: `rm flare-mq-broker/src/main/java/com/flare/mq/broker/cluster/FailoverManager.java && mvn test -pl flare-mq-broker -Dtest=ClusterTest 2>&1 | grep -E "ERROR|FailoverManager"`
Expected: 编译报错，指向 `ClusterManager` 中所有 `failoverManager` 引用。

- [ ] **Step 2: 清理 `ClusterManager` 引用**

删除字段声明：

```java
// 删除
private final FailoverManager failoverManager;
```

删除构造函数中的初始化：

```java
// 删除
this.failoverManager = new FailoverManager(this);
```

删除 `start()` / `shutdown()` 中的启动与关闭：

```java
// start() 中删除
failoverManager.start();

// shutdown() 中删除
failoverManager.shutdown();
```

删除 `checkClusterHealth()` 中对 `failoverManager` 的调用：

```java
// 删除
failoverManager.handleClusterDegradation(healthRatio);
```

删除 getter：

```java
// 删除
public FailoverManager getFailoverManager() { return failoverManager; }
```

- [ ] **Step 3: 编译 + 回归**

Run: `mvn test -pl flare-mq-broker -Dtest=ClusterTest,BrokerSendMessageTest`
Expected: PASS。

- [ ] **Step 4: 提交**

```bash
git add flare-mq-broker/src/main/java/com/flare/mq/broker/cluster/ClusterManager.java
git rm flare-mq-broker/src/main/java/com/flare/mq/broker/cluster/FailoverManager.java
git commit -m "refactor(broker): 删除本地空转的 FailoverManager（选举/故障转移收敛到 NameServer）"
```

---

### Task 8: 集成测试——kill master 后新 master 接管

**Files:**
- Create: `flare-mq-test/src/test/java/com/flare/mq/test/cluster/ClusterFailoverIntegrationTest.java`

> 依赖 Task 1-7 全部完成。`flare-mq-test` 已依赖 broker/nameserver/client（见其 pom.xml）。

- [ ] **Step 1: 写集成测试**

```java
package com.flare.mq.test.cluster;

import com.flare.mq.broker.cluster.ClusterConfig;
import com.flare.mq.broker.cluster.ClusterManager;
import com.flare.mq.nameserver.NameServerConfig;
import com.flare.mq.nameserver.NameServerController;
import com.flare.mq.nameserver.registry.BrokerData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class ClusterFailoverIntegrationTest {

    private NameServerController nameServer;
    private ClusterManager broker1; // master
    private ClusterManager broker2; // slave, offset 大
    private ClusterManager broker3; // slave, offset 小

    private static final String NS_ADDR = "127.0.0.1:19876";

    @BeforeEach
    public void setUp() throws Exception {
        NameServerConfig nsConfig = new NameServerConfig();
        nsConfig.setListenPort(19876);
        nsConfig.setMasterLeaseDurationMs(1500);   // 缩短租约，加速 kill-master 场景
        nsConfig.setFailoverScanIntervalMs(500);
        nameServer = new NameServerController(nsConfig);
        nameServer.start();

        broker1 = startBroker("127.0.0.1:20911", 0, "broker-1");
        broker2 = startBroker("127.0.0.1:20912", 1, "broker-2");
        broker3 = startBroker("127.0.0.1:20913", 2, "broker-3");
    }

    private ClusterManager startBroker(String addr, long brokerId, String dirSuffix) throws Exception {
        ClusterConfig config = new ClusterConfig(addr, brokerId);
        config.setNameServerAddr(NS_ADDR);
        Path tmp = Files.createTempDirectory("flare-" + dirSuffix);
        config.setDataDir(tmp.toString());
        ClusterManager cm = new ClusterManager("DefaultCluster", "127.0.0.1-" + addr.split(":")[1], config);
        cm.start();
        return cm;
    }

    @AfterEach
    public void tearDown() {
        if (broker3 != null) broker3.shutdown();
        if (broker2 != null) broker2.shutdown();
        if (broker1 != null) broker1.shutdown();
        if (nameServer != null) nameServer.shutdown();
    }

    @Test
    public void testFailoverPicksMostCaughtUpSlave() throws Exception {
        // 等待三个 broker 注册完成
        await(() -> nameServer.getServiceRegistry().getBrokerCount() == 3, 15_000);
        // 初始 master = brokerId 0 的 broker1（过滤存活，避开残留）
        BrokerData master = findAliveMaster();
        assertNotNull(master, "should have a master after startup");
        assertEquals("127.0.0.1:20911", master.getMasterAddr());

        // 制造 offset 差异：broker2 有 100 条，broker3 有 50 条
        nameServer.getServiceRegistry().getBrokerData("127.0.0.1-20912").setTotalMessages(100);
        nameServer.getServiceRegistry().getBrokerData("127.0.0.1-20913").setTotalMessages(50);

        long epochBefore = nameServer.getMasterElectionManager().getEpoch();

        // kill master
        broker1.shutdown();
        broker1 = null;

        // 等待 failover：新 master 地址变为 broker2（存活），epoch 递增，且 broker2 本地角色翻转
        await(() -> {
            BrokerData m = findAliveMaster();
            return m != null && "127.0.0.1:20912".equals(m.getMasterAddr())
                    && broker2.isMaster();
        }, 15_000);

        assertEquals(epochBefore + 1, nameServer.getMasterElectionManager().getEpoch());
    }

    /** 拥有 id0 槽位且租约内（存活）的节点才是当前 master。 */
    private BrokerData findAliveMaster() {
        long now = System.currentTimeMillis();
        for (BrokerData d : nameServer.getServiceRegistry().getAllBrokerData().values()) {
            if (d.getBrokerAddrs().containsKey(0L)
                    && (now - d.getLastUpdateTimestamp()) <= 5000) {
                return d;
            }
        }
        return null;
    }

    private void await(java.util.function.BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return;
            Thread.sleep(200);
        }
        fail("condition not met within " + timeoutMs + "ms");
    }
}
```

- [ ] **Step 2: 运行测试**

Run: `mvn test -pl flare-mq-test -Dtest=ClusterFailoverIntegrationTest`
Expected: PASS。若首次因端口占用/时序失败，重跑一次（端口 19876/20911/20912/20913 应空闲）。

- [ ] **Step 3: 全量回归**

Run: `mvn test`
Expected: 全绿（若存量测试与本次改动无冲突）。

- [ ] **Step 4: 提交**

```bash
git add flare-mq-test/src/test/java/com/flare/mq/test/cluster/ClusterFailoverIntegrationTest.java
git commit -m "test(cluster): 集成测试——kill master 后 offset 最大 slave 接管"
```

---

## Self-Review

**Spec 覆盖核对：**
- §1 命名推导 → Task 1。
- §2 心跳链路 + 租约 → Task 2 + Task 4（租约检测落在 `MasterElectionManager.checkAndFailover`，用 `BrokerData.lastUpdateTimestamp` + `masterLeaseDurationMs`，而非改 HealthChecker 公式——比 spec 更少改动且可测，见下方偏差）。
- §3 选举（offset 最大 → brokerId 决胜）+ epoch 栅栏 → Task 3 + Task 4 + Task 6。
- §4 故障转移链路 → Task 4（stand down → 选主 → 提升 → BECOME_MASTER）+ Task 5（broker 执行）。
- §5 路由切换（id0 槽位）→ Task 3 `promoteToMaster`。
- §6 死代码 → Task 7（范围收敛说明见该任务）。
- §7 测试 → 各任务单测 + Task 8 集成。

**计划偏差（相对 spec，均为收敛性调整）：**
1. 租约检测不落在 `HealthChecker`，而在 `MasterElectionManager` 用 `ServiceRegistry.BrokerData.lastUpdateTimestamp` 判定（leaseDuration=20s < HealthChecker 的 2 分钟移除阈值，不会在 failover 前被误删）。broker 心跳已在 Task 2 刷新该时间戳。
2. 删除 nameserver 侧 `FailoverManager`/`ClusterRouter` 收敛为只删 broker 侧 `FailoverManager`（`SmartRoutingEngine` 依赖所致，休眠子系统不在运行时路径）。
3. "客户端继续写入"的集成断言改为"新 master 被选出 + id0 槽位切换 + epoch+1 + 新 master 本地角色翻转"（控制面），topic 队列跨 broker 迁移不在本次范围（与 spec 非目标一致）。

**类型一致性：** `BrokerIdentity.resolve`、`MasterElectionManager(ServiceRegistry, long)`、`NameServerRequestHandler(..., HealthChecker, MasterElectionManager)`、`ClusterRoleListener` 三方法签名、`promoteToMaster` 返回值，前后各任务引用一致。

**Placeholder 扫描：** 各任务均含可执行代码与断言，无 TBD/TODO 描述性占位。
