# Console 真实监控数据设计

2026-07-04 | 替换 mock 数据 | 总括 6 文件

## 背景

Console 模块的概览/Broker/Topic 数据全部来自 `MonitorServiceImpl` 里的硬编码 mock 数据。需要接入真实的 NameServer 和 Broker 指标。

## 架构

```
Console (MonitorServiceImpl)
  │ GET_CLUSTER_STATS_REQUEST (每 5s)
  ▼
NameServer (NameServerRequestHandler.handleGetClusterStats)
  │ 读取 ServiceRegistry (BrokerData + 指标)
  │ 读取 RouteInfoManager (topic/queue count)
  ▼
返回 JSON: {brokers:[{...}], topicCount, queueCount, ...}
  │
  └─ Broker 每 30s 向 NameServer 注册时上报 CPU/内存/磁盘/消息数/TPS
```

## 模块一：协议层

### MessageType.java

新增两个消息类型：
```java
GET_CLUSTER_STATS_REQUEST(70),
GET_CLUSTER_STATS_RESPONSE(71),
```

### NameServer 响应 JSON 格式

```json
{
  "brokers": [
    {
      "brokerName": "broker-a",
      "clusterName": "default",
      "brokerAddr": "10.0.0.1:10911",
      "brokerId": 0,
      "role": "Master",
      "cpuUsage": 0.23,
      "memoryUsage": 0.45,
      "diskUsage": 0.32,
      "totalMessages": 45000,
      "currentTps": 850.0,
      "lastUpdateTimestamp": 1720060200000
    }
  ],
  "topicCount": 12,
  "queueCount": 48,
  "consumerGroupCount": 5
}
```

## 模块二：Broker 侧

### BrokerData.java — 新增字段

```java
private double cpuUsage;
private double memoryUsage;
private double diskUsage;
private long totalMessages;
private double currentTps;
// + getter/setter
```

### ServiceRegistry.registerBroker() — 解析并存储指标

从 `RegisterBrokerRequest` 中读取 cpuUsage/memoryUsage/diskUsage/totalMessages/currentTps，写入 `BrokerData`。

### NameServerRequestHandler — 新增端点

```java
case GET_CLUSTER_STATS_REQUEST:
    return handleGetClusterStats(request);
```

```java
private ProtocolMessage handleGetClusterStats(ProtocolMessage request) {
    // 1. 从 serviceRegistry.getAllBrokerData() 获取所有 broker 及指标
    // 2. 从 routeInfoManager.getStatistics() 获取 topic/queue 数量
    // 3. 从 serviceRegistry.getAllConsumerGroups() 获取 consumer 组数
    // 4. 组装 JSON 返回
}
```

## 模块三：Broker 注册上报

### BrokerRegistration.registerBroker() — RegisterBrokerRequest 新增字段

RegisterBrokerRequest 新增 5 个字段，在 `registerBroker()` 中采集：

| 字段 | 采集方式 |
|------|---------|
| `cpuUsage` | `ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage()` / `availableProcessors()` |
| `memoryUsage` | `1.0 - Runtime.getRuntime().freeMemory() / totalMemory()` |
| `diskUsage` | commitLog 目录 `totalSpace - usableSpace / totalSpace` |
| `totalMessages` | 通过构造时注入的引用获取（当前简化为估算值） |
| `currentTps` | Broker 内部每秒写入计数 |

## 模块四：Console 重写 MonitorServiceImpl

- 启动时创建 `NettyClient` 连接 NameServer（地址通过配置注入）
- 每 5 秒发 `GET_CLUSTER_STATS_REQUEST`，解析响应更新缓存
- `getSystemOverview()` / `getBrokerStatusList()` / `getTopicStatsList()` / `getClusterHealth()` 从缓存读取并转换为对应模型
- `healthStatus` 计算: 
  - `healthyBrokers/totalBrokers >= 0.8` → GREEN
  - `healthyBrokers/totalBrokers >= 0.5` → WARNING
  - 其余 → CRITICAL
  - NameServer 不可达 → UNKNOWN
- `healthy` 判定: `lastUpdateTimestamp` 距今 < 30s 为存活
- `getPerformanceMetrics()` / `getTpsStatistics()` / `getStorageStatistics()` / `getSystemAlerts()` 等暂不在本次范围的接口，返回空数据

## 改动清单

| 文件 | 模块 | 改动 |
|------|------|------|
| `MessageType.java` | protocol | +2 枚举值 |
| `BrokerData.java` | nameserver | +5 指标字段 |
| `ServiceRegistry.java` | nameserver | `registerBroker()` 解析指标 |
| `NameServerRequestHandler.java` | nameserver | 新增 `handleGetClusterStats()` |
| `BrokerRegistration.java` | broker | `registerBroker()` 上报指标、Request DTO 扩展 |
| `MonitorServiceImpl.java` | console | 全量重写，NettyClient + 定时拉取 |

## 不在此次范围

- ConsumerGroup 详细数据
- 性能指标（读写延迟/缓存命中率）
- Topic 级别的消息量和 TPS
- Broker 端 DefaultMessageStore 的精确消息计数传入
