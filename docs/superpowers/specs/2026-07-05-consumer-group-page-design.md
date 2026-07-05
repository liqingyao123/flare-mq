# ConsumerGroup 页面及消费进度监控设计

2026-07-05 | 新增 ConsumerGroup 管理页面 | 4 模块 8 文件

## 背景

Console 目前有 OverView/Broker/Topic 三个标签页，数据均来自 NameServer 的 GET_CLUSTER_STATS_REQUEST。
缺少对消费者组的可视化监控，无法查看各组的消费进度、队列堆积情况。

## 数据流

```
Broker (每 30s)
  ├─ ConsumerOffsetManager.getAllOffsets()                  → 各队列消费偏移
  ├─ DefaultMessageStore.getMaxOffset(topic, queueId)       → 队列最大偏移
  ├─ lag = maxOffset - consumedOffset
  ├─ consumeTps = delta(totalConsumed) / 30.0s
  └─ REPORT_CONSUMER_GROUP_STATS_REQUEST ──→ NameServer

Console (每 5s)
  └─ GET_CONSUMER_GROUPS_REQUEST ──→ NameServer
                                       │
                  ┌────────────────────┘
                  │ NameServer 合并两路数据：
                  │  · Broker 上报的队列消费进度
                  │  · ServiceRegistry 的消费者心跳数据
                  ▼
              GET_CONSUMER_GROUPS_RESPONSE (JSON)
                  │
                  ▼
            Console MonitorServiceImpl → MonitorController → Vue UI
```

## 协议层

### MessageType.java 新增

```java
REPORT_CONSUMER_GROUP_STATS_REQUEST(72),
REPORT_CONSUMER_GROUP_STATS_RESPONSE(73),
GET_CONSUMER_GROUPS_REQUEST(74),
GET_CONSUMER_GROUPS_RESPONSE(75);
```

### Broker → NameServer 上报 JSON

```json
{
  "groupName": "example_consumer_group",
  "topic": "QuickStartTopic",
  "queueStats": [
    { "queueId": 0, "maxOffset": 500, "consumedOffset": 420 },
    { "queueId": 1, "maxOffset": 500, "consumedOffset": 350 }
  ],
  "consumeTps": 12.5
}
```

### NameServer → Console 响应 JSON

```json
{
  "consumerGroups": [
    {
      "groupName": "example_consumer_group",
      "topic": "QuickStartTopic",
      "consumerCount": 2,
      "activeConsumers": 1,
      "status": "ACTIVE",
      "consumers": [
        { "consumerId": "client-1", "lastHeartbeat": 1720060200000 }
      ],
      "queueStats": [
        { "queueId": 0, "maxOffset": 500, "consumedOffset": 420, "lag": 80 },
        { "queueId": 1, "maxOffset": 500, "consumedOffset": 350, "lag": 150 }
      ],
      "totalConsumed": 770,
      "totalLag": 230,
      "consumeTps": 12.5
    }
  ]
}
```

**状态判定：** `activeConsumers > 0` → ACTIVE，否则 INACTIVE。

## Broker 侧

### BrokerRegistration 新增依赖

- `ConsumerOffsetManager` — 获取各队列消费偏移
- `DefaultMessageStore` — 获取各队列 maxOffset（已有）
- 每 30s 注册时，额外采集消费组数据并发独立上报

采集逻辑：

```
1. 遍历 ConsumerOffsetManager.getAllOffsets()
2. key 格式 "group@topic@queueId" → 解析出 groupName, topic, queueId
3. 按 groupName 分组聚合
4. 每个 Queue 调用 messageStore.getMaxOffset(topic, queueId) 获取 maxOffset
5. 计算 lag = maxOffset - consumedOffset
6. 对比上次上报的 totalConsumed，计算 consumeTps = delta / 30s
7. 组装 JSON 发送 REPORT_CONSUMER_GROUP_STATS_REQUEST
```

### ClusterManager

传递 `offsetManager` 给 `BrokerRegistration`（类似之前传递 `messageStore`）。

## NameServer 侧

### ServiceRegistry 新增

```java
// 存储 Broker 上报的消费组统计，按 brokerName → groupName → ConsumerGroupStats
private final ConcurrentHashMap<String, ConcurrentHashMap<String, ConsumerGroupStats>> consumerGroupStatsTable;

public void updateConsumerGroupStats(String brokerName, ConsumerGroupStats stats);
public List<ConsumerGroupStats> getAllConsumerGroupStats();
```

`ConsumerGroupStats` 内部类：groupName, topic, queueStats, consumeTps, lastUpdateTimestamp。

### NameServerRequestHandler 新增两个 handler

- `handleReportConsumerGroupStats()` — 接收 Broker 上报，存入 ServiceRegistry
- `handleGetConsumerGroups()` — 查询：合并 Broker 上报的消费进度 + ServiceRegistry 的消费者心跳，组装 JSON 返回

## Console 侧

### MonitorServiceImpl

- 新增 `fetchConsumerGroups()` — 向 NameServer 发 GET_CONSUMER_GROUPS_REQUEST
- 缓存 `cachedConsumerGroups`
- `getConsumerGroupStatusList()` 从缓存转换返回

### MonitorController

```java
@GetMapping("/consumers")
public List<ConsumerGroupStatus> getConsumerGroups()
```

### Model 变更 — ConsumerGroupStatus 扩展

现有字段保留，新增：
- `List<QueueStat> queues` — 每队列消费进度
- `List<ConsumerInfo> consumers` — 消费者实例列表

### 前端 — index.html

新增第四个标签页「消费者」：

```
┌──────────────────────────────────────────────────┐
│ [概览] [Broker] [Topic] [消费者]                  │
├──────────────────────────────────────────────────┤
│ 组名 │ Topic │ 消费者 │ 堆积 │ TPS  │ 状态       │
│ demo │ Quick │ 1/2    │ 230  │ 12.5 │ ACTIVE    │
│      │ Start │        │      │      │           │
├──────┴───────┴────────┴──────┴──────┴───────────┤
│ ▼ 展开详情                                      │
│   队列消费进度：                                 │
│   Queue  │ MaxOffset │ Consumed │ Lag │ 进度%   │
│   0      │ 500       │ 420      │ 80  │ 84%     │
│   1      │ 500       │ 350      │ 150 │ 70%     │
│                                                 │
│   消费者实例：                                   │
│   Consumer ID  │ 最近心跳             │ 状态    │
│   client-1     │ 2026-07-05 12:00:00 │ 在线    │
│   client-2     │ 2026-07-05 11:59:30 │ 离线    │
└──────────────────────────────────────────────────┘
```

5 秒轮询刷新，点击行展开/折叠详情。

## 改动清单

| 文件 | 模块 | 改动 |
|------|------|------|
| `MessageType.java` | protocol | +4 枚举值 |
| `BrokerRegistration.java` | broker | 注入 offsetManager，实现消费组统计采集上报 |
| `ClusterManager.java` | broker | 传递 offsetManager 给 BrokerRegistration |
| `ServiceRegistry.java` | nameserver | 新增 consumerGroupStatsTable 及存取方法 |
| `NameServerRequestHandler.java` | nameserver | 新增 2 个 handler |
| `MonitorServiceImpl.java` | console | 新增 fetchConsumerGroups + 缓存 + getConsumerGroupStatusList 实现 |
| `MonitorController.java` | console | 新增 GET /api/consumers |
| `index.html` | console | 新增消费者标签页 |

## 不在此次范围

- Consumer 负载均衡 / Rebalance 可视化
- 消费历史趋势图（ECharts）
- 消费告警（堆积超过阈值自动告警）
- 多 Topic 订阅（当前简化为一组一个 Topic）
