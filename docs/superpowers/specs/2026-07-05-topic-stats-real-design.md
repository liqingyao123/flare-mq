# Topic 页面真实数据修复

2026-07-05 | 修复 Topic 页面两个 bug | 5 文件

## 背景

Topic 页面有两个 bug：
1. 只展示 NameServer 内存中活跃的 topic（重启丢失），持久化的 topic 不展示
2. 每个 topic 的消息数始终为 0（硬编码）

根因：Broker 注册时未上报 topic 列表和 per-topic 消息数。

## 数据流

```
Broker (每 30s)
  → DefaultMessageStore.getTopicMessageCounts()
    遍历 ConsumeQueueManager 所有队列，按 topic 汇总 maxOffset
  → RegisterBrokerRequest.topicStats: [ {topicName, queueCount, messageCount} ]
  → NameServer REGISTER_BROKER_REQUEST

NameServer (handleGetClusterStats)
  → 合并 BrokerData 中的 topicStats
  → JSON: { topics: [{topicName, queueCount, messageCount}] }

Console (fetchClusterStats → getTopicStatsList)
  → 解析 JSON → TopicStats 对象，真实消息数
```

## 改动清单

### 1. DefaultMessageStore — 新增 getTopicMessageCounts()

```java
public Map<String, long[]> getTopicMessageCounts() {
    // 返回 Map<topicName, [queueCount, messageCount]>
    // 遍历 consumeQueueManager.getAllQueueKeys() 按 topic 汇总
}
```

### 2. BrokerRegistration — registerBroker() 上报 topic 数据

- RegisterBrokerRequest DTO 新增字段：`public List<TopicStatEntry> topicStats;`
- TopicStatEntry 内部类：`topicName`, `queueCount`, `messageCount`
- `registerBroker()` 中从 `messageStore.getTopicMessageCounts()` 采集并设置

### 3. NameServer ServiceRegistry — 存储 per-topic 数据

- BrokerData 新增字段：`Map<String, TopicStatEntry> topicStats`
- `registerBroker()` 时存储

### 4. NameServer handleGetClusterStats — 返回 per-topic 数据

- 从 BrokerData.topicStats 聚合所有 topic，取代仅从 routeInfoManager 读取
- 响应中 topics 数组每项包含：topicName, queueCount, messageCount

### 5. Console MonitorServiceImpl — getTopicStatsList 使用真实数据

- 读取 topics JSON 中 messageCount 字段，替换硬编码的 `setTotalMessages(0)`

## 不在此次范围

- Topic 级别的 TPS（需要额外追踪）
- 跨 Broker 同名 topic 去重合并
