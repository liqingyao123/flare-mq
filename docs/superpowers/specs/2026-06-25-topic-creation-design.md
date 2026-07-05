# Topic 创建功能设计文档

**日期：** 2026-06-25
**状态：** 待实现

---

## 1. 背景与目标

当前项目在 topic 创建方面存在以下问题：

1. **Producer 无法触发自动创建**：Producer 发送消息到不存在的 topic 时，NameServer 返回空路由，Producer 直接抛异常。Broker 端虽然已有 `ensureTopicAndQueues()` 自动创建逻辑，但 Producer 根本到不了 Broker。
2. **NameServer 路由系统断裂**：`RouteInfoManager` 和 `ServiceRegistry` 是两套独立路由表，Broker 注册 topic 路由时只更新了 `RouteInfoManager`，但 Producer 查询路由走的是 `ServiceDiscovery` → `ServiceRegistry`，两个系统未同步。
3. **NameServer 多个 stub 方法**：`handleCreateTopic()`、`handleQueryTopic()`、`updateServiceRegistryRoute()` 均为空壳。
4. **Console 无 topic 管理 API**：Console 模块仅有只读监控报表，缺少 topic CRUD 能力。

**目标**：实现完整的 topic 创建能力，覆盖"自动创建"和"手动管理"两种场景。

---

## 2. 方案概述

参考 RocketMQ autoCreateTopic 设计，采用 **"默认 Topic 引导 + Broker 兜底创建"** 模型：

- Producer 查不到目标 topic 路由时，回退查 `default-topic` 的路由，拿到 Broker 地址后发送消息，Broker 端自动创建 topic 并注册到 NameServer。
- Console 新增 REST API，手动创建/查询/删除 topic，同样通过 `default-topic` 引导找到 Broker。
- NameServer 修复三个 stub，保证 ServiceRegistry 与 RouteInfoManager 数据同步。

---

## 3. 架构

```
                     ┌─────────────────────────┐
                     │      Console            │
                     │  (新增 Topic REST API)   │
                     │  POST /api/topics        │
                     │  GET  /api/topics        │
                     │  DELETE /api/topics/{n}  │
                     └───────────┬─────────────┘
                                 │ CREATE_TOPIC/DELETE_TOPIC/LIST_TOPICS 协议
                                 ▼
Producer ──→ NameServer ──→ Broker ──→ CommitLog/ConsumeQueue
  │              │              │
  │   GET_ROUTEINFO_BY_TOPIC   ├─ ensureTopicAndQueues()  ← 已有
  │   ┌─ 查目标topic路由       ├─ TopicManager.createTopic()
  │   └─ 空 → 回退查           ├─ TopicManager.deleteTopic()
  │        default-topic       └─ 注册/同步到 NameServer
  │
  │    修复：handleCreateTopic
  │    修复：handleQueryTopic
  │    修复：updateServiceRegistryRoute
```

### 三条链路

| 链路 | 触发方 | 路径 |
|------|--------|------|
| 自动创建 | Producer | Producer 路由回退 → Broker → ensureTopicAndQueues → 注册到 NameServer |
| 手动创建/删除 | Console | Console → NameServer(查 default-topic 路由) → Broker → TopicManager |
| 查询 | Console | Console → Broker → topicExists/listTopics |

---

## 4. 各模块改动

### 4.1 Producer — 路由回退（flare-mq-client）

**文件：** `ProducerImpl.java`

改动 `getTopicRouteInfo(topic)` 方法：

```
getTopicRouteInfo(topic):
  1. 查本地缓存 → 命中则返回
  2. 向 NameServer 发 GET_ROUTEINFO_BY_TOPIC_REQUEST
  3. 有数据 → 缓存并返回
  4. 空 → 回退（新增）:
     a. 若 topic == "default-topic" → 返回 null（防无限递归）
     b. 查 NameServer 获取 "default-topic" 路由
     c. 有 → 用此 Broker 地址去连
     d. 空 → 返回 null（集群不可用）
```

`send()` 调用链保持不变，路由回退对调用方透明。

### 4.2 NameServer — 三个 Stub 修复（flare-mq-nameserver）

**文件：** `NameServerRequestHandler.java`

#### 4.2.1 updateServiceRegistryRoute() （当前为空方法，Line 278）

补全实现，将 Broker 注册的 topic 路由写入 `ServiceRegistry.topicRouteTable`：

```
updateServiceRegistryRoute(brokerName, topicName, queueCount):
  1. 从 ServiceRegistry.brokerAddrTable 获取 brokerAddrs
  2. 构造 QueueData { brokerName, readQueueNums=queueCount, writeQueueNums=queueCount }
  3. 写入 ServiceRegistry.topicRouteTable:
       topicRouteTable.computeIfAbsent(topicName, k -> new ConcurrentHashMap<>())
                      .put(brokerName, queueDataList)
```

#### 4.2.2 handleCreateTopic() （当前空壳，Lines 144-162）

改为真正创建。NameServer 不直接创建 topic，而是转发给 Broker：

```
handleCreateTopic(request):
  1. 解析 request body → { topic, queueCount }
  2. 从 ServiceRegistry 或 default-topic 路由中选一台可用 Broker
  3. 转发 CREATE_TOPIC_REQUEST 给该 Broker
  4. 等待 Broker 返回结果
  5. 若 Broker 返回成功，更新本地 ServiceRegistry + RouteInfoManager
  6. 返回结果给调用方
```

#### 4.2.3 handleQueryTopic() （当前空壳，Lines 120-139）

改为真实查询：

```
handleQueryTopic(request):
  1. 解析 request body → { topic }
  2. 查 ServiceRegistry.topicRouteTable.containsKey(topic)
  3. 返回真实存在性 + 路由信息
```

### 4.3 Broker — 新增列表/删除能力（flare-mq-broker）

**文件：** `BrokerRequestHandler.java`

新增两个 handler：

```
handleListTopics():
  返回 topicManager.getAllTopics() → TopicConfig 列表

handleDeleteTopic(request):
  1. 解析 topic 名称
  2. topicManager.deleteTopic(topic)      ← 已有
  3. queueManager.deleteQueues(topic)    ← 新增方法
  4. 向 NameServer 发送取消路由注册请求
  5. 返回结果
```

**文件：** `QueueManager.java`

新增 `deleteQueues(topic)` 方法，清理 `queueConfigTable` 中该 topic 的所有队列。

**文件：** `MessageType.java`

新增 4 个协议类型：

```
DELETE_TOPIC_REQUEST  = 40
DELETE_TOPIC_RESPONSE = 41
LIST_TOPICS_REQUEST   = 42
LIST_TOPICS_RESPONSE  = 43
```

### 4.4 Console — REST API（flare-mq-console）

**技术选型：** JDK 内置 `com.sun.net.httpserver.HttpServer`，零额外依赖。

**新增文件：** `TopicApiHandler.java`
**修改文件：** `ConsoleApplication.java`（启动时注册 HttpServer）

#### API 端点

| 方法 | 路径 | 说明 | Request Body |
|------|------|------|-------------|
| POST | /api/topics | 创建 topic | `{"name":"order-topic","queues":8}` |
| GET | /api/topics?topic=xxx | 查询单个 topic | — |
| GET | /api/topics | 列出所有 topic | — |
| DELETE | /api/topics/{name} | 删除 topic | — |

#### 创建 topic 流程

```
Console POST /api/topics
  → 解析 JSON body
  → 校验 topic 名称非空、队列数 > 0
  → 查 NameServer default-topic 路由，选一台 Broker
  → 发送 CREATE_TOPIC_REQUEST
  → 返回 { "success": true, "topic": "order-topic" }
```

#### 查询/删除流程

同上，通过 NameServer 路由 → Broker 对应协议处理。

#### JSON 处理

使用 `ObjectMapper`（Jackson），项目 common 模块已依赖。

---

## 5. 错误处理

### 5.1 自动创建链路

| 场景 | 处理方式 |
|------|----------|
| NameServer 完全不可用 | Producer 已有重试逻辑，超时后抛异常 |
| default-topic 也没有路由 | 无可用 Broker，Producer 抛 "No route info found" |
| Broker 创建 topic 后注册 NameServer 失败 | topic 已在 Broker 本地创建成功，下次心跳周期重新推送 |
| 并发两个 Producer 同时发往新 topic | Broker 端 `ConcurrentHashMap.putIfAbsent` 保证幂等 |
| 创建成功但消息存储失败 | 返回 SEND_FAILURE，Producer 自行重试 |

### 5.2 手动创建链路

| 场景 | 处理方式 |
|------|----------|
| 请求 JSON 格式错误 | 400 + 错误描述 |
| topic 名称为空或非法 | 400，Broker 端 validate 校验 |
| topic 已存在 | 200，幂等返回成功 |
| 无可用的 Broker | 503 "No broker available" |
| 删除不存在的 topic | 200，幂等返回成功 |

### 5.3 一致性说明

Topic 创建基于最终一致性，不引入分布式事务：

```
Broker 创建成功 → 通知 NameServer 失败（网络抖动）
  → Broker 在下次心跳/同步周期重新推送注册信息
```

---

## 6. 边界情况

- **ConcurrentHashMap 并发安全**：TopicManager 和 NameServer 的 topic 路由表均使用 `ConcurrentHashMap`，并发创建同一个 topic 时只有一个生效。
- **default-topic 防递归**：Producer 路由回退时，若目标 topic 本身就是 default-topic 则不再回退。
- **Broker 重启**：Topic 仅存内存，重启后丢失。默认 topic 由 `initializeDefaultTopics()` 恢复，业务 topic 在下一次消息到达时自动重建。

---

## 7. 测试策略

### 7.1 单元测试（JUnit 5 + Mockito）

| 测试对象 | 测试点 |
|----------|--------|
| ProducerImpl.getTopicRouteInfo() | 路由存在直接返回 / 空路由回退查 default-topic / default-topic 也为空抛异常 |
| TopicManager | createTopic 正常流程 / 重复创建幂等 / topicName 为空校验 |
| NameServerRequestHandler | handleCreateTopic 转发 / handleQueryTopic 真实查询 / updateServiceRegistryRoute 同步写入 |
| Console TopicApiHandler | POST/GET/DELETE 正常流程 / 非法 body 返回 400 |
| ServiceRegistry | topicRouteTable 读写 / registerTopicRoute |

### 7.2 集成测试

| 测试 | 覆盖链路 |
|------|----------|
| Producer → 新 topic | 自动创建 → 消息发送成功 → NameServer 可查路由 |
| Console → Broker | 创建 topic → NameServer 两套路由表均有数据 → Producer 可发送 |
| Console → Broker 删除 | 删除成功 → NameServer 路由清除 |
| Broker 重启 | 默认 topic 恢复，业务 topic 重新自动创建 |

---

## 8. 改动范围汇总

| 模块 | 改动量 | 说明 |
|------|--------|------|
| flare-mq-client | 小 | ProducerImpl 路由回退 |
| flare-mq-nameserver | 中 | 修复 3 个 stub |
| flare-mq-broker | 中 | 新增 handleListTopics/handleDeleteTopic + 4 个协议类型 |
| flare-mq-protocol | 小 | MessageType 新增 4 个枚举值 |
| flare-mq-console | 中 | 新增 TopicApiHandler + 修改 ConsoleApplication |
