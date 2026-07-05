# Topic 创建/删除打通 设计

2026-07-05 | 打通 Topic CRUD 真实交互 | 4 文件

## 背景

Console 的 Topic 创建/删除当前是假接口，直接返回 `{"success": true}` 不跟后端交互。
需要改为通过 NameServer 转发到 Broker 执行真实操作。

## 数据流

```
创建 Topic:
  Console ──CREATE_TOPIC_REQUEST──→ NameServer
                                      │ 1. 通过 default-topic 找 Broker
                                      │ 2. 随机选一台
                                      ▼
                                    Broker (handleCreateTopic)
                                      │ ensureTopicAndQueues()
                                      │ topicManager.createTopic()
                                      │   → registerTopicRoute() 注册到 NameServer
                                      ▼
                                    ← RESPONSE ── Console (refresh)

删除 Topic:
  Console ──DELETE_TOPIC_REQUEST──→ NameServer
                                      │ 1. 通过 topic 路由找 Broker
                                      │ 2. 转发请求
                                      ▼
                                    Broker (handleDeleteTopic)
                                      │ queueManager.deleteQueuesForTopic()
                                      │ topicManager.deleteTopic()
                                      │   → deregisterTopicRoute() 通知 NameServer 清理
                                      ▼
                                    ← RESPONSE ── Console (refresh)
```

NameServer 创建已有完整实现，无需改动。删除需要在 NameServer 新增 handler。

## 改动清单

### 1. MonitorService 接口 — 新增两个方法

```java
boolean createTopic(String topicName, int queueCount);
boolean deleteTopic(String topicName);
```

### 2. MonitorServiceImpl — 实现真实调用

- `createTopic(name, queueCount)`：构造 `{"topic":"...", "queueCount": N}` JSON，发 `CREATE_TOPIC_REQUEST` 到 NameServer，解析响应返回 true/false
- `deleteTopic(name)`：构造 `{"topic":"..."}` JSON，发 `DELETE_TOPIC_REQUEST` 到 NameServer，解析响应返回 true/false

### 3. MonitorController — 调用 service

- `POST /api/topics`：校验参数 → `monitorService.createTopic(name, queueCount)` → 返回真实结果
- `DELETE /api/topics/{name}`：`monitorService.deleteTopic(name)` → 返回真实结果

### 4. NameServerRequestHandler — 新增 handleDeleteTopic

```java
case DELETE_TOPIC_REQUEST:
    return handleDeleteTopic(request);
```

实现逻辑：
1. 解析 JSON，拿到 topic 名称
2. 通过 `routeInfoManager.getTopicRouteInfo(topic)` 找到该 topic 的 Broker
3. 向 Broker 发送 DELETE_TOPIC_REQUEST
4. 返回 Broker 的响应给 Console

## 不在此次范围

- Topic 配置修改（权限、队列数调整）
- 批量删除 Topic
- 删除保护（如 topic 有消息时拒绝删除）
