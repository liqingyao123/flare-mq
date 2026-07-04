# Producer 可靠发送设计

2026-07-04 | 金融交易场景 | 方案 A — 基础可靠性

## 背景

当前 `ProducerImpl` 已具备发送框架（同步/异步/单向发送、路由发现、统计），但存在以下关键缺口：

1. **重试未实现** — `ProducerConfig` 配置了 `retryTimesWhenSendFailed=2`，但 `send()` 失败时直接抛异常，不重试
2. **无 Broker 故障转移** — 选中 Broker 不可达时直接报错，不会换一个 Broker
3. **SendResult 信息残缺** — Broker 只返回 `"OK"` 字符串，Producer 端硬编码 `queueId=0`
4. **sendOneway 静默丢消息** — 发送失败只记日志，调用方无法感知

## 目标

- 同步/异步发送：失败自动重试（限可重试错误），超时次数用尽再返回错误
- Broker 故障转移：一个 Broker 失败时自动换另一个
- 结果完整性：`SendResult` 包含真实的 queueId、offset、brokerAddr
- 不改变现有 Producer API 接口签名

## 模块一：发送重试

### 改动文件

`ProducerImpl.java`

### 重试流程（同步发送）

```
send(message, timeoutMs)
  ├─ checkProducerStatus()
  ├─ validateMessage()
  ├─ getTopicRouteInfo()            ← 仅获取一次，重试期间复用
  └─ for retryCount = 0..retryTimesWhenSendFailed:
       ├─ routeInfo.selectQueue(retryCount, excludeBrokerName)
       ├─ getBrokerClient() → 发送 → handleSendResponse()
       ├─ result.isSuccess()? → return result
       ├─ result.needRetry() && retryCount < maxRetries?
       │    ├─ excludeBrokerName = result.brokerAddr 对应的 broker
       │    └─ continue
       └─ else → return result（重试用尽或不可重试）
```

### 可重试 vs 不可重试

| 错误条件 | 可重试 |
|---------|--------|
| `SEND_TIMEOUT` | 是 |
| `FLUSH_DISK_TIMEOUT` / `FLUSH_SLAVE_TIMEOUT` / `SLAVE_NOT_AVAILABLE` | 是 |
| 网络异常（连接断开、ConnectException） | 是 |
| `SEND_FAILED`（Broker 明确拒绝） | 否 |
| 消息格式错误、Topic 不存在 | 否 |

### 异步发送

`sendAsync()` 同理，在 `ResponseCallback.onFailure()` 中触发重试，使用 `retryTimesWhenSendAsyncFailed` 配置。重试逻辑与同步路径共享同一个 private 方法。

### 配置

复用已有配置项，默认值不变：

- `retryTimesWhenSendFailed` = 2
- `retryTimesWhenSendAsyncFailed` = 2

## 模块二：Broker 故障转移

### 改动文件

- `TopicRouteInfo.java` — 新增排除方法
- `ProducerImpl.java` — 整合到重试流程

### TopicRouteInfo 新增方法

```java
// 选择一个队列，排除指定 brokerName（仅排除，不排除 queueId）
// 如果所有队列都属于该 broker，返回 null（单点，无转移目标）
public QueueInfo selectAnotherQueue(String excludeBrokerName)
```

实现：遍历 `queueInfos`，过滤 `writable && !brokerName.equals(excludeBrokerName)`，对过滤结果进行轮询选择。

### 转移流程

```
retry > 0:
  ├─ excludeBrokerName = 上一次失败的 broker
  ├─ queueInfo = routeInfo.selectAnotherQueue(excludeBrokerName)
  ├─ if queueInfo == null → 无备选 Broker → 用原 queueInfo 重试
  ├─ 从 brokerClients 中清除失败 broker 的缓存连接（避免复用死连接）
  └─ getBrokerClient() → 建立新连接 → 发送
```

### 边界情况

- **只有一个 Broker**：`selectAnotherQueue` 返回 null → 仍重试同一 Broker（连接临时断开会恢复）
- **所有 Broker 都不通**：重试用尽 → 返回最终错误
- **Broker 新加入集群**：下次 `getTopicRouteInfo()` 时刷新路由列表覆盖

## 模块三：Broker 响应增强

### 改动文件

- `BrokerRequestHandler.java` — 返回结构化 JSON 响应
- `ProducerImpl.java` — 解析结构化响应

### Broker 端

```java
// 当前：return ProtocolMessage.createSuccessResponse(..., "OK".getBytes());
// 改为：
SendResponse resp = new SendResponse();
resp.messageId = storeMsg.getMessageId();
resp.queueId = queueId;
resp.offset = putRes.getAppendResult().getWroteOffset();
resp.topic = storeMsg.getTopic();
return ProtocolMessage.createSuccessResponse(..., JsonUtils.toJson(resp).getBytes());
```

响应 JSON 结构：
```json
{"messageId":"xxx","queueId":2,"offset":1048576,"topic":"test"}
```

### Producer 端

`handleSendResponse()` 解析 JSON 响应，填充 `SendResult`：

```java
// 当前：SendResult.success(message.getMessageId(), 0, System.currentTimeMillis());
// 改为：SendResult.success(responseParsed.messageId, responseParsed.queueId, responseParsed.offset)
//       然后 setBrokerAddr(...)
```

同时去除现有的 `queueId=0` 硬编码。

## 改动清单

| 文件 | 改动内容 | 行数估计 |
|------|---------|---------|
| `ProducerImpl.java` | 重试循环、故障转移、响应解析、死连接清理 | ~120 |
| `TopicRouteInfo.java` | `selectAnotherQueue(excludeBrokerName)` | ~20 |
| `BrokerRequestHandler.java` | 返回结构化 SendResponse JSON | ~15 |
| `SendResult.java` | 无结构改动，确保 brokerAddr 被赋值 | ~5 |

## 不在此次范围

- 同步刷盘（Broker 端 `putMessage` 后等待 `mappedFile.force()`）
- 发送方幂等（sequence number + Broker 去重）
- `sendOneway` 可靠化（与方案 A 冲突 — oneway 语义就是不关心结果）
- 事务消息
- 批量发送的完整实现（配置项已有但未实现）

## 测试要点

- 单 Broker 场景下重试 N 次的预期行为
- 多 Broker 场景下故障转移：第一个 Broker 失败 → 自动切换到第二个
- 超时场景下的重试与最终超时错误
- Broker 返回不可重试错误时不重试（如消息格式错误）
- `SendResult` 中 queueId、offset、brokerAddr 均为真实值而非硬编码
- Broker 响应 JSON 解析异常时的降级处理
