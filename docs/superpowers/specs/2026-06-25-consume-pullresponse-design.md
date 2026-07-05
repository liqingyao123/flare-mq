# Consumer PullResponse 解析修复

**日期：** 2026-06-25
**状态：** 待实现

---

## 1. 问题

`ConsumerImpl.handlePullResponse()`（Line 679-686）在收到 Broker 的 SUCCESS 响应时，始终返回 `PullResult.noNewMessage(0, 0, 0)`，完全丢弃了 Broker 返回的消息数据和偏移量信息。

## 2. 方案

解析 Broker 返回的 JSON 响应 body，有消息时返回 `PullResult.found()`，无消息时返回 `PullResult.noNewMessage()`，并正确传递 offset 信息。

## 3. 改动

**文件：** `flare-mq-client/src/main/java/com/ruyuan/mq/client/consumer/ConsumerImpl.java`

- 重写 `handlePullResponse()` 方法：解析 JSON → 转换消息 → 构造正确 PullResult
- 新增两个内部静态 DTO：`PullResponseDTO`、`SimpleMessageDTO`

## 4. 边界情况

| 场景 | 返回 |
|------|------|
| body 为空 | `noNewMessage(0,0,0)` |
| JSON 解析失败 | `noNewMessage(0,0,0)` |
| messages 为空 | `noNewMessage(nextBeginOffset, minOffset, maxOffset)` |
| messages 有数据 | `found(nextBeginOffset, minOffset, maxOffset, messages)` |
