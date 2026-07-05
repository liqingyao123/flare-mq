# Topic CRUD Real Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire Console topic create/delete to real NameServer→Broker interaction instead of fake success response

**Architecture:** Console sends CREATE_TOPIC_REQUEST / DELETE_TOPIC_REQUEST to NameServer via NettyClient → NameServer forwards to the appropriate Broker → returns result to Console

**Tech Stack:** Java 8, Netty, Spring Boot 3.2.5

## Global Constraints

- Console requests go through NameServer (never directly to Broker)
- NameServer already handles CREATE_TOPIC_REQUEST — Console only needs to send the request
- NameServer needs a new DELETE_TOPIC_REQUEST handler that forwards to the Broker that owns the topic
- Request JSON format for create: `{"topic":"...", "queueCount": N}`
- Request JSON format for delete: `{"topic":"..."}`
- No changes to Broker code needed (both handlers already exist)

---

### Task 1: MonitorService 接口 + MonitorServiceImpl 实现

**Files:**
- Modify: `flare-mq-console/src/main/java/com/flare/mq/console/service/MonitorService.java`
- Modify: `flare-mq-console/src/main/java/com/flare/mq/console/service/impl/MonitorServiceImpl.java`

**Interfaces:**
- Consumes: NameServer NettyClient (already connected in MonitorServiceImpl)
- Produces: `boolean createTopic(String topicName, int queueCount)`, `boolean deleteTopic(String topicName)`

- [ ] **Step 1: MonitorService 接口新增方法声明**

在 `getConsumerGroupStatusList()` 之后新增：

```java
    /**
     * 创建 Topic（通过 NameServer 转发到 Broker）
     */
    boolean createTopic(String topicName, int queueCount);

    /**
     * 删除 Topic（通过 NameServer 转发到 Broker）
     */
    boolean deleteTopic(String topicName);
```

- [ ] **Step 2: MonitorServiceImpl 新增 createTopic 实现**

在 `fetchConsumerGroups()` 方法之后新增：

```java
    @Override
    public boolean createTopic(String topicName, int queueCount) {
        if (!connected || nettyClient == null || !nettyClient.isConnected()) {
            logger.warn("Cannot create topic: not connected to NameServer");
            return false;
        }
        try {
            Map<String, Object> req = new HashMap<>();
            req.put("topic", topicName);
            req.put("queueCount", queueCount);
            String json = JsonUtils.toJson(req);

            ProtocolMessage request = new ProtocolMessage(
                    MessageType.CREATE_TOPIC_REQUEST,
                    json.getBytes(StandardCharsets.UTF_8));
            ProtocolMessage response = nettyClient.sendSync(request, 5000);

            if (response != null && response.getStatus() == ResponseCode.SUCCESS) {
                logger.info("Topic created successfully: topic={}, queueCount={}", topicName, queueCount);
                return true;
            }
            logger.warn("Failed to create topic: topic={}, status={}",
                    topicName, response != null ? response.getStatus() : "null");
            return false;
        } catch (Exception e) {
            logger.error("Error creating topic: topic=" + topicName, e);
            return false;
        }
    }
```

需要新增 import：
```java
import com.flare.mq.protocol.ResponseCode;
```

- [ ] **Step 3: MonitorServiceImpl 新增 deleteTopic 实现**

在 `createTopic` 方法之后新增：

```java
    @Override
    public boolean deleteTopic(String topicName) {
        if (!connected || nettyClient == null || !nettyClient.isConnected()) {
            logger.warn("Cannot delete topic: not connected to NameServer");
            return false;
        }
        try {
            Map<String, Object> req = new HashMap<>();
            req.put("topic", topicName);
            String json = JsonUtils.toJson(req);

            ProtocolMessage request = new ProtocolMessage(
                    MessageType.DELETE_TOPIC_REQUEST,
                    json.getBytes(StandardCharsets.UTF_8));
            ProtocolMessage response = nettyClient.sendSync(request, 5000);

            if (response != null && response.getStatus() == ResponseCode.SUCCESS) {
                logger.info("Topic deleted successfully: topic={}", topicName);
                return true;
            }
            logger.warn("Failed to delete topic: topic={}, status={}",
                    topicName, response != null ? response.getStatus() : "null");
            return false;
        } catch (Exception e) {
            logger.error("Error deleting topic: topic=" + topicName, e);
            return false;
        }
    }
```

- [ ] **Step 4: 编译验证**

```bash
mvn compile -pl flare-mq-console -am -q
```

- [ ] **Step 5: Commit**

```bash
git add flare-mq-console/src/main/java/com/flare/mq/console/service/MonitorService.java
git add flare-mq-console/src/main/java/com/flare/mq/console/service/impl/MonitorServiceImpl.java
git commit -m "feat: add real createTopic and deleteTopic to MonitorService"
```

---

### Task 2: MonitorController 改为调用 service

**Files:**
- Modify: `flare-mq-console/src/main/java/com/flare/mq/console/controller/MonitorController.java`

**Interfaces:**
- Consumes: `MonitorService.createTopic(String, int)` → `boolean`, `MonitorService.deleteTopic(String)` → `boolean` (from Task 1)

- [ ] **Step 1: 重写 createTopic 方法**

替换 `POST /api/topics` 方法体：

```java
    /** POST /api/topics — 新建 Topic */
    @PostMapping("/topics")
    public ResponseEntity<Map<String, Object>> createTopic(@RequestBody Map<String, Object> body) {
        String name = (String) body.get("name");
        int queueCount = body.get("queueCount") instanceof Number
                ? ((Number) body.get("queueCount")).intValue() : 4;

        if (name == null || name.trim().isEmpty()) {
            return ResponseEntity.badRequest().body(error("Topic name is required"));
        }

        boolean ok = monitorService.createTopic(name, queueCount);
        Map<String, Object> result = new HashMap<>();
        result.put("success", ok);
        result.put("topic", name);
        result.put("queueCount", queueCount);
        if (!ok) {
            result.put("error", "Failed to create topic via NameServer");
        }
        return ResponseEntity.ok(result);
    }
```

- [ ] **Step 2: 重写 deleteTopic 方法**

替换 `DELETE /api/topics/{name}` 方法体：

```java
    /** DELETE /api/topics/{name} — 删除 Topic */
    @DeleteMapping("/topics/{name}")
    public ResponseEntity<Map<String, Object>> deleteTopic(@PathVariable String name) {
        if (name == null || name.trim().isEmpty()) {
            return ResponseEntity.badRequest().body(error("Topic name is required"));
        }

        boolean ok = monitorService.deleteTopic(name);
        Map<String, Object> result = new HashMap<>();
        result.put("success", ok);
        result.put("topic", name);
        if (!ok) {
            result.put("error", "Failed to delete topic via NameServer");
        }
        return ResponseEntity.ok(result);
    }
```

- [ ] **Step 3: 编译验证**

```bash
mvn compile -pl flare-mq-console -am -q
```

- [ ] **Step 4: Commit**

```bash
git add flare-mq-console/src/main/java/com/flare/mq/console/controller/MonitorController.java
git commit -m "feat: wire topic create/delete to real NameServer calls"
```

---

### Task 3: NameServer 新增 DELETE_TOPIC handler

**Files:**
- Modify: `flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/NameServerRequestHandler.java`

**Interfaces:**
- Consumes: `routeInfoManager.getTopicRouteInfo(String topic)` → `TopicRouteInfo`
- Produces: `handleDeleteTopic(ProtocolMessage)` → handles DELETE_TOPIC_REQUEST, forwards to Broker

- [ ] **Step 1: 在 switch 中新增 case**

在 `case CREATE_TOPIC_REQUEST:` 之后新增：

```java
                case DELETE_TOPIC_REQUEST:
                    return handleDeleteTopic(request);
```

- [ ] **Step 2: 新增 handleDeleteTopic 方法**

在 `handleCreateTopic()` 方法之后新增：

```java
    private ProtocolMessage handleDeleteTopic(ProtocolMessage request) {
        logger.info("Handling delete topic request: {}", request.getRequestId());

        try {
            byte[] body = request.getBody();
            if (body == null || body.length == 0) {
                return ProtocolMessage.createErrorResponse(
                        MessageType.DELETE_TOPIC_RESPONSE,
                        request.getRequestId(),
                        ResponseCode.BAD_REQUEST);
            }

            String json = new String(body, StandardCharsets.UTF_8);
            DeleteTopicRequest deleteReq = JsonUtils.fromJson(json, DeleteTopicRequest.class);
            if (deleteReq == null || deleteReq.topic == null || deleteReq.topic.trim().isEmpty()) {
                return ProtocolMessage.createErrorResponse(
                        MessageType.DELETE_TOPIC_RESPONSE,
                        request.getRequestId(),
                        ResponseCode.BAD_REQUEST);
            }

            // 通过 topic 路由查找 Broker
            TopicRouteInfo routeInfo = routeInfoManager.getTopicRouteInfo(deleteReq.topic);
            if (routeInfo == null || !routeInfo.hasBrokerRoutes()) {
                logger.warn("No route info for topic: {}, cannot delete", deleteReq.topic);
                return ProtocolMessage.createErrorResponse(
                        MessageType.DELETE_TOPIC_RESPONSE,
                        request.getRequestId(),
                        ResponseCode.NOT_FOUND);
            }

            // 取第一台 Broker
            String brokerName = routeInfo.getBrokerRoutes().keySet().iterator().next();
            BrokerData brokerData = serviceRegistry.getBrokerData(brokerName);
            if (brokerData == null || !brokerData.getBrokerAddrs().containsKey(0L)) {
                return ProtocolMessage.createErrorResponse(
                        MessageType.DELETE_TOPIC_RESPONSE,
                        request.getRequestId(),
                        ResponseCode.INTERNAL_ERROR);
            }

            String brokerAddr = brokerData.getBrokerAddrs().get(0L);
            String[] parts = brokerAddr.split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 10911;

            // 向 Broker 发送删除请求
            com.flare.mq.protocol.client.NettyClient brokerClient =
                    new com.flare.mq.protocol.client.NettyClient(host, port);
            brokerClient.connect();

            try {
                ProtocolMessage brokerRequest = new ProtocolMessage(
                    MessageType.DELETE_TOPIC_REQUEST,
                    json.getBytes(StandardCharsets.UTF_8));
                ProtocolMessage brokerResponse = brokerClient.sendSync(brokerRequest, 5000);

                return brokerResponse != null ? brokerResponse :
                    ProtocolMessage.createErrorResponse(
                        MessageType.DELETE_TOPIC_RESPONSE, request.getRequestId(),
                        ResponseCode.INTERNAL_ERROR);
            } finally {
                brokerClient.disconnect();
            }

        } catch (Exception e) {
            logger.error("Handle delete topic error, requestId=" + request.getRequestId(), e);
            return ProtocolMessage.createErrorResponse(
                    MessageType.DELETE_TOPIC_RESPONSE,
                    request.getRequestId(),
                    ResponseCode.INTERNAL_ERROR);
        }
    }
```

- [ ] **Step 3: 新增 DeleteTopicRequest DTO**

在文件末尾的内部类区域新增：

```java
    static class DeleteTopicRequest {
        public String topic;
    }
```

- [ ] **Step 4: 编译验证**

```bash
mvn compile -pl flare-mq-nameserver -am -q
```

- [ ] **Step 5: Commit**

```bash
git add flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/NameServerRequestHandler.java
git commit -m "feat: add delete topic handler to NameServer with broker forwarding"
```
