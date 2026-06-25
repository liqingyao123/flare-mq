# Topic 创建功能实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现完整的 topic 创建能力：Producer 自动创建（路由回退 + Broker 兜底）+ Console REST API 手动管理 + NameServer 路由同步修复。

**Architecture:** Producer 查不到目标 topic 路由时回退查 default-topic 路由，连上 Broker 后由 Broker 端 `ensureTopicAndQueues()` 自动创建。Console 同理通过 NameServer 发现 Broker 后发送创建/查询/删除协议。NameServer 修复三个 stub 使两套路由系统同步。

**Tech Stack:** Java 8, Netty, JUnit 5, Mockito, JDK HttpServer, Jackson (ObjectMapper)

## Global Constraints

- Java 8 兼容
- 不引入新的外部依赖（Console HTTP 使用 JDK 内置 HttpServer）
- Topic 仅存内存，不持久化
- 遵循现有代码风格：JSON 序列化用 JsonUtils，DTO 用 public fields
- 最终一致性，不引入分布式事务

---

### Task 1: MessageType — 新增 4 个协议类型

**Files:**
- Modify: `ruyuan-mq-protocol/src/main/java/com/ruyuan/mq/protocol/MessageType.java`

**Interfaces:**
- Produces: `DELETE_TOPIC_REQUEST(40)`, `DELETE_TOPIC_RESPONSE(41)`, `LIST_TOPICS_REQUEST(42)`, `LIST_TOPICS_RESPONSE(43)` — 供 Broker 和 Nameserver 的 switch 分支使用

- [ ] **Step 1: 在枚举中添加 4 个新值**

在 `REGISTER_BROKER_RESPONSE((short) 39);` 之后，结束分号之前添加：

```java
    /**
     * 删除Topic请求
     */
    DELETE_TOPIC_REQUEST((short) 40),

    /**
     * 删除Topic响应
     */
    DELETE_TOPIC_RESPONSE((short) 41),

    /**
     * 列出所有Topic请求
     */
    LIST_TOPICS_REQUEST((short) 42),

    /**
     * 列出所有Topic响应
     */
    LIST_TOPICS_RESPONSE((short) 43);
```

- [ ] **Step 2: 更新 `isRequest()` 方法**

当前 `isRequest()` 列举了所有请求类型。需要在返回表达式中加入 4 个新类型的判断，同时加入已遗漏的 `REGISTER_BROKER_REQUEST`：

```java
    public boolean isRequest() {
        return this == REQUEST || this == HEARTBEAT_REQUEST ||
               this == SEND_MESSAGE_REQUEST || this == PULL_MESSAGE_REQUEST ||
               this == ACK_MESSAGE_REQUEST || this == CREATE_TOPIC_REQUEST ||
               this == QUERY_TOPIC_REQUEST || this == GET_ROUTEINFO_BY_TOPIC_REQUEST ||
               this == REGISTER_TOPIC_ROUTE_REQUEST || this == REGISTER_BROKER_REQUEST ||
               this == DELETE_TOPIC_REQUEST || this == LIST_TOPICS_REQUEST;
    }
```

- [ ] **Step 3: 验证编译通过**

```bash
mvn compile -pl ruyuan-mq-protocol -am -q
```

Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add ruyuan-mq-protocol/src/main/java/com/ruyuan/mq/protocol/MessageType.java
git commit -m "feat: add DELETE_TOPIC and LIST_TOPICS message types"
```

---

### Task 2: ServiceRegistry — 新增单个 topic 路由注册方法

**Files:**
- Modify: `ruyuan-mq-nameserver/src/main/java/com/ruyuan/mq/nameserver/registry/ServiceRegistry.java`

**Interfaces:**
- Produces: `public void registerTopicRoute(String brokerName, String topicName, int readQueueNums, int writeQueueNums, int perm)` — 供 NameServerRequestHandler.updateServiceRegistryRoute 调用
- Consumes: 无

- [ ] **Step 1: 添加公开的 registerTopicRoute 方法**

在 `ServiceRegistry.java` 中，`getTopicRouteData()` 方法之后（或 `updateTopicRouteInfo` 私有方法附近），添加：

```java
    /**
     * 注册单个Topic的路由信息
     */
    public void registerTopicRoute(String brokerName, String topicName,
                                    int readQueueNums, int writeQueueNums, int perm) {
        lock.writeLock().lock();
        try {
            TopicRouteData topicRouteData = topicRouteTable.get(topicName);
            if (topicRouteData == null) {
                topicRouteData = new TopicRouteData();
                topicRouteTable.put(topicName, topicRouteData);
            }

            QueueData queueData = new QueueData(brokerName, readQueueNums, writeQueueNums, perm);

            // 替换同 broker 的旧数据
            topicRouteData.getQueueDatas().removeIf(qd -> qd.getBrokerName().equals(brokerName));
            topicRouteData.getQueueDatas().add(queueData);

            logger.info("Registered topic route in ServiceRegistry: topic={}, broker={}, readQueues={}, writeQueues={}",
                       topicName, brokerName, readQueueNums, writeQueueNums);
        } finally {
            lock.writeLock().unlock();
        }
    }
```

- [ ] **Step 2: 验证编译通过**

```bash
mvn compile -pl ruyuan-mq-nameserver -am -q
```

Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add ruyuan-mq-nameserver/src/main/java/com/ruyuan/mq/nameserver/registry/ServiceRegistry.java
git commit -m "feat: add registerTopicRoute method to ServiceRegistry"
```

---

### Task 3: NameServerRequestHandler — 修复三个 stub

**Files:**
- Modify: `ruyuan-mq-nameserver/src/main/java/com/ruyuan/mq/nameserver/NameServerRequestHandler.java`

**Interfaces:**
- Consumes: `ServiceRegistry.registerTopicRoute()` (Task 2), `MessageType.DELETE_TOPIC_REQUEST/RESPONSE`, `MessageType.LIST_TOPICS_REQUEST/RESPONSE` (Task 1)
- Produces: 三个方法正确的实现

- [ ] **Step 1: 修复 `updateServiceRegistryRoute()`（当前空壳，Line 278）**

替换现有空方法体：

```java
    private void updateServiceRegistryRoute(RegisterTopicRouteRequest routeRequest) {
        try {
            serviceRegistry.registerTopicRoute(
                routeRequest.brokerName,
                routeRequest.topic,
                routeRequest.readQueueNums,
                routeRequest.writeQueueNums,
                routeRequest.perm
            );
            logger.info("Synced topic route to ServiceRegistry: topic={}, broker={}",
                       routeRequest.topic, routeRequest.brokerName);
        } catch (Exception e) {
            logger.warn("Failed to update service registry route for topic: " + routeRequest.topic, e);
        }
    }
```

- [ ] **Step 2: 修复 `handleQueryTopic()`（当前空壳，Lines 120-139）**

替换方法体，改为真实查询 ServiceRegistry：

```java
    private ProtocolMessage handleQueryTopic(ProtocolMessage request) {
        logger.info("Handling query topic request: {}", request.getRequestId());

        try {
            byte[] body = request.getBody();
            String topic = null;
            if (body != null && body.length > 0) {
                String json = new String(body, StandardCharsets.UTF_8);
                QueryTopicRequest queryReq = JsonUtils.fromJson(json, QueryTopicRequest.class);
                if (queryReq != null) {
                    topic = queryReq.topic;
                }
            }

            boolean exists = topic != null && !topic.trim().isEmpty()
                    && serviceRegistry.getTopicRouteData(topic) != null;

            String payload = "{\"exists\":" + exists + "}";
            return ProtocolMessage.createSuccessResponse(
                    MessageType.QUERY_TOPIC_RESPONSE,
                    request.getRequestId(),
                    payload.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            logger.error("Handle query topic error, requestId=" + request.getRequestId(), e);
            return ProtocolMessage.createErrorResponse(
                    MessageType.QUERY_TOPIC_RESPONSE,
                    request.getRequestId(),
                    ResponseCode.INTERNAL_ERROR);
        }
    }
```

- [ ] **Step 3: 修复 `handleCreateTopic()`（当前空壳，Lines 144-162）**

改为转发给 Broker 创建。需要向 Broker 发起 CREATE_TOPIC_REQUEST：

```java
    private ProtocolMessage handleCreateTopic(ProtocolMessage request) {
        logger.info("Handling create topic request: {}", request.getRequestId());

        try {
            byte[] body = request.getBody();
            if (body == null || body.length == 0) {
                return ProtocolMessage.createErrorResponse(
                        MessageType.CREATE_TOPIC_RESPONSE,
                        request.getRequestId(),
                        ResponseCode.BAD_REQUEST);
            }

            String json = new String(body, StandardCharsets.UTF_8);
            CreateTopicRequest createReq = JsonUtils.fromJson(json, CreateTopicRequest.class);
            if (createReq == null || createReq.topic == null || createReq.topic.trim().isEmpty()) {
                return ProtocolMessage.createErrorResponse(
                        MessageType.CREATE_TOPIC_RESPONSE,
                        request.getRequestId(),
                        ResponseCode.BAD_REQUEST);
            }

            // 通过 default-topic 查找可用 Broker
            TopicRouteData defaultRoute = serviceRegistry.getTopicRouteData("default-topic");
            if (defaultRoute == null || defaultRoute.getQueueDatas().isEmpty()) {
                logger.error("No available broker for creating topic: {}", createReq.topic);
                return ProtocolMessage.createErrorResponse(
                        MessageType.CREATE_TOPIC_RESPONSE,
                        request.getRequestId(),
                        ResponseCode.INTERNAL_ERROR);
            }

            // 从 default-topic 路由中随机选一台 Broker，确保新 topic 均匀分布
            java.util.List<QueueData> queueList = defaultRoute.getQueueDatas();
            int randomIndex = new java.util.Random().nextInt(queueList.size());
            String brokerName = queueList.get(randomIndex).getBrokerName();
            BrokerData brokerData = serviceRegistry.getBrokerData(brokerName);
            if (brokerData == null || !brokerData.getBrokerAddrs().containsKey(0L)) {
                return ProtocolMessage.createErrorResponse(
                        MessageType.CREATE_TOPIC_RESPONSE,
                        request.getRequestId(),
                        ResponseCode.INTERNAL_ERROR);
            }

            String brokerAddr = brokerData.getBrokerAddrs().get(0L);
            String[] parts = brokerAddr.split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 10911;

            // 向 Broker 发送创建 Topic 请求
            com.ruyuan.mq.protocol.client.NettyClient brokerClient =
                    new com.ruyuan.mq.protocol.client.NettyClient(host, port);
            brokerClient.connect();

            try {
                ProtocolMessage brokerRequest = new ProtocolMessage(
                    MessageType.CREATE_TOPIC_REQUEST,
                    json.getBytes(StandardCharsets.UTF_8));
                ProtocolMessage brokerResponse = brokerClient.sendSync(brokerRequest, 5000);

                if (brokerResponse != null && brokerResponse.getStatus() == ResponseCode.SUCCESS) {
                    int queueCount = createReq.queueCount > 0 ? createReq.queueCount : 4;
                    // 同步更新本地路由表
                    updateServiceRegistryRouteInner(brokerName, createReq.topic, queueCount, queueCount, 6);
                    routeInfoManager.updateTopicRouteInfo(createReq.topic, brokerName,
                            queueCount, queueCount, 6);
                }

                return brokerResponse != null ? brokerResponse :
                    ProtocolMessage.createErrorResponse(
                        MessageType.CREATE_TOPIC_RESPONSE, request.getRequestId(),
                        ResponseCode.INTERNAL_ERROR);
            } finally {
                brokerClient.disconnect();
            }

        } catch (Exception e) {
            logger.error("Handle create topic error, requestId=" + request.getRequestId(), e);
            return ProtocolMessage.createErrorResponse(
                    MessageType.CREATE_TOPIC_RESPONSE,
                    request.getRequestId(),
                    ResponseCode.INTERNAL_ERROR);
        }
    }
```

- [ ] **Step 4: 添加缺失的 DTO 类和辅助方法**

在 `NameServerRequestHandler.java` 的 DTO 区域，添加：

```java
    static class QueryTopicRequest {
        public String topic;
    }

    static class CreateTopicRequest {
        public String topic;
        public int queueCount;
    }

    private void updateServiceRegistryRouteInner(String brokerName, String topicName,
                                                  int readQueueNums, int writeQueueNums, int perm) {
        try {
            serviceRegistry.registerTopicRoute(brokerName, topicName,
                    readQueueNums, writeQueueNums, perm);
        } catch (Exception e) {
            logger.warn("Failed to register topic route: " + topicName, e);
        }
    }
```

- [ ] **Step 5: 验证编译通过**

```bash
mvn compile -pl ruyuan-mq-nameserver -am -q
```

Expected: BUILD SUCCESS

- [ ] **Step 6: Commit**

```bash
git add ruyuan-mq-nameserver/src/main/java/com/ruyuan/mq/nameserver/NameServerRequestHandler.java
git commit -m "fix: implement NameServer stub handlers for topic create/query/route-sync"
```

---

### Task 4: BrokerRequestHandler — 新增 deleteTopic 和 listTopics 处理

**Files:**
- Modify: `ruyuan-mq-broker/src/main/java/com/ruyuan/mq/broker/BrokerRequestHandler.java`

**Interfaces:**
- Consumes: `MessageType.DELETE_TOPIC_REQUEST/RESPONSE`, `MessageType.LIST_TOPICS_REQUEST/RESPONSE` (Task 1)
- Consumes: `queueManager.deleteQueuesForTopic()` (已有), `topicManager.deleteTopic()` (已有), `topicManager.getAllTopicConfigs()` (已有)

- [ ] **Step 1: 在 switch 分支中添加两个新 case**

在 `handleRequest()` 方法的 switch 中，`QUERY_TOPIC_REQUEST` case 之后添加：

```java
                case DELETE_TOPIC_REQUEST:
                    return handleDeleteTopic(request);
                case LIST_TOPICS_REQUEST:
                    return handleListTopics(request);
```

- [ ] **Step 2: 实现 `handleDeleteTopic()` 方法**

在 `handleQueryTopic()` 方法之后、`ensureTopicAndQueues()` 之前添加：

```java
    private ProtocolMessage handleDeleteTopic(ProtocolMessage request) {
        String json = request.getBody() != null ? new String(request.getBody(), StandardCharsets.UTF_8) : null;
        DeleteTopicRequest req = json != null ? JsonUtils.fromJson(json, DeleteTopicRequest.class) : null;
        String topic = req != null ? req.topic : null;

        if (topic == null || topic.trim().isEmpty()) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.DELETE_TOPIC_RESPONSE,
                    request.getRequestId(),
                    ResponseCode.BAD_REQUEST);
        }

        // 清理队列（幂等：队列不存在也返回成功）
        queueManager.deleteQueuesForTopic(topic);

        // 删除 topic 配置
        boolean deleted = topicManager.deleteTopic(topic);

        String payload = "{\"success\":" + deleted + "}";
        return ProtocolMessage.createSuccessResponse(
                MessageType.DELETE_TOPIC_RESPONSE,
                request.getRequestId(),
                payload.getBytes(StandardCharsets.UTF_8));
    }
```

- [ ] **Step 3: 实现 `handleListTopics()` 方法**

```java
    private ProtocolMessage handleListTopics(ProtocolMessage request) {
        Map<String, TopicConfig> allConfigs = topicManager.getAllTopicConfigs();

        List<Map<String, Object>> topicList = new ArrayList<>();
        for (Map.Entry<String, TopicConfig> entry : allConfigs.entrySet()) {
            Map<String, Object> item = new java.util.HashMap<>();
            item.put("name", entry.getKey());
            item.put("queueCount", entry.getValue().getQueueCount());
            item.put("permission", entry.getValue().getPermission());
            topicList.add(item);
        }

        String payload = JsonUtils.toJson(topicList);
        return ProtocolMessage.createSuccessResponse(
                MessageType.LIST_TOPICS_RESPONSE,
                request.getRequestId(),
                payload != null ? payload.getBytes(StandardCharsets.UTF_8) : null);
    }
```

- [ ] **Step 4: 添加 DTO 类**

在 BrokerRequestHandler 的 DTO 区域添加：

```java
    static class DeleteTopicRequest { public String topic; }
```

- [ ] **Step 5: 验证编译通过**

```bash
mvn compile -pl ruyuan-mq-broker -am -q
```

Expected: BUILD SUCCESS

- [ ] **Step 6: Commit**

```bash
git add ruyuan-mq-broker/src/main/java/com/ruyuan/mq/broker/BrokerRequestHandler.java
git commit -m "feat: add handleDeleteTopic and handleListTopics to broker"
```

---

### Task 5: ProducerImpl — 路由回退逻辑

**Files:**
- Modify: `ruyuan-mq-client/src/main/java/com/ruyuan/mq/client/producer/ProducerImpl.java`

**Interfaces:**
- Consumes: 无（内部方法改动）
- Produces: `getTopicRouteInfo()` 现在在目标 topic 路由为空时自动回退查 default-topic

- [ ] **Step 1: 修改 `getTopicRouteInfo()` 方法**

在获取到空路由数据后、返回 null 之前，插入回退逻辑。将方法中这一段：

```java
            if (routeResponse == null || routeResponse.topicRouteData == null) {
                logger.warn("Empty route data for topic: {}", topic);
                return null;
            }
```

替换为：

```java
            if (routeResponse == null || routeResponse.topicRouteData == null
                    || routeResponse.topicRouteData.queueDatas == null
                    || routeResponse.topicRouteData.queueDatas.isEmpty()) {

                // 回退：通过 default-topic 获取 Broker 路由
                if ("default-topic".equals(topic)) {
                    logger.warn("No route info for default-topic, cluster unavailable");
                    return null;
                }

                logger.info("No route for topic '{}', falling back to default-topic", topic);
                TopicRouteInfo fallbackRoute = getTopicRouteInfo("default-topic");
                if (fallbackRoute == null) {
                    logger.warn("No fallback route found for topic: {}", topic);
                    return null;
                }

                // 从 default-topic 的多台 Broker 中随机选一台，避免所有新 topic 集中在同一台 Broker
                java.util.List<TopicRouteInfo.QueueInfo> fallbackQueues = fallbackRoute.getQueueInfos();
                if (fallbackQueues == null || fallbackQueues.isEmpty()) {
                    logger.warn("No queues in fallback route for topic: {}", topic);
                    return null;
                }
                int randomIdx = new java.util.Random().nextInt(fallbackQueues.size());
                TopicRouteInfo.QueueInfo selectedQueue = fallbackQueues.get(randomIdx);

                // 构造仅包含选中 Broker 的路由信息
                TopicRouteInfo singleRoute = new TopicRouteInfo(topic);
                singleRoute.setQueueInfos(java.util.Collections.singletonList(selectedQueue));
                singleRoute.setBrokerInfos(java.util.Collections.singletonList(
                    fallbackRoute.getBrokerInfo(selectedQueue.getBrokerName())));
                return singleRoute;
            }
```

- [ ] **Step 2: 验证编译通过**

```bash
mvn compile -pl ruyuan-mq-client -am -q
```

Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add ruyuan-mq-client/src/main/java/com/ruyuan/mq/client/producer/ProducerImpl.java
git commit -m "feat: add default-topic route fallback in ProducerImpl"
```

---

### Task 6: Console — TopicApiHandler REST API

**Files:**
- Create: `ruyuan-mq-console/src/main/java/com/ruyuan/mq/console/api/TopicApiHandler.java`
- Modify: `ruyuan-mq-console/src/main/java/com/ruyuan/mq/console/ConsoleApplication.java`

**Interfaces:**
- Consumes: `MessageType.CREATE_TOPIC_REQUEST/RESPONSE`, `MessageType.DELETE_TOPIC_REQUEST/RESPONSE`, `MessageType.LIST_TOPICS_REQUEST/RESPONSE` (Task 1)
- Consumes: NettyClient (已有), JsonUtils (已有)
- Produces: HTTP REST API on port 8080

- [ ] **Step 1: 创建 `TopicApiHandler.java`**

```java
package com.ruyuan.mq.console.api;

import com.ruyuan.mq.common.util.JsonUtils;
import com.ruyuan.mq.protocol.MessageType;
import com.ruyuan.mq.protocol.ProtocolMessage;
import com.ruyuan.mq.protocol.ResponseCode;
import com.ruyuan.mq.protocol.client.NettyClient;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Topic 管理 REST API Handler
 */
public class TopicApiHandler implements HttpHandler {

    private static final Logger logger = LoggerFactory.getLogger(TopicApiHandler.class);

    private final String nameServerHost;
    private final int nameServerPort;

    public TopicApiHandler(String nameServerHost, int nameServerPort) {
        this.nameServerHost = nameServerHost;
        this.nameServerPort = nameServerPort;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        URI uri = exchange.getRequestURI();
        String path = uri.getPath();

        try {
            if ("POST".equals(method) && "/api/topics".equals(path)) {
                handleCreateTopic(exchange);
            } else if ("GET".equals(method) && "/api/topics".equals(path)) {
                String query = uri.getQuery();
                if (query != null && query.startsWith("topic=")) {
                    handleQueryTopic(exchange, query.substring(6));
                } else {
                    handleListTopics(exchange);
                }
            } else if ("DELETE".equals(method) && path.startsWith("/api/topics/")) {
                String topicName = path.substring("/api/topics/".length());
                handleDeleteTopic(exchange, topicName);
            } else {
                sendResponse(exchange, 404, errorJson("Not found"));
            }
        } catch (Exception e) {
            logger.error("Error handling request: " + path, e);
            sendResponse(exchange, 500, errorJson("Internal server error"));
        }
    }

    private void handleCreateTopic(HttpExchange exchange) throws IOException {
        String body = readBody(exchange);
        Map<String, Object> req = JsonUtils.fromJson(body, Map.class);
        if (req == null) {
            sendResponse(exchange, 400, errorJson("Invalid JSON body"));
            return;
        }

        String topicName = (String) req.get("name");
        if (topicName == null || topicName.trim().isEmpty()) {
            sendResponse(exchange, 400, errorJson("Topic name is required"));
            return;
        }

        int queueCount = req.get("queues") instanceof Number
                ? ((Number) req.get("queues")).intValue() : 4;
        if (queueCount <= 0) {
            sendResponse(exchange, 400, errorJson("queueCount must be > 0"));
            return;
        }

        // 通过 NameServer 获取 Broker 并发送创建请求
        ProtocolMessage response = sendToNameServerThenBroker(
                MessageType.CREATE_TOPIC_REQUEST,
                createTopicJson(topicName, queueCount));

        if (response != null && response.getStatus() == ResponseCode.SUCCESS) {
            String respJson = "{\"success\":true,\"topic\":\"" + topicName + "\",\"queues\":" + queueCount + "}";
            sendResponse(exchange, 200, respJson);
        } else {
            sendResponse(exchange, 503, errorJson("Failed to create topic"));
        }
    }

    private void handleQueryTopic(HttpExchange exchange, String topicName) throws IOException {
        if (topicName == null || topicName.trim().isEmpty()) {
            sendResponse(exchange, 400, errorJson("Topic name is required"));
            return;
        }

        // 直接查 NameServer 是否存在路由
        NettyClient nsClient = null;
        try {
            nsClient = new NettyClient(nameServerHost, nameServerPort);
            nsClient.connect();

            String reqJson = "{\"topic\":\"" + topicName + "\"}";
            ProtocolMessage request = new ProtocolMessage(
                    MessageType.QUERY_TOPIC_REQUEST,
                    reqJson.getBytes(StandardCharsets.UTF_8));
            ProtocolMessage response = nsClient.sendSync(request, 5000);

            if (response != null && response.getStatus() == ResponseCode.SUCCESS) {
                String respBody = new String(response.getBody(), StandardCharsets.UTF_8);
                sendResponse(exchange, 200, respBody);
            } else {
                sendResponse(exchange, 200, "{\"exists\":false}");
            }
        } catch (Exception e) {
            logger.error("Query topic failed", e);
            sendResponse(exchange, 500, errorJson("Query failed: " + e.getMessage()));
        } finally {
            if (nsClient != null) {
                try { nsClient.disconnect(); } catch (Exception ignore) {}
            }
        }
    }

    private void handleListTopics(HttpExchange exchange) throws IOException {
        ProtocolMessage response = sendToNameServerThenBroker(
                MessageType.LIST_TOPICS_REQUEST, "{}");

        if (response != null && response.getStatus() == ResponseCode.SUCCESS) {
            String respBody = new String(response.getBody(), StandardCharsets.UTF_8);
            sendResponse(exchange, 200, respBody);
        } else {
            sendResponse(exchange, 503, errorJson("Failed to list topics"));
        }
    }

    private void handleDeleteTopic(HttpExchange exchange, String topicName) throws IOException {
        if (topicName == null || topicName.trim().isEmpty()) {
            sendResponse(exchange, 400, errorJson("Topic name is required"));
            return;
        }

        String reqJson = "{\"topic\":\"" + topicName + "\"}";
        ProtocolMessage response = sendToNameServerThenBroker(
                MessageType.DELETE_TOPIC_REQUEST, reqJson);

        if (response != null && response.getStatus() == ResponseCode.SUCCESS) {
            sendResponse(exchange, 200, "{\"success\":true,\"topic\":\"" + topicName + "\"}");
        } else {
            sendResponse(exchange, 503, errorJson("Failed to delete topic"));
        }
    }

    /**
     * 通过 NameServer 获取 Broker 地址，然后向 Broker 发送请求
     */
    private ProtocolMessage sendToNameServerThenBroker(MessageType brokerRequestType, String bodyJson) {
        NettyClient nsClient = null;
        NettyClient brokerClient = null;
        try {
            // 1. 连接 NameServer 查 default-topic 路由
            nsClient = new NettyClient(nameServerHost, nameServerPort);
            nsClient.connect();

            String routeReqJson = "{\"topic\":\"default-topic\"}";
            ProtocolMessage routeRequest = new ProtocolMessage(
                    MessageType.GET_ROUTEINFO_BY_TOPIC_REQUEST,
                    routeReqJson.getBytes(StandardCharsets.UTF_8));

            ProtocolMessage routeResponse = nsClient.sendSync(routeRequest, 5000);
            if (routeResponse == null || routeResponse.getStatus() != ResponseCode.SUCCESS) {
                logger.error("Failed to get default-topic route from NameServer");
                return null;
            }

            // 2. 解析 Broker 地址
            String routeBody = new String(routeResponse.getBody(), StandardCharsets.UTF_8);
            Map<String, Object> routeMap = JsonUtils.fromJson(routeBody, Map.class);
            if (routeMap == null || !routeMap.containsKey("topicRouteData")) {
                logger.error("Invalid route response");
                return null;
            }

            Map<String, Object> routeData = (Map<String, Object>) routeMap.get("topicRouteData");
            java.util.List<Map<String, Object>> queueDatas =
                    (java.util.List<Map<String, Object>>) routeData.get("queueDatas");
            java.util.List<Map<String, Object>> brokerDatas =
                    (java.util.List<Map<String, Object>>) routeData.get("brokerDatas");

            if (queueDatas == null || queueDatas.isEmpty() || brokerDatas == null || brokerDatas.isEmpty()) {
                logger.error("No available broker in default-topic route");
                return null;
            }

            // 随机选一台 Broker，确保新 topic 均匀分布在各 Broker 上
            int randomIdx = new java.util.Random().nextInt(queueDatas.size());
            String brokerName = (String) queueDatas.get(randomIdx).get("brokerName");
            String brokerAddr = null;
            for (Map<String, Object> bd : brokerDatas) {
                if (brokerName.equals(bd.get("brokerName"))) {
                    Map<String, Object> addrs = (Map<String, Object>) bd.get("brokerAddrs");
                    if (addrs != null) {
                        // brokerId=0 是 Master
                        Object addr = addrs.get("0");
                        if (addr != null) brokerAddr = addr.toString();
                    }
                    break;
                }
            }

            if (brokerAddr == null) {
                logger.error("No master address for broker: {}", brokerName);
                return null;
            }

            // 3. 连接 Broker 并发送请求
            String[] parts = brokerAddr.split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 10911;

            brokerClient = new NettyClient(host, port);
            brokerClient.connect();

            ProtocolMessage brokerRequest = new ProtocolMessage(
                    brokerRequestType, bodyJson.getBytes(StandardCharsets.UTF_8));
            return brokerClient.sendSync(brokerRequest, 5000);

        } catch (Exception e) {
            logger.error("Error in sendToNameServerThenBroker", e);
            return null;
        } finally {
            if (nsClient != null) {
                try { nsClient.disconnect(); } catch (Exception ignore) {}
            }
            if (brokerClient != null) {
                try { brokerClient.disconnect(); } catch (Exception ignore) {}
            }
        }
    }

    private String createTopicJson(String topicName, int queueCount) {
        return "{\"topic\":\"" + topicName + "\",\"queueCount\":" + queueCount + "}";
    }

    private String readBody(HttpExchange exchange) throws IOException {
        InputStream is = exchange.getRequestBody();
        byte[] bytes = new byte[4096];
        int len = is.read(bytes);
        return len > 0 ? new String(bytes, 0, len, StandardCharsets.UTF_8) : "";
    }

    private String errorJson(String message) {
        return "{\"success\":false,\"error\":\"" + message + "\"}";
    }

    private void sendResponse(HttpExchange exchange, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
```

- [ ] **Step 2: 修改 `ConsoleApplication.java`**

在 `ConsoleApplication.java` 中添加 HttpServer 启动逻辑。修改 `start()` 方法，在 `monitorService.start()` 之后添加：

```java
            // 启动 Topic 管理 HTTP API
            startTopicApi();
```

然后添加 `startTopicApi()` 私有方法和一个字段：

在类字段区域添加：

```java
    private com.sun.net.httpserver.HttpServer httpServer;
```

在 `startScheduledTasks()` 方法之前添加：

```java
    /**
     * 启动Topic管理HTTP API
     */
    private void startTopicApi() {
        try {
            com.ruyuan.mq.console.api.TopicApiHandler topicHandler =
                    new com.ruyuan.mq.console.api.TopicApiHandler("localhost", 9876);

            httpServer = com.sun.net.httpserver.HttpServer.create(
                    new java.net.InetSocketAddress(8080), 0);
            httpServer.createContext("/api/topics", topicHandler);
            httpServer.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(4));
            httpServer.start();

            logger.info("Topic API HTTP server started on port 8080");
        } catch (Exception e) {
            logger.error("Failed to start topic API HTTP server", e);
        }
    }
```

在 `shutdown()` 方法中添加关闭逻辑，在 `scheduledExecutor.shutdown()` 之前：

```java
            // 关闭 HTTP 服务
            if (httpServer != null) {
                httpServer.stop(3);
            }
```

- [ ] **Step 3: 验证编译通过**

```bash
mvn compile -pl ruyuan-mq-console -am -q
```

Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add ruyuan-mq-console/src/main/java/com/ruyuan/mq/console/api/TopicApiHandler.java
git add ruyuan-mq-console/src/main/java/com/ruyuan/mq/console/ConsoleApplication.java
git commit -m "feat: add topic management REST API to console"
```

---

### Task 7: 集成测试验证

**Files:**
- Create: `ruyuan-mq-test/src/test/java/com/ruyuan/mq/test/topic/TopicCreationTest.java`

**Interfaces:**
- Consumes: 所有之前 Task 的实现

- [ ] **Step 1: 编写集成测试**

```java
package com.ruyuan.mq.test.topic;

import com.ruyuan.mq.broker.BrokerRequestHandler;
import com.ruyuan.mq.broker.queue.QueueManager;
import com.ruyuan.mq.broker.topic.TopicManager;
import com.ruyuan.mq.client.producer.ProducerImpl;
import com.ruyuan.mq.client.producer.ProducerConfig;
import com.ruyuan.mq.protocol.MessageType;
import com.ruyuan.mq.protocol.ProtocolMessage;
import com.ruyuan.mq.protocol.ResponseCode;
import com.ruyuan.mq.store.DefaultMessageStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Topic 创建功能集成测试
 */
public class TopicCreationTest {

    private TopicManager topicManager;
    private QueueManager queueManager;
    private DefaultMessageStore messageStore;
    private BrokerRequestHandler handler;

    @BeforeEach
    public void setUp() {
        topicManager = new TopicManager();
        queueManager = new QueueManager();
        messageStore = new DefaultMessageStore(null);
        messageStore.start();
        handler = new BrokerRequestHandler(topicManager, queueManager, messageStore);
    }

    // ========== Topic 自动创建（Broker 端 ensureTopicAndQueues）==========

    @Test
    public void testAutoCreateTopicOnSend() {
        // 发送消息到一个不存在的 topic，应该自动创建
        String sendJson = "{\"topic\":\"test-autocreate\",\"tags\":\"test\",\"key\":\"k1\",\"body\":\"hello\"}";
        ProtocolMessage request = new ProtocolMessage(
                MessageType.SEND_MESSAGE_REQUEST,
                sendJson.getBytes(StandardCharsets.UTF_8));

        ProtocolMessage response = handler.handleRequest(null, request);

        assertEquals(ResponseCode.SUCCESS, response.getStatus(),
                "Send should succeed with auto-created topic");
        assertTrue(topicManager.topicExists("test-autocreate"),
                "Topic should be auto-created after first message");
    }

    @Test
    public void testAutoCreateTopicIsIdempotent() {
        String sendJson = "{\"topic\":\"test-idempotent\",\"tags\":\"t\",\"key\":\"k\",\"body\":\"msg\"}";
        ProtocolMessage request = new ProtocolMessage(
                MessageType.SEND_MESSAGE_REQUEST,
                sendJson.getBytes(StandardCharsets.UTF_8));

        // 发送两次，第二次应该也成功
        ProtocolMessage r1 = handler.handleRequest(null, request);
        ProtocolMessage r2 = handler.handleRequest(null, request);

        assertEquals(ResponseCode.SUCCESS, r1.getStatus());
        assertEquals(ResponseCode.SUCCESS, r2.getStatus());
    }

    // ========== 手动创建 Topic（通过协议）==========

    @Test
    public void testManualCreateTopic() {
        String createJson = "{\"topic\":\"manual-topic\"}";
        ProtocolMessage request = new ProtocolMessage(
                MessageType.CREATE_TOPIC_REQUEST,
                createJson.getBytes(StandardCharsets.UTF_8));

        ProtocolMessage response = handler.handleRequest(null, request);

        assertEquals(ResponseCode.SUCCESS, response.getStatus());
        assertTrue(topicManager.topicExists("manual-topic"));
    }

    @Test
    public void testCreateTopicWithEmptyName() {
        ProtocolMessage request = new ProtocolMessage(
                MessageType.CREATE_TOPIC_REQUEST,
                "{\"topic\":\"\"}".getBytes(StandardCharsets.UTF_8));

        ProtocolMessage response = handler.handleRequest(null, request);

        assertEquals(ResponseCode.BAD_REQUEST, response.getStatus());
    }

    // ========== 查询 Topic ==========

    @Test
    public void testQueryTopicExists() {
        topicManager.createTopic("query-test");

        String queryJson = "{\"topic\":\"query-test\"}";
        ProtocolMessage request = new ProtocolMessage(
                MessageType.QUERY_TOPIC_REQUEST,
                queryJson.getBytes(StandardCharsets.UTF_8));

        ProtocolMessage response = handler.handleRequest(null, request);

        assertEquals(ResponseCode.SUCCESS, response.getStatus());
        String body = new String(response.getBody(), StandardCharsets.UTF_8);
        assertTrue(body.contains("\"exists\":true"));
    }

    @Test
    public void testQueryTopicNotExists() {
        String queryJson = "{\"topic\":\"nonexistent\"}";
        ProtocolMessage request = new ProtocolMessage(
                MessageType.QUERY_TOPIC_REQUEST,
                queryJson.getBytes(StandardCharsets.UTF_8));

        ProtocolMessage response = handler.handleRequest(null, request);

        assertEquals(ResponseCode.SUCCESS, response.getStatus());
        String body = new String(response.getBody(), StandardCharsets.UTF_8);
        assertTrue(body.contains("\"exists\":false"));
    }

    // ========== 删除 Topic ==========

    @Test
    public void testDeleteTopic() {
        topicManager.createTopic("delete-me");

        String deleteJson = "{\"topic\":\"delete-me\"}";
        ProtocolMessage request = new ProtocolMessage(
                MessageType.DELETE_TOPIC_REQUEST,
                deleteJson.getBytes(StandardCharsets.UTF_8));

        ProtocolMessage response = handler.handleRequest(null, request);

        assertEquals(ResponseCode.SUCCESS, response.getStatus());
        assertFalse(topicManager.topicExists("delete-me"));
    }

    @Test
    public void testDeleteTopicIdempotent() {
        String deleteJson = "{\"topic\":\"no-such-topic\"}";
        ProtocolMessage request = new ProtocolMessage(
                MessageType.DELETE_TOPIC_REQUEST,
                deleteJson.getBytes(StandardCharsets.UTF_8));

        // 删除不存在的 topic 也不报错
        ProtocolMessage response = handler.handleRequest(null, request);
        assertEquals(ResponseCode.SUCCESS, response.getStatus());
    }

    // ========== 列出 Topic ==========

    @Test
    public void testListTopics() {
        topicManager.createTopic("list-test-1");
        topicManager.createTopic("list-test-2");

        ProtocolMessage request = new ProtocolMessage(
                MessageType.LIST_TOPICS_REQUEST,
                "{}".getBytes(StandardCharsets.UTF_8));

        ProtocolMessage response = handler.handleRequest(null, request);

        assertEquals(ResponseCode.SUCCESS, response.getStatus());
        String body = new String(response.getBody(), StandardCharsets.UTF_8);
        assertTrue(body.contains("list-test-1"));
        assertTrue(body.contains("list-test-2"));
    }
}
```

- [ ] **Step 2: 运行测试**

```bash
mvn test -pl ruyuan-mq-test -am -Dtest=TopicCreationTest
```

Expected: All tests PASS

- [ ] **Step 3: Commit**

```bash
git add ruyuan-mq-test/src/test/java/com/ruyuan/mq/test/topic/TopicCreationTest.java
git commit -m "test: add integration tests for topic creation"
```

---

### Task 8: 最终验证

- [ ] **Step 1: 运行全量测试**

```bash
mvn test -q
```

Expected: All tests PASS, BUILD SUCCESS

- [ ] **Step 2: 确认无编译警告和错误**

```bash
mvn compile -q
```

Expected: BUILD SUCCESS, no warnings
