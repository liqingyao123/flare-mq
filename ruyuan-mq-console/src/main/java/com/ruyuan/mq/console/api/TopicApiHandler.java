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
