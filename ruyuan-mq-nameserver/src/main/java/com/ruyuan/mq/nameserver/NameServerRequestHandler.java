package com.ruyuan.mq.nameserver;

import com.ruyuan.mq.nameserver.registry.ServiceDiscovery;
import com.ruyuan.mq.nameserver.registry.ServiceRegistry;
import com.ruyuan.mq.nameserver.registry.ServiceRegistry.ConsumerHeartbeatData;
import com.ruyuan.mq.nameserver.registry.TopicRouteData;
import com.ruyuan.mq.nameserver.registry.BrokerData;
import com.ruyuan.mq.nameserver.registry.QueueData;
import com.ruyuan.mq.nameserver.registry.TopicConfigSerializeWrapper;
import com.ruyuan.mq.nameserver.registry.TopicConfig;
import com.ruyuan.mq.nameserver.registry.RegisterBrokerResult;
import com.ruyuan.mq.nameserver.route.RouteInfoManager;
import com.ruyuan.mq.nameserver.route.RouteInfoManager.TopicRouteInfo;
import com.ruyuan.mq.protocol.ProtocolMessage;
import com.ruyuan.mq.protocol.MessageType;
import com.ruyuan.mq.protocol.ResponseCode;
import com.ruyuan.mq.protocol.server.ServerRequestHandler;
import com.ruyuan.mq.common.util.JsonUtils;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.LinkedHashMap;

/**
 * NameServer请求处理器
 *
 * @author RuYuan MQ Team
 */
public class NameServerRequestHandler implements ServerRequestHandler {

    private static final Logger logger = LoggerFactory.getLogger(NameServerRequestHandler.class);

    private final ServiceDiscovery serviceDiscovery;
    private final ServiceRegistry serviceRegistry;
    private final RouteInfoManager routeInfoManager;

    public NameServerRequestHandler(ServiceDiscovery serviceDiscovery, ServiceRegistry serviceRegistry, RouteInfoManager routeInfoManager) {
        this.serviceDiscovery = serviceDiscovery;
        this.serviceRegistry = serviceRegistry;
        this.routeInfoManager = routeInfoManager;
    }

    @Override
    public ProtocolMessage handleRequest(ChannelHandlerContext ctx, ProtocolMessage request) {
        try {
            if (request.getType() == MessageType.HEARTBEAT_REQUEST) {
                return ProtocolMessage.createHeartbeatResponse(request.getRequestId());
            }

            switch (request.getType()) {
                case GET_ROUTEINFO_BY_TOPIC_REQUEST:
                    return handleGetRouteInfoByTopic(request);
                case QUERY_TOPIC_REQUEST:
                    return handleQueryTopic(request);
                case CREATE_TOPIC_REQUEST:
                    return handleCreateTopic(request);
                case DELETE_TOPIC_REQUEST:
                    return handleDeleteTopic(request);
                case REGISTER_TOPIC_ROUTE_REQUEST:
                    return handleRegisterTopicRoute(request);
                case REGISTER_BROKER_REQUEST:
                    return handleRegisterBroker(request);
                case CONSUMER_REGISTER_REQUEST:
                    return handleConsumerRegister(request);
                case CONSUMER_HEARTBEAT_REQUEST:
                    return handleConsumerHeartbeat(request);
                case GET_CLUSTER_STATS_REQUEST:
                    return handleGetClusterStats(request);
                case REPORT_CONSUMER_GROUP_STATS_REQUEST:
                    return handleReportConsumerGroupStats(request);
                case GET_CONSUMER_GROUPS_REQUEST:
                    return handleGetConsumerGroups(request);
                default:
                    logger.warn("Unknown request type: {}", request.getType());
                    return ProtocolMessage.createErrorResponse(
                            MessageType.RESPONSE, request.getRequestId(), ResponseCode.BAD_REQUEST);
            }

        } catch (Exception e) {
            logger.error("Handle request failed: " + request, e);
            return ProtocolMessage.createErrorResponse(
                    MessageType.RESPONSE, request.getRequestId(), ResponseCode.INTERNAL_ERROR);
        }
    }

    /**
     * 处理获取Topic路由信息请求
     */
    private ProtocolMessage handleGetRouteInfoByTopic(ProtocolMessage request) {
        byte[] body = request.getBody();
        if (body == null || body.length == 0) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.GET_ROUTEINFO_BY_TOPIC_RESPONSE, 
                    request.getRequestId(), 
                    ResponseCode.BAD_REQUEST);
        }

        String json = new String(body, StandardCharsets.UTF_8);
        GetRouteInfoRequest routeRequest = JsonUtils.fromJson(json, GetRouteInfoRequest.class);
        if (routeRequest == null || routeRequest.topic == null || routeRequest.topic.trim().isEmpty()) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.GET_ROUTEINFO_BY_TOPIC_RESPONSE, 
                    request.getRequestId(), 
                    ResponseCode.BAD_REQUEST);
        }

        logger.info("Handling get route info request for topic: {}", routeRequest.topic);

        // 从服务发现获取路由信息
        TopicRouteData routeData = serviceDiscovery.getTopicRouteData(routeRequest.topic);

        if (routeData == null) {
            // 如果没有找到路由信息，返回空的路由数据
            routeData = new TopicRouteData();
        }

        // 转换为响应格式
        GetRouteInfoResponse response = new GetRouteInfoResponse();
        response.topicRouteData = convertToRouteResponse(routeData);

        String responseJson = JsonUtils.toJson(response);
        return ProtocolMessage.createSuccessResponse(
                MessageType.GET_ROUTEINFO_BY_TOPIC_RESPONSE,
                request.getRequestId(),
                responseJson != null ? responseJson.getBytes(StandardCharsets.UTF_8) : null);
    }

    /**
     * 处理查询Topic请求
     */
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
                    ResponseCode.INTERNAL_ERROR
            );
        }
    }

    /**
     * 处理创建Topic请求
     */
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

    /**
     * 处理删除Topic请求
     */
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
            if (routeInfo == null || routeInfo.getBrokerRoutes().isEmpty()) {
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
            com.ruyuan.mq.protocol.client.NettyClient brokerClient =
                    new com.ruyuan.mq.protocol.client.NettyClient(host, port);
            brokerClient.connect();

            try {
                ProtocolMessage brokerRequest = new ProtocolMessage(
                    MessageType.DELETE_TOPIC_REQUEST,
                    json.getBytes(StandardCharsets.UTF_8));
                ProtocolMessage brokerResponse = brokerClient.sendSync(brokerRequest, 5000);

                if (brokerResponse != null && brokerResponse.getStatus() == ResponseCode.SUCCESS) {
                    routeInfoManager.removeTopicRouteInfo(deleteReq.topic, brokerName);
                    serviceRegistry.removeTopicRoute(deleteReq.topic, brokerName);
                }

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

    /**
     * 处理Broker注册Topic路由请求
     */
    private ProtocolMessage handleRegisterTopicRoute(ProtocolMessage request) {
        byte[] body = request.getBody();
        if (body == null || body.length == 0) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.REGISTER_TOPIC_ROUTE_RESPONSE,
                    request.getRequestId(),
                    ResponseCode.BAD_REQUEST);
        }

        String json = new String(body, StandardCharsets.UTF_8);
        RegisterTopicRouteRequest routeRequest = JsonUtils.fromJson(json, RegisterTopicRouteRequest.class);
        if (routeRequest == null) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.REGISTER_TOPIC_ROUTE_RESPONSE,
                    request.getRequestId(),
                    ResponseCode.BAD_REQUEST);
        }

        logger.info("Handling register topic route request: topic={}, broker={}, readQueues={}, writeQueues={}",
                   routeRequest.topic, routeRequest.brokerName, routeRequest.readQueueNums, routeRequest.writeQueueNums);

        try {
            // 如果队列数为0，表示取消注册
            if (routeRequest.readQueueNums == 0 && routeRequest.writeQueueNums == 0) {
                routeInfoManager.removeTopicRouteInfo(routeRequest.topic, routeRequest.brokerName);
                serviceRegistry.removeTopicRoute(routeRequest.topic, routeRequest.brokerName);

                String responseJson = "{\"result\":\"success\"}";
                return ProtocolMessage.createSuccessResponse(
                        MessageType.REGISTER_TOPIC_ROUTE_RESPONSE,
                        request.getRequestId(),
                        responseJson.getBytes(StandardCharsets.UTF_8));
            }

            // 更新路由信息到RouteInfoManager
            routeInfoManager.updateTopicRouteInfo(
                routeRequest.topic,
                routeRequest.brokerName,
                routeRequest.readQueueNums,
                routeRequest.writeQueueNums,
                routeRequest.perm
            );

            // 同时更新到ServiceRegistry（为了兼容现有的查询逻辑）
            updateServiceRegistryRoute(routeRequest);

            String responseJson = "{\"result\":\"success\"}";
            return ProtocolMessage.createSuccessResponse(
                    MessageType.REGISTER_TOPIC_ROUTE_RESPONSE,
                    request.getRequestId(),
                    responseJson.getBytes(StandardCharsets.UTF_8));

        } catch (Exception e) {
            logger.error("Handle register topic route error, requestId=" + request.getRequestId(), e);
            return ProtocolMessage.createErrorResponse(
                    MessageType.REGISTER_TOPIC_ROUTE_RESPONSE,
                    request.getRequestId(),
                    ResponseCode.INTERNAL_ERROR
            );
        }
    }

    /**
     * 处理Broker注册请求
     */
    private ProtocolMessage handleRegisterBroker(ProtocolMessage request) {
        byte[] body = request.getBody();
        if (body == null || body.length == 0) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.REGISTER_BROKER_RESPONSE,
                    request.getRequestId(),
                    ResponseCode.BAD_REQUEST);
        }

        String json = new String(body, StandardCharsets.UTF_8);
        RegisterBrokerRequest brokerRequest = JsonUtils.fromJson(json, RegisterBrokerRequest.class);
        if (brokerRequest == null) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.REGISTER_BROKER_RESPONSE,
                    request.getRequestId(),
                    ResponseCode.BAD_REQUEST);
        }

        logger.info("Handling register broker request: cluster={}, brokerName={}, brokerAddr={}, brokerId={}",
                   brokerRequest.clusterName, brokerRequest.brokerName, brokerRequest.brokerAddr, brokerRequest.brokerId);

        try {
            // 调用ServiceRegistry注册Broker
            RegisterBrokerResult result = serviceRegistry.registerBroker(
                brokerRequest.clusterName,
                brokerRequest.brokerAddr,
                brokerRequest.brokerName,
                brokerRequest.brokerId,
                brokerRequest.haServerAddr,
                brokerRequest.topicConfigWrapper,
                brokerRequest.filterServerList,
                brokerRequest.compressed
            );

            // 存储Broker上报的监控指标
            BrokerData brokerData = serviceRegistry.getBrokerData(brokerRequest.brokerName);
            if (brokerData != null) {
                brokerData.setCpuUsage(brokerRequest.cpuUsage);
                brokerData.setMemoryUsage(brokerRequest.memoryUsage);
                brokerData.setDiskUsage(brokerRequest.diskUsage);
                brokerData.setTotalMessages(brokerRequest.totalMessages);
                brokerData.setCurrentTps(brokerRequest.currentTps);
            }

            // 构建响应
            RegisterBrokerResponse response = new RegisterBrokerResponse();
            response.haServerAddr = result.getHaServerAddr();
            response.masterAddr = result.getMasterAddr();

            String responseJson = JsonUtils.toJson(response);
            return ProtocolMessage.createSuccessResponse(
                    MessageType.REGISTER_BROKER_RESPONSE,
                    request.getRequestId(),
                    responseJson.getBytes(StandardCharsets.UTF_8));

        } catch (Exception e) {
            logger.error("Handle register broker error, requestId=" + request.getRequestId(), e);
            return ProtocolMessage.createErrorResponse(
                    MessageType.REGISTER_BROKER_RESPONSE,
                    request.getRequestId(),
                    ResponseCode.INTERNAL_ERROR
            );
        }
    }

    /**
     * 处理 Consumer 注册请求
     */
    private ProtocolMessage handleConsumerRegister(ProtocolMessage request) {
        byte[] body = request.getBody();
        if (body == null || body.length == 0) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.CONSUMER_REGISTER_RESPONSE,
                    request.getRequestId(), ResponseCode.BAD_REQUEST);
        }

        String json = new String(body, StandardCharsets.UTF_8);
        ConsumerRegisterRequest req = JsonUtils.fromJson(json, ConsumerRegisterRequest.class);
        if (req == null || req.consumerGroup == null || req.consumerId == null) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.CONSUMER_REGISTER_RESPONSE,
                    request.getRequestId(), ResponseCode.BAD_REQUEST);
        }

        logger.info("Consumer registering: group={}, consumerId={}", req.consumerGroup, req.consumerId);

        java.util.List<String> topics = req.topics != null ? req.topics : java.util.Collections.emptyList();
        java.util.List<String> consumerIds = serviceRegistry.registerConsumer(
                req.consumerGroup, req.consumerId, topics);

        ConsumerRegisterResponse resp = new ConsumerRegisterResponse();
        resp.consumerIdList = consumerIds;

        String respJson = JsonUtils.toJson(resp);
        return ProtocolMessage.createSuccessResponse(
                MessageType.CONSUMER_REGISTER_RESPONSE,
                request.getRequestId(),
                respJson != null ? respJson.getBytes(StandardCharsets.UTF_8) : null);
    }

    /**
     * 处理 Consumer 心跳请求
     */
    private ProtocolMessage handleConsumerHeartbeat(ProtocolMessage request) {
        byte[] body = request.getBody();
        if (body == null || body.length == 0) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.CONSUMER_HEARTBEAT_RESPONSE,
                    request.getRequestId(), ResponseCode.BAD_REQUEST);
        }

        String json = new String(body, StandardCharsets.UTF_8);
        ConsumerHeartbeatRequest req = JsonUtils.fromJson(json, ConsumerHeartbeatRequest.class);
        if (req == null || req.consumerGroup == null || req.consumerId == null) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.CONSUMER_HEARTBEAT_RESPONSE,
                    request.getRequestId(), ResponseCode.BAD_REQUEST);
        }

        serviceRegistry.heartbeatConsumer(req.consumerGroup, req.consumerId);
        return ProtocolMessage.createSuccessResponse(
                MessageType.CONSUMER_HEARTBEAT_RESPONSE,
                request.getRequestId(),
                "OK".getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 处理获取集群统计信息请求
     */
    private ProtocolMessage handleGetClusterStats(ProtocolMessage request) {
        try {
            // 收集所有Broker数据
            Map<String, BrokerData> allBrokers = serviceRegistry.getAllBrokerData();
            List<Map<String, Object>> brokerList = new ArrayList<>();

            for (BrokerData bd : allBrokers.values()) {
                Map<String, Object> b = new LinkedHashMap<>();
                b.put("brokerName", bd.getBrokerName());
                b.put("clusterName", bd.getCluster());
                String addr = bd.getBrokerAddrs().get(0L);
                b.put("brokerAddr", addr != null ? addr : "");
                b.put("brokerId", 0L);
                b.put("role", bd.hasMaster() ? "Master" : "Slave");
                b.put("cpuUsage", bd.getCpuUsage());
                b.put("memoryUsage", bd.getMemoryUsage());
                b.put("diskUsage", bd.getDiskUsage());
                b.put("totalMessages", bd.getTotalMessages());
                b.put("currentTps", bd.getCurrentTps());
                b.put("lastUpdateTimestamp", bd.getLastUpdateTimestamp());
                brokerList.add(b);
            }

            // 收集路由统计信息
            RouteInfoManager.RouteStatistics stats = routeInfoManager.getStatistics();
            int consumerGroupCount = serviceRegistry.getAllConsumerGroups().size();

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("brokers", brokerList);
            result.put("topicCount", stats.getTopicCount());
            result.put("queueCount", stats.getQueueCount());
            result.put("consumerGroupCount", consumerGroupCount);

            String json = JsonUtils.toJson(result);
            return ProtocolMessage.createSuccessResponse(
                    MessageType.GET_CLUSTER_STATS_RESPONSE,
                    request.getRequestId(),
                    json.getBytes(StandardCharsets.UTF_8));

        } catch (Exception e) {
            logger.error("Error handling cluster stats request", e);
            return ProtocolMessage.createErrorResponse(
                    MessageType.GET_CLUSTER_STATS_RESPONSE,
                    request.getRequestId(), ResponseCode.INTERNAL_ERROR);
        }
    }

    /**
     * 处理 Broker 上报的消费组统计信息
     */
    private ProtocolMessage handleReportConsumerGroupStats(ProtocolMessage request) {
        byte[] body = request.getBody();
        if (body == null || body.length == 0) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.REPORT_CONSUMER_GROUP_STATS_RESPONSE,
                    request.getRequestId(), ResponseCode.BAD_REQUEST);
        }

        String json = new String(body, StandardCharsets.UTF_8);
        @SuppressWarnings("unchecked")
        Map<String, Object> data = JsonUtils.fromJson(json, Map.class);
        if (data == null) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.REPORT_CONSUMER_GROUP_STATS_RESPONSE,
                    request.getRequestId(), ResponseCode.BAD_REQUEST);
        }

        String groupName = (String) data.get("groupName");
        String topic = (String) data.get("topic");
        String brokerName = (String) data.get("brokerName");

        if (groupName == null || topic == null || brokerName == null) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.REPORT_CONSUMER_GROUP_STATS_RESPONSE,
                    request.getRequestId(), ResponseCode.BAD_REQUEST);
        }

        ServiceRegistry.ConsumerGroupStats stats = new ServiceRegistry.ConsumerGroupStats();
        stats.setGroupName(groupName);
        stats.setTopic(topic);
        stats.setConsumeTps(data.get("consumeTps") instanceof Number
                ? ((Number) data.get("consumeTps")).doubleValue() : 0.0);

        @SuppressWarnings("unchecked")
        java.util.List<Map<String, Object>> rawQueueStats =
                (java.util.List<Map<String, Object>>) data.get("queueStats");
        if (rawQueueStats != null) {
            java.util.List<ServiceRegistry.QueueStat> queueStats = new java.util.ArrayList<>();
            for (Map<String, Object> qs : rawQueueStats) {
                ServiceRegistry.QueueStat q = new ServiceRegistry.QueueStat();
                q.setQueueId(qs.get("queueId") instanceof Number
                        ? ((Number) qs.get("queueId")).intValue() : 0);
                q.setMaxOffset(qs.get("maxOffset") instanceof Number
                        ? ((Number) qs.get("maxOffset")).longValue() : 0L);
                q.setConsumedOffset(qs.get("consumedOffset") instanceof Number
                        ? ((Number) qs.get("consumedOffset")).longValue() : 0L);
                queueStats.add(q);
            }
            stats.setQueueStats(queueStats);
        }

        serviceRegistry.updateConsumerGroupStats(brokerName, stats);

        return ProtocolMessage.createSuccessResponse(
                MessageType.REPORT_CONSUMER_GROUP_STATS_RESPONSE,
                request.getRequestId(),
                "OK".getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 处理查询消费组列表请求（供 Console 使用）
     */
    private ProtocolMessage handleGetConsumerGroups(ProtocolMessage request) {
        try {
            java.util.List<ServiceRegistry.ConsumerGroupStats> allStats =
                    serviceRegistry.getAllConsumerGroupStats();

            java.util.List<Map<String, Object>> groupList = new java.util.ArrayList<>();
            java.util.Set<String> allGroupNames = new java.util.LinkedHashSet<>();
            allGroupNames.addAll(serviceRegistry.getAllConsumerGroups());

            java.util.Map<String, ServiceRegistry.ConsumerGroupStats> statsByGroup = new java.util.LinkedHashMap<>();
            for (ServiceRegistry.ConsumerGroupStats s : allStats) {
                statsByGroup.put(s.getGroupName(), s);
                allGroupNames.add(s.getGroupName());
            }

            for (String groupName : allGroupNames) {
                Map<String, Object> g = new LinkedHashMap<>();
                g.put("groupName", groupName);

                ServiceRegistry.ConsumerGroupStats brokerStats = statsByGroup.get(groupName);

                // 合并 Broker 上报的消费进度
                if (brokerStats != null) {
                    g.put("topic", brokerStats.getTopic());
                    g.put("consumeTps", brokerStats.getConsumeTps());
                    java.util.List<Map<String, Object>> qs = new java.util.ArrayList<>();
                    long totalConsumed = 0;
                    long totalLag = 0;
                    for (ServiceRegistry.QueueStat q : brokerStats.getQueueStats()) {
                        Map<String, Object> qm = new LinkedHashMap<>();
                        qm.put("queueId", q.getQueueId());
                        qm.put("maxOffset", q.getMaxOffset());
                        qm.put("consumedOffset", q.getConsumedOffset());
                        qm.put("lag", q.getMaxOffset() - q.getConsumedOffset());
                        qs.add(qm);
                        totalConsumed += q.getConsumedOffset();
                        totalLag += (q.getMaxOffset() - q.getConsumedOffset());
                    }
                    g.put("queueStats", qs);
                    g.put("totalConsumed", totalConsumed);
                    g.put("totalLag", totalLag);
                } else {
                    g.put("topic", "");
                    g.put("consumeTps", 0.0);
                    g.put("queueStats", java.util.Collections.emptyList());
                    g.put("totalConsumed", 0L);
                    g.put("totalLag", 0L);
                }

                // 合并消费者心跳数据
                Map<String, ConsumerHeartbeatData> consumers =
                        serviceRegistry.getConsumerHeartbeatData(groupName);
                int activeCount = 0;
                java.util.List<Map<String, Object>> consumerList = new java.util.ArrayList<>();
                for (ConsumerHeartbeatData hb : consumers.values()) {
                    Map<String, Object> cm = new LinkedHashMap<>();
                    cm.put("consumerId", hb.getConsumerId());
                    cm.put("lastHeartbeat", hb.getLastHeartbeatTime());
                    boolean alive = (System.currentTimeMillis() - hb.getLastHeartbeatTime()) < 30000L;
                    if (alive) activeCount++;
                    cm.put("alive", alive);
                    consumerList.add(cm);
                }
                g.put("consumerCount", consumers.size());
                g.put("activeConsumers", activeCount);
                g.put("consumers", consumerList);
                g.put("status", activeCount > 0 ? "ACTIVE" : "INACTIVE");

                groupList.add(g);
            }

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("consumerGroups", groupList);

            String json = JsonUtils.toJson(result);
            return ProtocolMessage.createSuccessResponse(
                    MessageType.GET_CONSUMER_GROUPS_RESPONSE,
                    request.getRequestId(),
                    json.getBytes(StandardCharsets.UTF_8));

        } catch (Exception e) {
            logger.error("Error handling get consumer groups request", e);
            return ProtocolMessage.createErrorResponse(
                    MessageType.GET_CONSUMER_GROUPS_RESPONSE,
                    request.getRequestId(), ResponseCode.INTERNAL_ERROR);
        }
    }

    /**
     * 更新ServiceRegistry中的路由信息（兼容现有查询逻辑）
     */
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

    private void updateServiceRegistryRouteInner(String brokerName, String topicName,
                                                  int readQueueNums, int writeQueueNums, int perm) {
        try {
            serviceRegistry.registerTopicRoute(brokerName, topicName,
                    readQueueNums, writeQueueNums, perm);
        } catch (Exception e) {
            logger.warn("Failed to register topic route: " + topicName, e);
        }
    }

    /**
     * 转换路由数据为响应格式
     */
    private TopicRouteResponse convertToRouteResponse(TopicRouteData routeData) {
        TopicRouteResponse response = new TopicRouteResponse();
        response.orderTopicConf = routeData.getOrderTopicConf();
        response.filterServerTable = routeData.getFilterServerTable();
        
        // 转换QueueData
        response.queueDatas = new ArrayList<>();
        for (QueueData queueData : routeData.getQueueDatas()) {
            QueueDataResponse qdr = new QueueDataResponse();
            qdr.brokerName = queueData.getBrokerName();
            qdr.readQueueNums = queueData.getReadQueueNums();
            qdr.writeQueueNums = queueData.getWriteQueueNums();
            qdr.perm = queueData.getPerm();
            qdr.topicSynFlag = queueData.getTopicSynFlag();
            response.queueDatas.add(qdr);
        }
        
        // 转换BrokerData
        response.brokerDatas = new ArrayList<>();
        for (BrokerData brokerData : routeData.getBrokerDatas()) {
            BrokerDataResponse bdr = new BrokerDataResponse();
            bdr.cluster = brokerData.getCluster();
            bdr.brokerName = brokerData.getBrokerName();
            bdr.brokerAddrs = brokerData.getBrokerAddrs();
            response.brokerDatas.add(bdr);
        }
        
        return response;
    }

    // ===== 请求/响应DTO =====
    static class GetRouteInfoRequest {
        public String topic;
    }

    static class GetRouteInfoResponse {
        public TopicRouteResponse topicRouteData;
    }

    static class TopicRouteResponse {
        public String orderTopicConf;
        public List<QueueDataResponse> queueDatas;
        public List<BrokerDataResponse> brokerDatas;
        public String filterServerTable;
    }

    static class QueueDataResponse {
        public String brokerName;
        public int readQueueNums;
        public int writeQueueNums;
        public int perm;
        public int topicSynFlag;
    }

    static class BrokerDataResponse {
        public String cluster;
        public String brokerName;
        public java.util.Map<Long, String> brokerAddrs;
    }

    static class RegisterTopicRouteRequest {
        public String topic;
        public String brokerName;
        public String brokerAddr;
        public int readQueueNums;
        public int writeQueueNums;
        public int perm;
    }

    static class RegisterBrokerRequest {
        public String clusterName;
        public String brokerAddr;
        public String brokerName;
        public long brokerId;
        public String haServerAddr;
        public TopicConfigSerializeWrapper topicConfigWrapper;
        public java.util.List<String> filterServerList;
        public boolean compressed;
        public double cpuUsage;
        public double memoryUsage;
        public double diskUsage;
        public long totalMessages;
        public double currentTps;
    }

    static class RegisterBrokerResponse {
        public String haServerAddr;
        public String masterAddr;
    }

    static class ConsumerRegisterRequest {
        public String consumerGroup;
        public String consumerId;
        public java.util.List<String> topics;
    }

    static class ConsumerRegisterResponse {
        public java.util.List<String> consumerIdList;
    }

    static class ConsumerHeartbeatRequest {
        public String consumerGroup;
        public String consumerId;
    }

    static class QueryTopicRequest {
        public String topic;
    }

    static class CreateTopicRequest {
        public String topic;
        public int queueCount;
    }

    static class DeleteTopicRequest {
        public String topic;
    }
}
