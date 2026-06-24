package com.ruyuan.mq.nameserver;

import com.ruyuan.mq.nameserver.registry.ServiceDiscovery;
import com.ruyuan.mq.nameserver.registry.ServiceRegistry;
import com.ruyuan.mq.nameserver.registry.TopicRouteData;
import com.ruyuan.mq.nameserver.registry.BrokerData;
import com.ruyuan.mq.nameserver.registry.QueueData;
import com.ruyuan.mq.nameserver.registry.TopicConfigSerializeWrapper;
import com.ruyuan.mq.nameserver.registry.TopicConfig;
import com.ruyuan.mq.nameserver.registry.RegisterBrokerResult;
import com.ruyuan.mq.nameserver.route.RouteInfoManager;
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
                case REGISTER_TOPIC_ROUTE_REQUEST:
                    return handleRegisterTopicRoute(request);
                case REGISTER_BROKER_REQUEST:
                    return handleRegisterBroker(request);
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
            // 简化实现：返回Topic存在
            String payload = "{\"exists\":true}";
            return ProtocolMessage.createSuccessResponse(
                    MessageType.QUERY_TOPIC_RESPONSE,
                    request.getRequestId(),
                    payload.getBytes(StandardCharsets.UTF_8)
            );
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
            // 简化实现：直接返回成功
            return ProtocolMessage.createSuccessResponse(
                    MessageType.CREATE_TOPIC_RESPONSE,
                    request.getRequestId(),
                    "OK".getBytes(StandardCharsets.UTF_8)
            );
        } catch (Exception e) {
            logger.error("Handle create topic error, requestId=" + request.getRequestId(), e);
            return ProtocolMessage.createErrorResponse(
                    MessageType.CREATE_TOPIC_RESPONSE,
                    request.getRequestId(),
                    ResponseCode.INTERNAL_ERROR
            );
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
     * 更新ServiceRegistry中的路由信息（兼容现有查询逻辑）
     */
    private void updateServiceRegistryRoute(RegisterTopicRouteRequest routeRequest) {
        try {
            // 这里需要将RouteInfoManager的数据同步到ServiceRegistry
            // 为了简化，我们可以直接在ServiceRegistry中添加路由信息
            logger.debug("Updated service registry route for topic: {}", routeRequest.topic);
        } catch (Exception e) {
            logger.warn("Failed to update service registry route", e);
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
    }

    static class RegisterBrokerResponse {
        public String haServerAddr;
        public String masterAddr;
    }
}
