package com.ruyuan.mq.broker.topic;

import com.ruyuan.mq.protocol.client.NettyClient;
import com.ruyuan.mq.protocol.ProtocolMessage;
import com.ruyuan.mq.protocol.MessageType;
import com.ruyuan.mq.common.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.nio.charset.StandardCharsets;

/**
 * Topic管理器
 * 
 * 负责Topic的创建、删除、查询等管理功能
 * 
 * @author RuYuan MQ Team
 */
public class TopicManager {
    
    private static final Logger logger = LoggerFactory.getLogger(TopicManager.class);
    
    /**
     * Topic配置缓存
     * Key: topicName, Value: TopicConfig
     */
    private final ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();

    /**
     * NameServer客户端连接
     */
    private NettyClient nameServerClient;

    /**
     * Broker名称
     */
    private String brokerName;

    /**
     * Broker地址
     */
    private String brokerAddr;
    
    /**
     * 默认Topic配置
     */
    private static final int DEFAULT_QUEUE_COUNT = 4;
    private static final int DEFAULT_PERMISSION = TopicPermission.READ_WRITE;

    /**
     * 初始化TopicManager
     */
    public void initialize(String nameServerAddr, String brokerName, String brokerAddr) {
        this.brokerName = brokerName;
        this.brokerAddr = brokerAddr;

        // 连接到NameServer
        try {
            String[] parts = nameServerAddr.split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9876;

            this.nameServerClient = new NettyClient(host, port);
            this.nameServerClient.connect();

            logger.info("TopicManager connected to NameServer: {}", nameServerAddr);
        } catch (Exception e) {
            logger.error("Failed to connect to NameServer: " + nameServerAddr, e);
        }
    }
    
    /**
     * 创建Topic
     */
    public boolean createTopic(String topicName, int queueCount, int permission) {
        if (topicName == null || topicName.trim().isEmpty()) {
            logger.warn("Topic名称不能为空");
            return false;
        }
        
        if (queueCount <= 0) {
            logger.warn("队列数量必须大于0: {}", queueCount);
            return false;
        }
        
        // 检查Topic是否已存在
        if (topicConfigTable.containsKey(topicName)) {
            logger.warn("Topic已存在: {}", topicName);
            return false;
        }
        
        try {
            TopicConfig topicConfig = new TopicConfig(topicName, queueCount, permission);
            topicConfigTable.put(topicName, topicConfig);

            // 向NameServer注册Topic路由信息
            registerTopicRoute(topicName, queueCount, queueCount, permission);

            logger.info("创建Topic成功: {}, 队列数量: {}, 权限: {}",
                       topicName, queueCount, permission);
            return true;

        } catch (Exception e) {
            logger.error("创建Topic失败: " + topicName, e);
            return false;
        }
    }
    
    /**
     * 创建Topic（使用默认配置）
     */
    public boolean createTopic(String topicName) {
        return createTopic(topicName, DEFAULT_QUEUE_COUNT, DEFAULT_PERMISSION);
    }
    
    /**
     * 删除Topic
     */
    public boolean deleteTopic(String topicName) {
        if (topicName == null || topicName.trim().isEmpty()) {
            logger.warn("Topic名称不能为空");
            return false;
        }

        TopicConfig removed = topicConfigTable.remove(topicName);
        if (removed != null) {
            // 通知NameServer移除路由
            deregisterTopicRoute(topicName);
            logger.info("删除Topic成功: {}", topicName);
            return true;
        } else {
            logger.warn("Topic不存在，无法删除: {}", topicName);
            return false;
        }
    }
    
    /**
     * 查询Topic配置
     */
    public TopicConfig getTopicConfig(String topicName) {
        return topicConfigTable.get(topicName);
    }
    
    /**
     * 检查Topic是否存在
     */
    public boolean topicExists(String topicName) {
        return topicConfigTable.containsKey(topicName);
    }
    
    /**
     * 获取所有Topic列表
     */
    public List<String> getAllTopics() {
        return new ArrayList<>(topicConfigTable.keySet());
    }
    
    /**
     * 获取所有Topic配置
     */
    public Map<String, TopicConfig> getAllTopicConfigs() {
        return new ConcurrentHashMap<>(topicConfigTable);
    }
    
    /**
     * 更新Topic配置
     */
    public boolean updateTopicConfig(String topicName, int queueCount, int permission) {
        TopicConfig existingConfig = topicConfigTable.get(topicName);
        if (existingConfig == null) {
            logger.warn("Topic不存在，无法更新: {}", topicName);
            return false;
        }
        
        try {
            TopicConfig newConfig = new TopicConfig(topicName, queueCount, permission);
            topicConfigTable.put(topicName, newConfig);
            
            logger.info("更新Topic配置成功: {}, 队列数量: {} -> {}, 权限: {} -> {}", 
                       topicName, existingConfig.getQueueCount(), queueCount,
                       existingConfig.getPermission(), permission);
            return true;
            
        } catch (Exception e) {
            logger.error("更新Topic配置失败: " + topicName, e);
            return false;
        }
    }
    
    /**
     * 获取Topic统计信息
     */
    public TopicStats getTopicStats() {
        TopicStats stats = new TopicStats();
        stats.setTotalTopicCount(topicConfigTable.size());
        
        int totalQueueCount = 0;
        for (TopicConfig config : topicConfigTable.values()) {
            totalQueueCount += config.getQueueCount();
        }
        stats.setTotalQueueCount(totalQueueCount);
        
        return stats;
    }
    
    /**
     * 清空所有Topic
     */
    public void clearAllTopics() {
        int count = topicConfigTable.size();
        topicConfigTable.clear();
        logger.info("清空所有Topic，共删除: {} 个", count);
    }
    
    /**
     * 初始化默认Topic
     */
    public void initializeDefaultTopics() {
        // 创建一些默认的Topic
        createTopic("default-topic", 4, TopicPermission.READ_WRITE);
        createTopic("order-topic", 8, TopicPermission.READ_WRITE);
        createTopic("log-topic", 2, TopicPermission.WRITE_ONLY);
        createTopic("notification-topic", 4, TopicPermission.READ_WRITE);
        
        logger.info("初始化默认Topic完成，共创建: {} 个", topicConfigTable.size());
    }
    
    /**
     * 获取Topic数量
     */
    public int getTopicCount() {
        return topicConfigTable.size();
    }
    
    /**
     * 验证Topic名称格式
     */
    public boolean isValidTopicName(String topicName) {
        if (topicName == null || topicName.trim().isEmpty()) {
            return false;
        }

        // Topic名称长度限制
        if (topicName.length() > 255) {
            return false;
        }

        // Topic名称只能包含字母、数字、下划线、中划线、点号
        return topicName.matches("^[a-zA-Z0-9_.-]+$");
    }

    /**
     * 向NameServer注册Topic路由信息
     */
    private void registerTopicRoute(String topicName, int readQueueNums, int writeQueueNums, int perm) {
        if (nameServerClient == null || !nameServerClient.isConnected()) {
            logger.warn("NameServer connection not available, skip route registration for topic: {}", topicName);
            return;
        }

        try {
            RegisterTopicRouteRequest request = new RegisterTopicRouteRequest();
            request.topic = topicName;
            request.brokerName = this.brokerName;
            request.brokerAddr = this.brokerAddr;
            request.readQueueNums = readQueueNums;
            request.writeQueueNums = writeQueueNums;
            request.perm = perm;

            String requestJson = JsonUtils.toJson(request);
            ProtocolMessage protocolMessage = new ProtocolMessage(
                MessageType.REGISTER_TOPIC_ROUTE_REQUEST,
                requestJson.getBytes(StandardCharsets.UTF_8)
            );

            ProtocolMessage response = nameServerClient.sendSync(protocolMessage, 5000);
            if (response != null && response.getStatus().getCode() == 0) {
                logger.info("Successfully registered topic route to NameServer: topic={}, broker={}",
                           topicName, brokerName);
            } else {
                logger.warn("Failed to register topic route to NameServer: topic={}, response={}",
                           topicName, response != null ? response.getStatus() : "null");
            }

        } catch (Exception e) {
            logger.error("Error registering topic route to NameServer: topic=" + topicName, e);
        }
    }

    /**
     * 向NameServer发送取消Topic路由注册请求
     */
    private void deregisterTopicRoute(String topicName) {
        if (nameServerClient == null || !nameServerClient.isConnected()) {
            logger.warn("NameServer connection not available, skip route deregistration for topic: {}", topicName);
            return;
        }
        try {
            RegisterTopicRouteRequest request = new RegisterTopicRouteRequest();
            request.topic = topicName;
            request.brokerName = this.brokerName;
            request.brokerAddr = this.brokerAddr;
            request.readQueueNums = 0;
            request.writeQueueNums = 0;
            request.perm = 0;

            String requestJson = JsonUtils.toJson(request);
            ProtocolMessage protocolMessage = new ProtocolMessage(
                MessageType.REGISTER_TOPIC_ROUTE_REQUEST,
                requestJson.getBytes(StandardCharsets.UTF_8));

            ProtocolMessage response = nameServerClient.sendSync(protocolMessage, 5000);
            if (response != null && response.getStatus().getCode() == 0) {
                logger.info("Successfully deregistered topic route from NameServer: topic={}", topicName);
            } else {
                logger.warn("Failed to deregister topic route from NameServer: topic={}", topicName);
            }
        } catch (Exception e) {
            logger.error("Error deregistering topic route from NameServer: topic=" + topicName, e);
        }
    }

    /**
     * 关闭TopicManager
     */
    public void shutdown() {
        if (nameServerClient != null) {
            nameServerClient.disconnect();
            logger.info("TopicManager disconnected from NameServer");
        }
    }

    // ===== DTO Classes =====
    static class RegisterTopicRouteRequest {
        public String topic;
        public String brokerName;
        public String brokerAddr;
        public int readQueueNums;
        public int writeQueueNums;
        public int perm;
    }
}
