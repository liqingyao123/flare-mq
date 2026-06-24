package com.ruyuan.mq.client.producer;

import com.ruyuan.mq.protocol.client.NettyClient;
import com.ruyuan.mq.protocol.ProtocolMessage;
import com.ruyuan.mq.protocol.MessageType;
import com.ruyuan.mq.protocol.ResponseCode;
import com.ruyuan.mq.protocol.client.ResponseCallback;
import com.ruyuan.mq.common.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.nio.charset.StandardCharsets;


/**
 * Producer implementation class
 *
 * @author RuYuan MQ Team
 */
public class ProducerImpl implements Producer {

    private static final Logger logger = LoggerFactory.getLogger(ProducerImpl.class);

    /**
     * Producer configuration
     */
    private final ProducerConfig config;

    /**
     * Producer status
     */
    private volatile ProducerStatus status;

    /**
     * Producer statistics
     */
    private final ProducerStats stats;

    /**
     * NameServer client for route discovery
     */
    private NettyClient nameServerClient;

    /**
     * Broker clients for message sending
     */
    private final Map<String, NettyClient> brokerClients = new ConcurrentHashMap<>();

    /**
     * Async callback executor
     */
    private ThreadPoolExecutor callbackExecutor;

    /**
     * Message ID generator
     */
    private final AtomicInteger messageIdGenerator = new AtomicInteger(0);

    /**
     * Topic route cache
     */
    private final Map<String, TopicRouteInfo> topicRouteCache = new ConcurrentHashMap<>();

    /**
     * Constructor
     */
    public ProducerImpl(ProducerConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("ProducerConfig cannot be null");
        }
        if (!config.isValid()) {
            throw new IllegalArgumentException("ProducerConfig is invalid");
        }
        
        this.config = config.copy();
        this.status = ProducerStatus.CREATE_JUST;
        this.stats = new ProducerStats();
    }
    
    @Override
    public void start() throws Exception {
        if (status != ProducerStatus.CREATE_JUST) {
            logger.warn("Producer already started or closed, current status: {}", status);
            return;
        }

        try {
            logger.info("Starting Producer: {}", config.getProducerGroup());

            // Initialize NameServer client
            initNameServerClient();

            // Initialize callback executor
            initCallbackExecutor();

            status = ProducerStatus.RUNNING;
            logger.info("Producer started successfully: {}", config.getProducerGroup());

        } catch (Exception e) {
            status = ProducerStatus.START_FAILED;
            logger.error("Producer startup failed: " + config.getProducerGroup(), e);
            throw e;
        }
    }
    
    @Override
    public void shutdown() {
        if (status == ProducerStatus.SHUTDOWN_ALREADY) {
            logger.warn("Producer already closed");
            return;
        }

        logger.info("Starting to close Producer: {}", config.getProducerGroup());
        
        status = ProducerStatus.SHUTDOWN_ALREADY;

        // Close NameServer client
        if (nameServerClient != null) {
            nameServerClient.disconnect();
        }

        // Close all broker clients
        for (NettyClient brokerClient : brokerClients.values()) {
            if (brokerClient != null) {
                brokerClient.disconnect();
            }
        }
        brokerClients.clear();

        // Close callback executor
        if (callbackExecutor != null) {
            callbackExecutor.shutdown();
            try {
                if (!callbackExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    callbackExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                callbackExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        logger.info("Producer closed successfully: {}", config.getProducerGroup());
    }
    
    @Override
    public SendResult send(Message message) throws Exception {
        return send(message, config.getSendMsgTimeout());
    }
    
    @Override
    public SendResult send(Message message, long timeoutMs) throws Exception {
        // Check status
        checkProducerStatus();

        // Validate message
        validateMessage(message);

        long startTime = System.currentTimeMillis();

        try {
            // Generate message ID
            if (message.getMessageId() == null) {
                message.setMessageId(generateMessageId());
            }

            // Get topic route info
            TopicRouteInfo routeInfo = getTopicRouteInfo(message.getTopic());
            if (routeInfo == null) {
                throw new Exception("No route info found for topic: " + message.getTopic());
            }

            // Select queue
            TopicRouteInfo.QueueInfo queueInfo = routeInfo.selectQueue();
            if (queueInfo == null) {
                throw new Exception("No available queue for topic: " + message.getTopic());
            }

            // Get broker client
            NettyClient brokerClient = getBrokerClient(queueInfo.getBrokerName(), routeInfo);
            if (brokerClient == null) {
                throw new Exception("Cannot connect to broker: " + queueInfo.getBrokerName());
            }

            // Build protocol message
            ProtocolMessage protocolMessage = buildProtocolMessage(message);

            // Send message to broker
            ProtocolMessage response = brokerClient.sendSync(protocolMessage, timeoutMs);

            // Handle response
            SendResult result = handleSendResponse(response, message);

            // Record statistics
            long costTime = System.currentTimeMillis() - startTime;
            result.setCostTime(costTime);
            
            if (result.isSuccess()) {
                stats.recordSendSuccess(costTime, message.getMessageSize());
            } else {
                stats.recordSendFailure(costTime);
            }
            
            return result;
            
        } catch (Exception e) {
            long costTime = System.currentTimeMillis() - startTime;
            stats.recordSendFailure(costTime);
            
            logger.error("Send message failed: " + message, e);
            throw e;
        }
    }
    
    @Override
    public void sendAsync(Message message, SendCallback callback) {
        sendAsync(message, callback, config.getSendMsgTimeout());
    }
    
    @Override
    public void sendAsync(Message message, SendCallback callback, long timeoutMs) {
        // Check status
        try {
            checkProducerStatus();
            validateMessage(message);
        } catch (Exception e) {
            if (callback != null) {
                callbackExecutor.execute(() -> callback.onException(e));
            }
            return;
        }

        long startTime = System.currentTimeMillis();

        try {
            // Generate message ID
            if (message.getMessageId() == null) {
                message.setMessageId(generateMessageId());
            }

            // Get topic route info
            TopicRouteInfo routeInfo = getTopicRouteInfo(message.getTopic());
            if (routeInfo == null) {
                throw new Exception("No route info found for topic: " + message.getTopic());
            }

            // Select queue
            TopicRouteInfo.QueueInfo queueInfo = routeInfo.selectQueue();
            if (queueInfo == null) {
                throw new Exception("No available queue for topic: " + message.getTopic());
            }

            // Get broker client
            NettyClient brokerClient = getBrokerClient(queueInfo.getBrokerName(), routeInfo);
            if (brokerClient == null) {
                throw new Exception("Cannot connect to broker: " + queueInfo.getBrokerName());
            }

            // Build protocol message
            ProtocolMessage protocolMessage = buildProtocolMessage(message);

            // Send message asynchronously to broker
            brokerClient.sendAsync(protocolMessage, new ResponseCallback() {
                @Override
                public void onSuccess(ProtocolMessage response) {
                    callbackExecutor.execute(() -> {
                        try {
                            SendResult result = handleSendResponse(response, message);
                            long costTime = System.currentTimeMillis() - startTime;
                            result.setCostTime(costTime);
                            
                            if (result.isSuccess()) {
                                stats.recordSendSuccess(costTime, message.getMessageSize());
                            } else {
                                stats.recordSendFailure(costTime);
                            }
                            
                            if (callback != null) {
                                callback.onSuccess(result);
                            }
                        } catch (Exception e) {
                            long costTime = System.currentTimeMillis() - startTime;
                            stats.recordSendFailure(costTime);
                            
                            if (callback != null) {
                                callback.onException(e);
                            }
                        }
                    });
                }
                
                @Override
                public void onFailure(Throwable throwable) {
                    callbackExecutor.execute(() -> {
                        long costTime = System.currentTimeMillis() - startTime;
                        stats.recordSendFailure(costTime);
                        
                        if (callback != null) {
                            callback.onException(throwable);
                        }
                    });
                }
            });
            
        } catch (Exception e) {
            long costTime = System.currentTimeMillis() - startTime;
            stats.recordSendFailure(costTime);
            
            if (callback != null) {
                callbackExecutor.execute(() -> callback.onException(e));
            }
        }
    }
    
    @Override
    public void sendOneway(Message message) throws Exception {
        // Check status
        checkProducerStatus();

        // Validate message
        validateMessage(message);

        // Generate message ID
        if (message.getMessageId() == null) {
            message.setMessageId(generateMessageId());
        }

        // Get topic route info
        TopicRouteInfo routeInfo = getTopicRouteInfo(message.getTopic());
        if (routeInfo == null) {
            logger.error("No route info found for topic: {}", message.getTopic());
            return;
        }

        // Select queue
        TopicRouteInfo.QueueInfo queueInfo = routeInfo.selectQueue();
        if (queueInfo == null) {
            logger.error("No available queue for topic: {}", message.getTopic());
            return;
        }

        // Get broker client
        NettyClient brokerClient = getBrokerClient(queueInfo.getBrokerName(), routeInfo);
        if (brokerClient == null) {
            logger.error("Cannot connect to broker: {}", queueInfo.getBrokerName());
            return;
        }

        // Build protocol message
        ProtocolMessage protocolMessage = buildProtocolMessage(message);

        // Send oneway (no response expected)
        brokerClient.sendAsync(protocolMessage, null);
    }
    
    @Override
    public ProducerStatus getStatus() {
        return status;
    }
    
    @Override
    public ProducerConfig getConfig() {
        return config.copy();
    }
    
    @Override
    public ProducerStats getStats() {
        return stats;
    }
    
    /**
     * Initialize NameServer client
     */
    private void initNameServerClient() {
        // Parse NameServer address - support multiple addresses separated by semicolon
        String nameServerAddr = config.getNameServerAddr();
        String[] addresses = nameServerAddr.split(";");

        // Implement simple round-robin + failover for multiple NameServers
        List<String> addressList = new ArrayList<>();
        for (String addr : addresses) {
            String a = addr.trim();
            if (!a.isEmpty()) addressList.add(a);
        }
        if (addressList.isEmpty()) {
            throw new IllegalArgumentException("No valid NameServer address configured");
        }

        // Round-robin select with fallback
        Exception lastEx = null;
        for (int i = 0; i < addressList.size(); i++) {
            String candidate = addressList.get((int)(System.nanoTime() % addressList.size()));
            String[] parts = candidate.split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9876;

            try {
                nameServerClient = new NettyClient(host, port);
                nameServerClient.connect();
                logger.info("Connected to NameServer: {}", candidate);
                return;
            } catch (Exception ex) {
                lastEx = ex;
                logger.warn("Failed to connect to NameServer: {}, will try next", candidate, ex);
                // ensure client is disconnected before next attempt
                try { if (nameServerClient != null) nameServerClient.disconnect(); } catch (Exception ignore) {}
            }
        }
        throw new RuntimeException("All NameServer addresses are unreachable: " + addressList, lastEx);
    }

    /**
     * Initialize callback executor
     */
    private void initCallbackExecutor() {
        callbackExecutor = new ThreadPoolExecutor(
            config.getClientCallbackExecutorThreads(),
            config.getClientCallbackExecutorThreads(),
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(10000),
            r -> new Thread(r, "ProducerCallback-" + config.getProducerGroup())
        );
    }
    
    /**
     * Get topic route info from NameServer
     */
    private TopicRouteInfo getTopicRouteInfo(String topic) {
        try {
            // Check cache first
            TopicRouteInfo cachedInfo = topicRouteCache.get(topic);
            if (cachedInfo != null && !cachedInfo.isExpired()) {
                return cachedInfo;
            }

            // Query from NameServer
            String requestJson = JsonUtils.toJson(new GetRouteInfoRequest(topic));
            ProtocolMessage request = new ProtocolMessage(
                MessageType.GET_ROUTEINFO_BY_TOPIC_REQUEST,
                requestJson.getBytes(StandardCharsets.UTF_8)
            );

            ProtocolMessage response = nameServerClient.sendSync(request, 5000);
            if (response.getStatus() != ResponseCode.SUCCESS) {
                logger.error("Failed to get route info for topic: {}, code: {}",
                           topic, response.getStatus());
                return null;
            }

            // Parse response
            String responseJson = new String(response.getBody(), StandardCharsets.UTF_8);
            GetRouteInfoResponse routeResponse = JsonUtils.fromJson(responseJson, GetRouteInfoResponse.class);

            if (routeResponse == null || routeResponse.topicRouteData == null) {
                logger.warn("Empty route data for topic: {}", topic);
                return null;
            }

            // Convert to TopicRouteInfo
            TopicRouteInfo routeInfo = convertToTopicRouteInfo(topic, routeResponse.topicRouteData);

            // Update cache
            topicRouteCache.put(topic, routeInfo);

            logger.debug("Updated route info for topic: {}, queues: {}, brokers: {}",
                       topic, routeInfo.getQueueInfos().size(), routeInfo.getBrokerInfos().size());

            return routeInfo;

        } catch (Exception e) {
            logger.error("Failed to get route info for topic: " + topic, e);
            return null;
        }
    }

    /**
     * Get or create broker client
     */
    private NettyClient getBrokerClient(String brokerName, TopicRouteInfo routeInfo) {
        try {
            // Check existing connection
            NettyClient existingClient = brokerClients.get(brokerName);
            if (existingClient != null && existingClient.isConnected()) {
                return existingClient;
            }

            // Get broker address
            TopicRouteInfo.BrokerInfo brokerInfo = routeInfo.getBrokerInfo(brokerName);
            if (brokerInfo == null) {
                logger.error("Broker info not found: {}", brokerName);
                return null;
            }

            String brokerAddr = brokerInfo.getMasterAddr();
            if (brokerAddr == null) {
                logger.error("Master broker address not found: {}", brokerName);
                return null;
            }

            // Parse address
            String[] parts = brokerAddr.split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 10911;

            // Create new connection
            NettyClient brokerClient = new NettyClient(host, port);
            brokerClient.connect();

            // Cache the connection
            brokerClients.put(brokerName, brokerClient);

            logger.info("Connected to broker: {} at {}", brokerName, brokerAddr);
            return brokerClient;

        } catch (Exception e) {
            logger.error("Failed to connect to broker: " + brokerName, e);
            return null;
        }
    }

    /**
     * Check Producer status
     */
    private void checkProducerStatus() throws Exception {
        if (status != ProducerStatus.RUNNING) {
            throw new IllegalStateException("Producer status abnormal: " + status);
        }
    }

    /**
     * Validate message
     */
    private void validateMessage(Message message) throws Exception {
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }

        if (!message.isValid()) {
            throw new IllegalArgumentException("Message format invalid: " + message);
        }

        if (message.getMessageSize() > config.getMaxMessageSize()) {
            throw new IllegalArgumentException("Message size exceeds limit: " + message.getMessageSize() + " > " + config.getMaxMessageSize());
        }
    }

    /**
     * Generate message ID
     */
    private String generateMessageId() {
        return config.getProducerGroup() + "_" + 
               System.currentTimeMillis() + "_" + 
               messageIdGenerator.incrementAndGet();
    }
    
    /**
     * Build protocol message
     */
    private ProtocolMessage buildProtocolMessage(Message message) {
        // Simplified handling, should serialize Message object in practice
        String messageJson = String.format(
            "{\"messageId\":\"%s\",\"topic\":\"%s\",\"tags\":\"%s\",\"key\":\"%s\",\"body\":\"%s\"}",
            message.getMessageId(),
            message.getTopic(),
            message.getTags(),
            message.getKey(),
            new String(message.getBody())
        );

        // Use constructor that automatically generates requestId
        return new ProtocolMessage(MessageType.SEND_MESSAGE_REQUEST, messageJson.getBytes());
    }

    /**
     * Handle send response
     */
    private SendResult handleSendResponse(ProtocolMessage response, Message message) {
        if (response == null) {
            return SendResult.failure("Response is null");
        }

        if (response.getStatus() == ResponseCode.SUCCESS) {
            // Parse response body to get queue info (simplified handling)
            return SendResult.success(message.getMessageId(), 0, System.currentTimeMillis());
        } else {
            return SendResult.failure("Send failed, error code: " + response.getStatus(), response.getStatus().getCode());
        }
    }

    /**
     * Convert NameServer response to TopicRouteInfo
     */
    private TopicRouteInfo convertToTopicRouteInfo(String topic, TopicRouteResponse routeData) {
        TopicRouteInfo routeInfo = new TopicRouteInfo(topic);

        // Convert queue data
        List<TopicRouteInfo.QueueInfo> queueInfos = new ArrayList<>();
        if (routeData.queueDatas != null) {
            for (QueueDataResponse queueData : routeData.queueDatas) {
                for (int i = 0; i < queueData.writeQueueNums; i++) {
                    TopicRouteInfo.QueueInfo queueInfo = new TopicRouteInfo.QueueInfo(
                        queueData.brokerName, i, true, true
                    );
                    queueInfos.add(queueInfo);
                }
            }
        }
        routeInfo.setQueueInfos(queueInfos);

        // Convert broker data
        List<TopicRouteInfo.BrokerInfo> brokerInfos = new ArrayList<>();
        if (routeData.brokerDatas != null) {
            for (BrokerDataResponse brokerData : routeData.brokerDatas) {
                TopicRouteInfo.BrokerInfo brokerInfo = new TopicRouteInfo.BrokerInfo(
                    brokerData.brokerName, brokerData.cluster, brokerData.brokerAddrs
                );
                brokerInfos.add(brokerInfo);
            }
        }
        routeInfo.setBrokerInfos(brokerInfos);

        return routeInfo;
    }

    // ===== DTO Classes =====
    static class GetRouteInfoRequest {
        public String topic;

        public GetRouteInfoRequest() {}

        public GetRouteInfoRequest(String topic) {
            this.topic = topic;
        }
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
        public Map<Long, String> brokerAddrs;
    }
}
