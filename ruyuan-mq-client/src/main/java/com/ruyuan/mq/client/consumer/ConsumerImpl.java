package com.ruyuan.mq.client.consumer;

import com.ruyuan.mq.client.producer.Message;
import com.ruyuan.mq.client.producer.TopicRouteInfo;
import com.ruyuan.mq.protocol.client.NettyClient;
import com.ruyuan.mq.protocol.ProtocolMessage;
import com.ruyuan.mq.protocol.MessageType;
import com.ruyuan.mq.protocol.ResponseCode;
import com.ruyuan.mq.common.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.nio.charset.StandardCharsets;

/**
 * Consumer implementation class
 *
 * @author RuYuan MQ Team
 */
public class ConsumerImpl implements Consumer {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerImpl.class);

    /**
     * Consumer configuration
     */
    private final ConsumerConfig config;

    /**
     * Consumer status
     */
    private volatile ConsumerStatus status;

    /**
     * Consumer statistics
     */
    private final ConsumerStats stats;

    /**
     * Subscription information
     */
    private final ConcurrentMap<String, SubscriptionData> subscriptions = new ConcurrentHashMap<>();

    /**
     * Consume progress management
     */
    private final ConcurrentMap<String, Long> consumeProgress = new ConcurrentHashMap<>();

    /**
     * NameServer client for route discovery
     */
    private NettyClient nameServerClient;

    /**
     * Broker clients for message pulling
     */
    private final Map<String, NettyClient> brokerClients = new ConcurrentHashMap<>();

    /**
     * Topic route cache
     */
    private final Map<String, TopicRouteInfo> topicRouteCache = new ConcurrentHashMap<>();

    /**
     * Consume thread pool
     */
    private ThreadPoolExecutor consumeExecutor;

    /**
     * Pull message scheduler
     */
    private ScheduledExecutorService pullScheduler;

    /**
     * Constructor
     */
    public ConsumerImpl(ConsumerConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("ConsumerConfig cannot be null");
        }
        if (!config.isValid()) {
            throw new IllegalArgumentException("ConsumerConfig is invalid");
        }
        
        this.config = config.copy();
        this.status = ConsumerStatus.CREATE_JUST;
        this.stats = new ConsumerStats();
    }
    
    @Override
    public void start() throws Exception {
        if (status != ConsumerStatus.CREATE_JUST) {
            logger.warn("Consumer already started or closed, current status: {}", status);
            return;
        }

        try {
            logger.info("Starting Consumer: {}", config.getConsumerGroup());
            
            // Initialize NameServer client
            initNameServerClient();

            // Initialize consume thread pool
            initConsumeExecutor();

            // Initialize pull scheduler
            initPullScheduler();

            // Start pull task
            startPullTask();
            
            status = ConsumerStatus.RUNNING;
            logger.info("Consumer started successfully: {}", config.getConsumerGroup());

        } catch (Exception e) {
            status = ConsumerStatus.START_FAILED;
            logger.error("Consumer startup failed: " + config.getConsumerGroup(), e);
            throw e;
        }
    }
    
    @Override
    public void shutdown() {
        if (status == ConsumerStatus.SHUTDOWN_ALREADY) {
            logger.warn("Consumer already closed");
            return;
        }

        logger.info("Starting to close Consumer: {}", config.getConsumerGroup());
        
        status = ConsumerStatus.SHUTDOWN_ALREADY;
        
        // Close pull scheduler
        if (pullScheduler != null) {
            pullScheduler.shutdown();
            try {
                if (!pullScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    pullScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                pullScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // Close consume thread pool
        if (consumeExecutor != null) {
            consumeExecutor.shutdown();
            try {
                if (!consumeExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    consumeExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                consumeExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

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
        
        logger.info("Consumer closed successfully: {}", config.getConsumerGroup());
    }
    
    @Override
    public void subscribe(String topic, String tags, MessageListener listener) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic cannot be empty");
        }
        if (listener == null) {
            throw new IllegalArgumentException("MessageListener cannot be null");
        }

        SubscriptionData subscriptionData = new SubscriptionData(topic, tags, listener);
        subscriptions.put(topic, subscriptionData);

        // If Consumer is already running, start pull task for this topic
        if (status == ConsumerStatus.RUNNING && config.getConsumeType() == ConsumeType.CONSUME_ACTIVELY) {
            startPullTaskForTopic(topic);
        }

        logger.info("Subscribed to Topic successfully: topic={}, tags={}", topic, tags);
    }
    
    @Override
    public void unsubscribe(String topic) {
        if (topic == null || topic.trim().isEmpty()) {
            logger.warn("Topic cannot be empty");
            return;
        }

        SubscriptionData removed = subscriptions.remove(topic);
        if (removed != null) {
            logger.info("Unsubscribed from Topic successfully: {}", topic);
        } else {
            logger.warn("Topic not subscribed, cannot unsubscribe: {}", topic);
        }
    }
    
    @Override
    public PullResult pullMessage(String topic, int queueId, long offset, int maxNums) throws Exception {
        return pullMessage(topic, queueId, offset, maxNums, config.getPullMsgTimeout());
    }
    
    @Override
    public PullResult pullMessage(String topic, int queueId, long offset, int maxNums, long timeoutMs) throws Exception {
        // Check status
        checkConsumerStatus();

        // Validate parameters
        validatePullParams(topic, queueId, offset, maxNums);

        long startTime = System.currentTimeMillis();

        try {
            // Get topic route info
            TopicRouteInfo routeInfo = getTopicRouteInfo(topic);
            if (routeInfo == null) {
                throw new Exception("No route info found for topic: " + topic);
            }

            // Find queue info
            TopicRouteInfo.QueueInfo queueInfo = findQueueInfo(routeInfo, queueId);
            if (queueInfo == null) {
                throw new Exception("Queue not found: " + topic + "-" + queueId);
            }

            // Get broker client
            NettyClient brokerClient = getBrokerClient(queueInfo.getBrokerName(), routeInfo);
            if (brokerClient == null) {
                throw new Exception("Cannot connect to broker: " + queueInfo.getBrokerName());
            }

            // Build pull request
            ProtocolMessage request = buildPullRequest(topic, queueId, offset, maxNums);

            // Send pull request to broker
            ProtocolMessage response = brokerClient.sendSync(request, timeoutMs);

            // Handle pull response
            PullResult result = handlePullResponse(response);

            // Record statistics
            long costTime = System.currentTimeMillis() - startTime;
            result.setCostTime(costTime);
            
            if (result.isSuccess()) {
                stats.recordPullSuccess(costTime, result.getMessageCount());
            } else {
                stats.recordPullFailure(costTime);
            }
            
            return result;
            
        } catch (Exception e) {
            long costTime = System.currentTimeMillis() - startTime;
            stats.recordPullFailure(costTime);
            
            logger.error("Pull message failed: topic={}, queueId={}, offset={}", topic, queueId, offset, e);
            throw e;
        }
    }
    
    @Override
    public AckResult ackMessage(String messageId) throws Exception {
        List<String> messageIds = new ArrayList<>();
        messageIds.add(messageId);
        return ackMessages(messageIds);
    }
    
    @Override
    public AckResult ackMessages(List<String> messageIds) throws Exception {
        // Check status
        checkConsumerStatus();

        // Validate parameters
        if (messageIds == null || messageIds.isEmpty()) {
            throw new IllegalArgumentException("Message ID list cannot be empty");
        }

        long startTime = System.currentTimeMillis();

        try {
            // Get any subscribed topic for route info (simplified implementation)
            String topic = subscriptions.keySet().iterator().next();
            TopicRouteInfo routeInfo = getTopicRouteInfo(topic);
            if (routeInfo == null) {
                throw new Exception("No route info found for topic: " + topic);
            }

            // Use first available broker (simplified implementation)
            TopicRouteInfo.BrokerInfo brokerInfo = routeInfo.getBrokerInfos().get(0);
            NettyClient brokerClient = getBrokerClient(brokerInfo.getBrokerName(), routeInfo);
            if (brokerClient == null) {
                throw new Exception("Cannot connect to broker: " + brokerInfo.getBrokerName());
            }

            // Build ack request
            ProtocolMessage request = buildAckRequest(messageIds);

            // Send ack request to broker
            ProtocolMessage response = brokerClient.sendSync(request, config.getPullMsgTimeout());

            // Handle ack response
            AckResult result = handleAckResponse(response, messageIds);

            // Record statistics
            long costTime = System.currentTimeMillis() - startTime;
            result.setCostTime(costTime);
            
            return result;
            
        } catch (Exception e) {
            logger.error("Acknowledge message failed: messageIds={}", messageIds, e);
            throw e;
        }
    }
    
    @Override
    public ConsumerStatus getStatus() {
        return status;
    }
    
    @Override
    public ConsumerConfig getConfig() {
        return config.copy();
    }
    
    @Override
    public ConsumerStats getStats() {
        return stats;
    }
    
    @Override
    public Map<String, SubscriptionData> getSubscriptions() {
        Map<String, SubscriptionData> result = new ConcurrentHashMap<>();
        for (Map.Entry<String, SubscriptionData> entry : subscriptions.entrySet()) {
            result.put(entry.getKey(), entry.getValue().copy());
        }
        return result;
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

        Exception lastEx = null;
        // try all addresses once in a random start order for basic load spread
        int n = addressList.size();
        int start = (int)(System.nanoTime() % n);
        for (int i = 0; i < n; i++) {
            String candidate = addressList.get((start + i) % n);
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
                try { if (nameServerClient != null) nameServerClient.disconnect(); } catch (Exception ignore) {}
            }
        }
        throw new RuntimeException("All NameServer addresses are unreachable: " + addressList, lastEx);
    }

    /**
     * Initialize consume thread pool
     */
    private void initConsumeExecutor() {
        consumeExecutor = new ThreadPoolExecutor(
            config.getConsumeThreadNums(),
            config.getConsumeThreadMax(),
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(config.getConsumeQueueThreshold()),
            r -> new Thread(r, "ConsumeThread-" + config.getConsumerGroup())
        );
    }

    /**
     * Initialize pull scheduler
     */
    private void initPullScheduler() {
        // Use fixed size thread pool, not dependent on subscriptions.size()
        pullScheduler = Executors.newScheduledThreadPool(
            Math.max(2, Runtime.getRuntime().availableProcessors()),
            r -> new Thread(r, "PullScheduler-" + config.getConsumerGroup())
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
     * Find queue info by queue ID
     */
    private TopicRouteInfo.QueueInfo findQueueInfo(TopicRouteInfo routeInfo, int queueId) {
        for (TopicRouteInfo.QueueInfo queueInfo : routeInfo.getQueueInfos()) {
            if (queueInfo.getQueueId() == queueId) {
                return queueInfo;
            }
        }
        return null;
    }
    
    /**
     * Start pull task
     */
    private void startPullTask() {
        if (config.getConsumeType() == ConsumeType.CONSUME_ACTIVELY) {
            // Active pull mode
            for (String topic : subscriptions.keySet()) {
                startPullTaskForTopic(topic);
            }
        }
    }

    /**
     * Start pull task for specific topic
     */
    private void startPullTaskForTopic(String topic) {
        if (config.getConsumeType() == ConsumeType.CONSUME_ACTIVELY && pullScheduler != null) {
            pullScheduler.scheduleWithFixedDelay(
                () -> pullMessageForTopic(topic),
                0,
                Math.max(1, config.getPullInterval()),
                TimeUnit.MILLISECONDS
            );
            logger.debug("Started pull task for topic: {}", topic);
        }
    }

    /**
     * Pull messages for specified Topic
     */
    private void pullMessageForTopic(String topic) {
        try {
            SubscriptionData subscription = subscriptions.get(topic);
            if (subscription == null || !subscription.isEnabled()) {
                return;
            }

            // Simplified handling: assume only one queue
            int queueId = 0;
            String progressKey = topic + "_" + queueId;
            long offset = consumeProgress.getOrDefault(progressKey, 0L);

            PullResult pullResult = pullMessage(topic, queueId, offset, config.getPullBatchSize());

            if (pullResult.hasMessage()) {
                // Update consume progress
                consumeProgress.put(progressKey, pullResult.getNextBeginOffset());

                // Submit consume task
                consumeExecutor.submit(() -> consumeMessages(pullResult.getMessages(), subscription));

                logger.debug("Pulled messages: topic={}, queueId={}, offset={}, count={}, nextOffset={}",
                           topic, queueId, offset, pullResult.getMessageCount(), pullResult.getNextBeginOffset());
            } else {
                logger.debug("No messages pulled: topic={}, queueId={}, offset={}", topic, queueId, offset);
            }

        } catch (Exception e) {
            logger.error("Pull message failed: topic={}", topic, e);
        }
    }
    
    /**
     * Consume messages
     */
    private void consumeMessages(List<Message> messages, SubscriptionData subscription) {
        for (Message message : messages) {
            long startTime = System.currentTimeMillis();

            try {
                // Check tag matching
                if (!subscription.matchTag(message.getTags())) {
                    continue;
                }

                // Call message listener
                ConsumeStatus status = subscription.getMessageListener().consumeMessage(message);

                long costTime = System.currentTimeMillis() - startTime;

                if (status.isSuccess()) {
                    stats.recordConsumeSuccess(costTime, message.getMessageSize());
                    // Auto acknowledge message
                    ackMessage(message.getMessageId());
                } else {
                    stats.recordConsumeFailure(costTime);
                    logger.warn("Consume message failed: messageId={}, status={}", message.getMessageId(), status);
                }

            } catch (Exception e) {
                long costTime = System.currentTimeMillis() - startTime;
                stats.recordConsumeFailure(costTime);
                logger.error("Consume message exception: messageId=" + message.getMessageId(), e);
            }
        }
    }

    /**
     * Check Consumer status
     */
    private void checkConsumerStatus() throws Exception {
        if (!status.canConsumeMessage()) {
            throw new IllegalStateException("Consumer status abnormal: " + status);
        }
    }
    
    /**
     * Validate pull parameters
     */
    private void validatePullParams(String topic, int queueId, long offset, int maxNums) throws Exception {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic cannot be empty");
        }
        if (queueId < 0) {
            throw new IllegalArgumentException("Queue ID cannot be negative");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative");
        }
        if (maxNums <= 0) {
            throw new IllegalArgumentException("Max message count must be greater than 0");
        }
    }

    /**
     * Build pull request
     */
    private ProtocolMessage buildPullRequest(String topic, int queueId, long offset, int maxNums) {
        // Simplified handling: build JSON format request body
        String requestJson = String.format(
            "{\"topic\":\"%s\",\"queueId\":%d,\"offset\":%d,\"maxNums\":%d,\"consumerGroup\":\"%s\"}",
            topic, queueId, offset, maxNums, config.getConsumerGroup()
        );

        // Use constructor that automatically generates requestId
        return new ProtocolMessage(MessageType.PULL_MESSAGE_REQUEST, requestJson.getBytes());
    }

    /**
     * Handle pull response
     */
    private PullResult handlePullResponse(ProtocolMessage response) {
        if (response == null) {
            return PullResult.failure("Response is null");
        }

        if (response.getStatus() == ResponseCode.SUCCESS) {
            // Simplified handling: assume pull success but no messages
            return PullResult.noNewMessage(0, 0, 0);
        } else {
            return PullResult.failure("Pull failed, error code: " + response.getStatus(), response.getStatus().getCode());
        }
    }
    
    /**
     * Build ack request
     */
    private ProtocolMessage buildAckRequest(List<String> messageIds) {
        // Simplified handling: build JSON format request body
        StringBuilder sb = new StringBuilder();
        sb.append("{\"messageIds\":[");
        for (int i = 0; i < messageIds.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("\"").append(messageIds.get(i)).append("\"");
        }
        sb.append("],\"consumerGroup\":\"").append(config.getConsumerGroup()).append("\"}");

        // Use constructor that automatically generates requestId
        return new ProtocolMessage(MessageType.ACK_MESSAGE_REQUEST, sb.toString().getBytes());
    }

    /**
     * Handle ack response
     */
    private AckResult handleAckResponse(ProtocolMessage response, List<String> messageIds) {
        if (response == null) {
            return AckResult.failure("Response is null");
        }

        if (response.getStatus() == ResponseCode.SUCCESS) {
            return AckResult.success(messageIds);
        } else {
            return AckResult.failure("Ack failed, error code: " + response.getStatus(), response.getStatus().getCode());
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
                for (int i = 0; i < queueData.readQueueNums; i++) {
                    TopicRouteInfo.QueueInfo queueInfo = new TopicRouteInfo.QueueInfo(
                        queueData.brokerName, i, true, false
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
