package com.flare.mq.client.producer;

import com.flare.mq.protocol.client.NettyClient;
import com.flare.mq.protocol.ProtocolMessage;
import com.flare.mq.protocol.MessageType;
import com.flare.mq.protocol.ResponseCode;
import com.flare.mq.protocol.client.ResponseCallback;
import com.flare.mq.common.util.JsonUtils;
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
 * @author FlareMQ Team
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
        checkProducerStatus();
        validateMessage(message);

        if (message.getMessageId() == null) {
            message.setMessageId(generateMessageId());
        }

        TopicRouteInfo routeInfo = getTopicRouteInfo(message.getTopic());
        if (routeInfo == null) {
            throw new Exception("No route info found for topic: " + message.getTopic());
        }

        int maxRetries = config.getRetryTimesWhenSendFailed();
        String excludeBrokerName = null;
        SendResult lastResult = null;

        for (int retryCount = 0; retryCount <= maxRetries; retryCount++) {
            long startTime = System.currentTimeMillis();
            String currentBroker = null;

            try {
                // Select queue — exclude failed broker on retry
                TopicRouteInfo.QueueInfo queueInfo;
                if (retryCount == 0 || excludeBrokerName == null) {
                    queueInfo = routeInfo.selectQueue();
                } else {
                    queueInfo = routeInfo.selectAnotherQueue(excludeBrokerName);
                    if (queueInfo == null) {
                        logger.warn("No alternative broker for {}, retrying same broker", excludeBrokerName);
                        queueInfo = routeInfo.selectQueue();
                    }
                }

                if (queueInfo == null) {
                    long costTime = System.currentTimeMillis() - startTime;
                    stats.recordSendFailure(costTime);
                    return SendResult.failure("No available queue for topic: " + message.getTopic());
                }

                currentBroker = queueInfo.getBrokerName();

                // Clean dead connection on retry
                if (retryCount > 0 && excludeBrokerName != null) {
                    NettyClient deadClient = brokerClients.remove(excludeBrokerName);
                    if (deadClient != null) {
                        try { deadClient.disconnect(); } catch (Exception ignore) {}
                        logger.info("Removed dead broker connection: {}", excludeBrokerName);
                    }
                }

                // Get broker client
                NettyClient brokerClient = getBrokerClient(currentBroker, routeInfo);
                if (brokerClient == null) {
                    lastResult = SendResult.failure("Cannot connect to broker: " + currentBroker);
                    if (retryCount < maxRetries) {
                        excludeBrokerName = currentBroker;
                        continue;
                    }
                    return lastResult;
                }

                // Build and send
                ProtocolMessage protocolMessage = buildProtocolMessage(message);
                ProtocolMessage response = brokerClient.sendSync(protocolMessage, timeoutMs);

                // Handle response
                SendResult result = handleSendResponse(response, message, currentBroker, routeInfo);
                long costTime = System.currentTimeMillis() - startTime;
                result.setCostTime(costTime);

                if (result.isSuccess()) {
                    stats.recordSendSuccess(costTime, message.getMessageSize());
                    return result;
                }

                stats.recordSendFailure(costTime);
                lastResult = result;

                if (retryCount < maxRetries && isRetryableError(result)) {
                    logger.warn("Send failed, will retry (attempt {}/{}): broker={}, status={}",
                            retryCount + 1, maxRetries, currentBroker, result.getSendStatus());
                    excludeBrokerName = currentBroker;
                    continue;
                }

                return result;

            } catch (Exception e) {
                long costTime = System.currentTimeMillis() - startTime;
                stats.recordSendFailure(costTime);

                lastResult = SendResult.failure(
                        e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());

                if (retryCount < maxRetries && isNetworkError(e)) {
                    logger.warn("Network error, will retry (attempt {}/{}): broker={}, error={}",
                            retryCount + 1, maxRetries, currentBroker, e.getMessage());
                    excludeBrokerName = currentBroker;
                    continue;
                }

                logger.error("Send message failed (non-retryable): " + message, e);
                return lastResult;
            }
        }

        return lastResult != null ? lastResult : SendResult.failure("Retry exhausted");
    }
    
    @Override
    public void sendAsync(Message message, SendCallback callback) {
        sendAsync(message, callback, config.getSendMsgTimeout());
    }
    
    @Override
    public void sendAsync(Message message, SendCallback callback, long timeoutMs) {
        try {
            checkProducerStatus();
            validateMessage(message);
        } catch (Exception e) {
            if (callback != null) {
                callbackExecutor.execute(() -> callback.onException(e));
            }
            return;
        }

        if (message.getMessageId() == null) {
            message.setMessageId(generateMessageId());
        }

        TopicRouteInfo routeInfo;
        try {
            routeInfo = getTopicRouteInfo(message.getTopic());
            if (routeInfo == null) {
                throw new Exception("No route info found for topic: " + message.getTopic());
            }
        } catch (Exception e) {
            if (callback != null) {
                callbackExecutor.execute(() -> callback.onException(e));
            }
            return;
        }

        int maxRetries = config.getRetryTimesWhenSendAsyncFailed();
        doSendAsyncWithRetry(message, routeInfo, null, 0, maxRetries,
                timeoutMs, callback, System.currentTimeMillis());
    }

    /**
     * Recursive async send with retry
     */
    private void doSendAsyncWithRetry(Message message, TopicRouteInfo routeInfo,
            String excludeBrokerName, int retryCount, int maxRetries,
            long timeoutMs, SendCallback callback, long startTime) {

        // Select queue
        TopicRouteInfo.QueueInfo queueInfo;
        if (retryCount == 0 || excludeBrokerName == null) {
            queueInfo = routeInfo.selectQueue();
        } else {
            queueInfo = routeInfo.selectAnotherQueue(excludeBrokerName);
            if (queueInfo == null) {
                queueInfo = routeInfo.selectQueue();
            }
        }

        if (queueInfo == null) {
            if (callback != null) {
                callbackExecutor.execute(() ->
                        callback.onException(new Exception("No available queue")));
            }
            return;
        }

        final String currentBroker = queueInfo.getBrokerName();

        // Clean dead connection on retry
        if (retryCount > 0 && excludeBrokerName != null) {
            NettyClient deadClient = brokerClients.remove(excludeBrokerName);
            if (deadClient != null) {
                try { deadClient.disconnect(); } catch (Exception ignore) {}
                logger.info("Removed dead broker connection: {}", excludeBrokerName);
            }
        }

        NettyClient brokerClient = getBrokerClient(currentBroker, routeInfo);
        if (brokerClient == null) {
            if (retryCount < maxRetries) {
                doSendAsyncWithRetry(message, routeInfo, currentBroker,
                        retryCount + 1, maxRetries, timeoutMs, callback, startTime);
                return;
            }
            if (callback != null) {
                callbackExecutor.execute(() ->
                        callback.onException(new Exception("Cannot connect to broker: " + currentBroker)));
            }
            return;
        }

        ProtocolMessage protocolMessage = buildProtocolMessage(message);

        brokerClient.sendAsync(protocolMessage, new ResponseCallback() {
            @Override
            public void onSuccess(ProtocolMessage response) {
                callbackExecutor.execute(() -> {
                    try {
                        SendResult result = handleSendResponse(response, message,
                                currentBroker, routeInfo);
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
                    if (retryCount < maxRetries && isNetworkError(throwable)) {
                        logger.warn("Async send failed, retrying (attempt {}/{}): broker={}",
                                retryCount + 1, maxRetries, currentBroker, throwable);
                        doSendAsyncWithRetry(message, routeInfo, currentBroker,
                                retryCount + 1, maxRetries, timeoutMs, callback, startTime);
                        return;
                    }

                    long costTime = System.currentTimeMillis() - startTime;
                    stats.recordSendFailure(costTime);
                    if (callback != null) {
                        callback.onException(throwable);
                    }
                });
            }

            @Override
            public void onTimeout() {
                callbackExecutor.execute(() -> {
                    if (retryCount < maxRetries) {
                        logger.warn("Async send timeout, retrying (attempt {}/{}): broker={}",
                                retryCount + 1, maxRetries, currentBroker);
                        doSendAsyncWithRetry(message, routeInfo, currentBroker,
                                retryCount + 1, maxRetries, timeoutMs, callback, startTime);
                        return;
                    }

                    long costTime = System.currentTimeMillis() - startTime;
                    stats.recordSendTimeout(costTime);
                    if (callback != null) {
                        callback.onException(new RuntimeException("Send async timeout after "
                                + maxRetries + " retries"));
                    }
                });
            }
        });
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
     * Handle send response — parse structured JSON from broker
     */
    private SendResult handleSendResponse(ProtocolMessage response, Message message,
                                           String brokerName, TopicRouteInfo routeInfo) {
        if (response == null) {
            return SendResult.failure("Response is null");
        }

        if (response.getStatus() == ResponseCode.SUCCESS) {
            try {
                // Parse structured SendResponse JSON
                String body = response.getBody() != null
                        ? new String(response.getBody(), StandardCharsets.UTF_8) : "";
                SendResponseDto dto = JsonUtils.fromJson(body, SendResponseDto.class);

                if (dto != null && dto.messageId != null) {
                    SendResult result = SendResult.success(dto.messageId, dto.queueId, dto.offset);
                    result.setBrokerAddr(resolveBrokerAddr(brokerName, routeInfo));
                    return result;
                }

                // Fallback: broker returned success but body is not parseable
                SendResult result = SendResult.success(message.getMessageId(), 0, 0);
                result.setBrokerAddr(resolveBrokerAddr(brokerName, routeInfo));
                return result;

            } catch (Exception e) {
                logger.warn("Failed to parse broker response, using fallback: {}", e.getMessage());
                SendResult result = SendResult.success(message.getMessageId(), 0, 0);
                result.setBrokerAddr(resolveBrokerAddr(brokerName, routeInfo));
                return result;
            }
        } else {
            return SendResult.failure(
                    "Send failed, error code: " + response.getStatus(),
                    response.getStatus().getCode());
        }
    }

    private String resolveBrokerAddr(String brokerName, TopicRouteInfo routeInfo) {
        TopicRouteInfo.BrokerInfo brokerInfo = routeInfo.getBrokerInfo(brokerName);
        return brokerInfo != null ? brokerInfo.getMasterAddr() : null;
    }

    /**
     * 判断 SendResult 是否可重试
     */
    private boolean isRetryableError(SendResult result) {
        if (result == null) return true;
        return result.needRetry();
    }

    /**
     * 判断异常是否为网络相关（可重试）
     */
    private boolean isNetworkError(Throwable e) {
        if (e == null) return false;
        String msg = e.getClass().getName() + ": " + (e.getMessage() != null ? e.getMessage() : "");
        return msg.contains("ConnectException")
            || msg.contains("connect")
            || msg.contains("timeout")
            || msg.contains("Timeout")
            || msg.contains("Connection refused")
            || msg.contains("SocketException")
            || msg.contains("Not connected")
            || msg.contains("Channel");
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

    // Inner DTO for structured broker response
    static class SendResponseDto {
        public String messageId;
        public int queueId;
        public long offset;
        public String topic;
    }
}
