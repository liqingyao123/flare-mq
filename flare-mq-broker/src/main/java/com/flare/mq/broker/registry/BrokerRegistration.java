package com.flare.mq.broker.registry;

import com.flare.mq.protocol.client.NettyClient;
import com.flare.mq.protocol.ProtocolMessage;
import com.flare.mq.protocol.MessageType;
import com.flare.mq.protocol.client.ResponseCallback;
import com.flare.mq.common.util.JsonUtils;
import com.flare.mq.store.DefaultMessageStore;
import com.flare.mq.broker.offset.ConsumerOffsetManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Broker注册管理器
 * 负责向NameServer注册Broker信息并维持心跳
 */
public class BrokerRegistration {
    
    private static final Logger logger = LoggerFactory.getLogger(BrokerRegistration.class);
    
    /**
     * 注册间隔时间（毫秒）
     */
    private static final long REGISTER_INTERVAL_MS = 30000; // 30秒
    
    /**
     * 心跳间隔时间（毫秒）
     */
    private static final long HEARTBEAT_INTERVAL_MS = 10000; // 10秒
    
    private final String clusterName;
    private final String brokerName;
    private final String brokerAddr;
    private final long brokerId;
    private final String haServerAddr;
    
    private NettyClient nameServerClient;
    private ScheduledExecutorService scheduledExecutor;
    private volatile boolean running = false;

    private DefaultMessageStore messageStore;
    private ConsumerOffsetManager offsetManager;
    private Map<String, Long> lastConsumedByGroup = new LinkedHashMap<>();
    private long lastConsumeStatsTimestamp;
    private long lastTotalMessageCount;
    private long lastRegisterTimestamp;

    // 心跳连续失败计数与监听（连续 3 次失败触发停写）
    private volatile int consecutiveHeartbeatFailures = 0;
    private volatile Runnable heartbeatLossListener;
    private volatile long currentEpoch = 0L;

    public void setHeartbeatLossListener(Runnable listener) {
        this.heartbeatLossListener = listener;
    }

    public void setCurrentEpoch(long epoch) {
        this.currentEpoch = epoch;
    }

    public BrokerRegistration(String clusterName, String brokerName, String brokerAddr, long brokerId) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.brokerAddr = brokerAddr;
        this.brokerId = brokerId;
        this.haServerAddr = brokerAddr.replace("10911", "10912"); // 简化的HA地址
    }

    public void setMessageStore(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public void setConsumerOffsetManager(ConsumerOffsetManager offsetManager) {
        this.offsetManager = offsetManager;
    }
    
    /**
     * 初始化并连接到NameServer
     */
    public void initialize(String nameServerAddr) {
        try {
            String[] parts = nameServerAddr.split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9876;
            
            this.nameServerClient = new NettyClient(host, port);
            this.nameServerClient.connect();
            
            logger.info("BrokerRegistration connected to NameServer: {}", nameServerAddr);
        } catch (Exception e) {
            logger.error("Failed to connect to NameServer: " + nameServerAddr, e);
            throw new RuntimeException("Cannot connect to NameServer", e);
        }
    }
    
    /**
     * 启动注册和心跳任务
     */
    public void start() {
        if (running) {
            logger.warn("BrokerRegistration already running");
            return;
        }
        
        this.scheduledExecutor = Executors.newScheduledThreadPool(2);
        this.running = true;
        
        // 立即执行一次注册
        registerBroker();
        
        // 定期注册任务
        scheduledExecutor.scheduleAtFixedRate(
            this::registerBroker, 
            REGISTER_INTERVAL_MS, 
            REGISTER_INTERVAL_MS, 
            TimeUnit.MILLISECONDS
        );
        
        // 定期心跳任务
        scheduledExecutor.scheduleAtFixedRate(
            this::sendHeartbeat, 
            HEARTBEAT_INTERVAL_MS, 
            HEARTBEAT_INTERVAL_MS, 
            TimeUnit.MILLISECONDS
        );
        
        logger.info("BrokerRegistration started successfully");
    }
    
    /**
     * 停止注册和心跳任务
     */
    public void shutdown() {
        running = false;
        
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdown();
            try {
                if (!scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduledExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduledExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        if (nameServerClient != null) {
            nameServerClient.disconnect();
        }
        
        logger.info("BrokerRegistration shutdown completed");
    }
    
    /**
     * 向NameServer注册Broker
     */
    private void registerBroker() {
        if (!running || nameServerClient == null || !nameServerClient.isConnected()) {
            logger.warn("Cannot register broker: not connected to NameServer");
            return;
        }
        
        try {
            RegisterBrokerRequest request = new RegisterBrokerRequest();
            request.clusterName = this.clusterName;
            request.brokerAddr = this.brokerAddr;
            request.brokerName = this.brokerName;
            request.brokerId = this.brokerId;
            request.epoch = this.currentEpoch;
            request.haServerAddr = this.haServerAddr;
            request.topicConfigWrapper = null; // TODO: 添加Topic配置
            request.filterServerList = null;
            request.compressed = false;

            // Collect system metrics
            java.lang.management.OperatingSystemMXBean osBean =
                    java.lang.management.ManagementFactory.getOperatingSystemMXBean();
            double systemLoad = osBean.getSystemLoadAverage();
            int processors = Runtime.getRuntime().availableProcessors();
            request.cpuUsage = systemLoad > 0 ? systemLoad / processors : 0.0;

            Runtime runtime = Runtime.getRuntime();
            long totalMem = runtime.totalMemory();
            long freeMem = runtime.freeMemory();
            request.memoryUsage = totalMem > 0 ? 1.0 - (double) freeMem / totalMem : 0.0;

            java.io.File storeDir = new java.io.File(System.getProperty("user.home"), "flare-mq-store");
            if (storeDir.exists()) {
                long totalSpace = storeDir.getTotalSpace();
                long usableSpace = storeDir.getUsableSpace();
                request.diskUsage = totalSpace > 0 ? 1.0 - (double) usableSpace / totalSpace : 0.0;
            }

            // 从持久化存储获取真实的消息总数和TPS
            if (messageStore != null) {
                long now = System.currentTimeMillis();
                request.totalMessages = messageStore.getTotalMessageCount();

                if (lastRegisterTimestamp > 0) {
                    double elapsedSec = (now - lastRegisterTimestamp) / 1000.0;
                    long delta = request.totalMessages - lastTotalMessageCount;
                    request.currentTps = elapsedSec > 0 ? Math.max(0, delta) / elapsedSec : 0.0;
                }
                lastTotalMessageCount = request.totalMessages;
                lastRegisterTimestamp = now;
            }

            // 收集Topic维度消息统计
            if (messageStore != null) {
                java.util.Map<String, long[]> topicCounts = messageStore.getTopicMessageCounts();
                List<TopicStatEntry> topicEntries = new ArrayList<>();
                for (java.util.Map.Entry<String, long[]> e : topicCounts.entrySet()) {
                    TopicStatEntry entry = new TopicStatEntry();
                    entry.topicName = e.getKey();
                    entry.queueCount = e.getValue()[0];
                    entry.messageCount = e.getValue()[1];
                    topicEntries.add(entry);
                }
                request.topicStats = topicEntries;
            }

            // 上报消费组统计
            if (offsetManager != null && messageStore != null) {
                reportConsumerGroupStats();
            }

            String requestJson = JsonUtils.toJson(request);
            ProtocolMessage protocolMessage = new ProtocolMessage(
                MessageType.REGISTER_BROKER_REQUEST,
                requestJson.getBytes(StandardCharsets.UTF_8)
            );
            
            ProtocolMessage response = nameServerClient.sendSync(protocolMessage, 5000);
            if (response != null && response.getStatus().getCode() == 0) {
                logger.debug("Successfully registered broker to NameServer: brokerName={}", brokerName);
            } else {
                logger.warn("Failed to register broker to NameServer: brokerName={}, response={}", 
                           brokerName, response != null ? response.getStatus() : "null");
            }
            
        } catch (Exception e) {
            logger.error("Error registering broker to NameServer: brokerName=" + brokerName, e);
        }
    }
    
    /**
     * 发送心跳到NameServer
     */
    private void sendHeartbeat() {
        if (!running || nameServerClient == null || !nameServerClient.isConnected()) {
            onHeartbeatFailure("not connected");
            return;
        }
        try {
            Map<String, Object> hb = new LinkedHashMap<>();
            hb.put("clusterName", clusterName);
            hb.put("brokerName", brokerName);
            hb.put("brokerAddr", brokerAddr);
            hb.put("brokerId", brokerId);
            ProtocolMessage heartbeat = new ProtocolMessage(
                    MessageType.HEARTBEAT_REQUEST,
                    JsonUtils.toJson(hb).getBytes(StandardCharsets.UTF_8));
            ProtocolMessage response = nameServerClient.sendSync(heartbeat, 3000);
            if (response == null || response.getStatus().getCode() != 0) {
                onHeartbeatFailure("bad response");
            } else {
                consecutiveHeartbeatFailures = 0;
            }
        } catch (Exception e) {
            onHeartbeatFailure("exception: " + e.getMessage());
        }
    }

    /**
     * 心跳失败统一处理：所有失败路径（未连接 / 异常 / 响应非法）均计数，连续 3 次触发停写监听
     */
    private void onHeartbeatFailure(String reason) {
        consecutiveHeartbeatFailures++;
        logger.warn("Heartbeat failed to NameServer: brokerName={}, reason={}, consecutiveFailures={}",
                brokerName, reason, consecutiveHeartbeatFailures);
        if (consecutiveHeartbeatFailures >= 3 && heartbeatLossListener != null) {
            heartbeatLossListener.run();
        }
    }

    /**
     * 采集消费组统计并上报到 NameServer
     */
    private void reportConsumerGroupStats() {
        if (offsetManager == null || messageStore == null) return;
        try {
            Map<String, Long> allOffsets = offsetManager.getAllOffsets();
            if (allOffsets.isEmpty()) return;

            // groupName → topic → queueId → consumedOffset
            Map<String, Map<String, Map<Integer, Long>>> grouped = new LinkedHashMap<>();

            for (Map.Entry<String, Long> entry : allOffsets.entrySet()) {
                String key = entry.getKey();  // "group@topic@queueId"
                String[] parts = key.split("@", 3);
                if (parts.length != 3) continue;
                String groupName = parts[0];
                String topic = parts[1];
                int queueId;
                try {
                    queueId = Integer.parseInt(parts[2]);
                } catch (NumberFormatException e) {
                    continue;
                }
                long consumedOffset = entry.getValue();

                grouped.computeIfAbsent(groupName, g -> new LinkedHashMap<>())
                       .computeIfAbsent(topic, t -> new LinkedHashMap<>())
                       .put(queueId, consumedOffset);
            }

            for (Map.Entry<String, Map<String, Map<Integer, Long>>> ge : grouped.entrySet()) {
                String groupName = ge.getKey();
                Map<String, Map<Integer, Long>> topicMap = ge.getValue();

                for (Map.Entry<String, Map<Integer, Long>> te : topicMap.entrySet()) {
                    String topic = te.getKey();
                    Map<Integer, Long> queueMap = te.getValue();

                    List<Map<String, Object>> queueStats = new ArrayList<>();
                    long groupConsumed = 0;
                    for (Map.Entry<Integer, Long> qe : queueMap.entrySet()) {
                        int qid = qe.getKey();
                        long consumed = qe.getValue();
                        long maxOffset = messageStore.getMaxOffset(topic, qid);
                        groupConsumed += consumed;

                        Map<String, Object> qs = new LinkedHashMap<>();
                        qs.put("queueId", qid);
                        qs.put("maxOffset", maxOffset);
                        qs.put("consumedOffset", consumed);
                        queueStats.add(qs);
                    }
                    String gtKey = groupName + "@" + topic;
                    long prevConsumed = lastConsumedByGroup.getOrDefault(gtKey, 0L);

                    Map<String, Object> report = new LinkedHashMap<>();
                    report.put("brokerName", this.brokerName);
                    report.put("groupName", groupName);
                    report.put("topic", topic);
                    report.put("consumeTps", calcConsumeTps(groupConsumed, prevConsumed));
                    report.put("queueStats", queueStats);

                    lastConsumedByGroup.put(gtKey, groupConsumed);

                    String json = JsonUtils.toJson(report);
                    ProtocolMessage msg = new ProtocolMessage(
                            MessageType.REPORT_CONSUMER_GROUP_STATS_REQUEST,
                            json.getBytes(StandardCharsets.UTF_8));
                    nameServerClient.sendAsync(msg, new ResponseCallback() {
                        @Override
                        public void onSuccess(ProtocolMessage response) {
                            logger.debug("Consumer group stats reported: group={}, topic={}", groupName, topic);
                        }
                        @Override
                        public void onFailure(Throwable cause) {
                            logger.warn("Failed to report consumer group stats: group={}, topic={}, error={}",
                                    groupName, topic, cause.getMessage());
                        }
                    });
                }
            }

            lastConsumeStatsTimestamp = System.currentTimeMillis();

        } catch (Exception e) {
            logger.warn("Failed to report consumer group stats: {}", e.getMessage());
        }
    }

    private double calcConsumeTps(long consumed, long prevConsumed) {
        if (prevConsumed <= 0 || lastConsumeStatsTimestamp <= 0) return 0.0;
        double elapsed = (System.currentTimeMillis() - lastConsumeStatsTimestamp) / 1000.0;
        long delta = consumed - prevConsumed;
        return elapsed > 0 ? Math.max(0, delta) / elapsed : 0.0;
    }

    // ===== DTO Classes =====
    static class RegisterBrokerRequest {
        public String clusterName;
        public String brokerAddr;
        public String brokerName;
        public long brokerId;
        public long epoch;
        public String haServerAddr;
        public Object topicConfigWrapper; // 简化实现
        public List<String> filterServerList;
        public boolean compressed;
        public double cpuUsage;
        public double memoryUsage;
        public double diskUsage;
        public long totalMessages;
        public double currentTps;
        public List<TopicStatEntry> topicStats;
    }

    static class TopicStatEntry {
        public String topicName;
        public long queueCount;
        public long messageCount;
    }
}
