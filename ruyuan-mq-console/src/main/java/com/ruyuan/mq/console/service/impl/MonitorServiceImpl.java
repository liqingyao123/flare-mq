package com.ruyuan.mq.console.service.impl;

import com.ruyuan.mq.common.util.JsonUtils;
import com.ruyuan.mq.console.model.*;
import com.ruyuan.mq.console.service.MonitorService;
import com.ruyuan.mq.protocol.MessageType;
import com.ruyuan.mq.protocol.ProtocolMessage;
import com.ruyuan.mq.protocol.client.NettyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 监控服务实现 — 从 NameServer 拉取真实集群数据
 *
 * @author RuYuan
 * @version 2.0.0
 */
public class MonitorServiceImpl implements MonitorService {

    private static final Logger logger = LoggerFactory.getLogger(MonitorServiceImpl.class);

    /** 判定 Broker 健康的时间阈值（ms）：最近 30 秒内有上报即为健康 */
    private static final long HEALTHY_THRESHOLD_MS = 30_000;

    private volatile boolean running = false;
    private long startTimeMillis;

    // --- NameServer 连接 ---
    private String nameServerHost;
    private int nameServerPort;
    private NettyClient nettyClient;
    private volatile boolean connected = false;

    // --- 从 NameServer 拉取并缓存的原始数据 ---
    private final Object cacheLock = new Object();
    private List<Map<String, Object>> cachedBrokers = Collections.emptyList();
    private int cachedTopicCount = 0;
    private int cachedQueueCount = 0;
    private int cachedConsumerGroupCount = 0;

    // --- 对外暴露的模型对象 ---
    private final SystemOverview systemOverview = new SystemOverview();
    private final PerformanceMetrics performanceMetrics = new PerformanceMetrics();
    private final ClusterHealth clusterHealth = new ClusterHealth();
    private final ConcurrentLinkedQueue<SystemAlert> systemAlerts = new ConcurrentLinkedQueue<>();
    private final TpsStatistics tpsStatistics = new TpsStatistics();
    private final StorageStatistics storageStatistics = new StorageStatistics();

    // ======================== 配置 ========================

    public void setNameServerAddr(String host, int port) {
        this.nameServerHost = host;
        this.nameServerPort = port;
    }

    // ======================== 生命周期 ========================

    @Override
    public void start() {
        if (running) {
            logger.warn("MonitorService already running");
            return;
        }

        if (nameServerHost == null || nameServerHost.isEmpty()) {
            logger.error("NameServer address not configured, cannot start MonitorService");
            return;
        }

        logger.info("Starting MonitorService, connecting to NameServer {}:{}", nameServerHost, nameServerPort);

        this.startTimeMillis = System.currentTimeMillis();
        running = true;

        connectToNameServer();

        // 首次拉取
        refreshSystemMetrics();

        logger.info("MonitorService started successfully");
    }

    private void connectToNameServer() {
        try {
            if (nettyClient != null) {
                nettyClient.shutdown();
            }
            nettyClient = new NettyClient(nameServerHost, nameServerPort);
            nettyClient.connect();
            connected = true;
            logger.info("Connected to NameServer {}:{}", nameServerHost, nameServerPort);
        } catch (Exception e) {
            connected = false;
            logger.warn("Failed to connect to NameServer {}:{}, will retry on next refresh. Cause: {}",
                    nameServerHost, nameServerPort, e.getMessage());
        }
    }

    @Override
    public void shutdown() {
        if (!running) {
            return;
        }

        logger.info("Shutting down MonitorService...");
        running = false;

        try {
            if (nettyClient != null) {
                nettyClient.shutdown();
            }
        } catch (Exception e) {
            logger.warn("Error shutting down NettyClient: {}", e.getMessage());
        }

        logger.info("MonitorService shutdown completed");
    }

    // ======================== 数据拉取 ========================

    @Override
    public void refreshSystemMetrics() {
        if (!running) {
            return;
        }

        logger.debug("Refreshing system metrics from NameServer...");

        // 从 NameServer 拉取最新数据
        try {
            fetchClusterStats();
        } catch (Exception e) {
            logger.warn("Failed to fetch metrics from NameServer: {}", e.getMessage());
            // NameServer 不可达时不清空已有缓存，继续使用旧数据
        }

        // 基于缓存数据更新各个模型
        try {
            updateSystemOverview();
            updateClusterHealth();
            updatePerformanceMetrics();
            updateTpsStatistics();
            updateStorageStatistics();
            checkSystemAlerts();
        } catch (Exception e) {
            logger.error("Failed to update derived metrics: {}", e.getMessage(), e);
        }

        logger.debug("System metrics refreshed: brokers={}, topics={}, queues={}, consumerGroups={}",
                cachedBrokers.size(), cachedTopicCount, cachedQueueCount, cachedConsumerGroupCount);
    }

    /**
     * 通过 NettyClient 向 NameServer 发送 GET_CLUSTER_STATS_REQUEST 并解析 JSON 响应。
     */
    private void fetchClusterStats() throws Exception {
        // 确保连接有效
        if (!connected || nettyClient == null || !nettyClient.isConnected()) {
            logger.info("Reconnecting to NameServer...");
            connectToNameServer();
            if (!connected) {
                throw new RuntimeException("NameServer unreachable at " + nameServerHost + ":" + nameServerPort);
            }
        }

        ProtocolMessage request = new ProtocolMessage(
                MessageType.GET_CLUSTER_STATS_REQUEST, null);
        ProtocolMessage response = nettyClient.sendSync(request, 5000);

        if (response == null || response.getBody() == null) {
            throw new RuntimeException("Empty response body from NameServer");
        }

        String json = new String(response.getBody(), StandardCharsets.UTF_8);
        @SuppressWarnings("unchecked")
        Map<String, Object> data = JsonUtils.fromJson(json, Map.class);

        if (data == null) {
            throw new RuntimeException("Failed to parse NameServer JSON response");
        }

        // 原子更新缓存
        synchronized (cacheLock) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> brokerList = (List<Map<String, Object>>) data.get("brokers");
            cachedBrokers = brokerList != null ? new ArrayList<>(brokerList) : Collections.emptyList();
            cachedTopicCount = data.get("topicCount") instanceof Number
                    ? ((Number) data.get("topicCount")).intValue() : 0;
            cachedQueueCount = data.get("queueCount") instanceof Number
                    ? ((Number) data.get("queueCount")).intValue() : 0;
            cachedConsumerGroupCount = data.get("consumerGroupCount") instanceof Number
                    ? ((Number) data.get("consumerGroupCount")).intValue() : 0;
        }
    }

    // ======================== 模型更新 ========================

    private void updateSystemOverview() {
        List<Map<String, Object>> brokers;
        int topicCount, queueCount;
        synchronized (cacheLock) {
            brokers = this.cachedBrokers;
            topicCount = this.cachedTopicCount;
            queueCount = this.cachedQueueCount;
        }

        int totalBrokers = brokers.size();
        int healthyBrokers = 0;
        double cpuSum = 0, memSum = 0, diskSum = 0;
        long totalMessages = 0;
        double totalTps = 0;
        long now = System.currentTimeMillis();

        for (Map<String, Object> b : brokers) {
            long lastUpdate = getLong(b, "lastUpdateTimestamp");
            if (lastUpdate > 0 && (now - lastUpdate) < HEALTHY_THRESHOLD_MS) {
                healthyBrokers++;
            }
            cpuSum += getDouble(b, "cpuUsage");
            memSum += getDouble(b, "memoryUsage");
            diskSum += getDouble(b, "diskUsage");
            totalMessages += getLong(b, "totalMessages");
            totalTps += getDouble(b, "currentTps");
        }

        long uptime = System.currentTimeMillis() - startTimeMillis;

        systemOverview.setUptime(uptime);
        systemOverview.setTotalBrokers(totalBrokers);
        systemOverview.setHealthyBrokers(healthyBrokers);
        systemOverview.setTotalTopics(topicCount);
        systemOverview.setTotalQueues(queueCount);
        systemOverview.setTotalMessages(totalMessages);
        systemOverview.setCurrentTps(totalTps);

        if (totalBrokers > 0) {
            systemOverview.setCpuUsage(cpuSum / totalBrokers);
            systemOverview.setMemoryUsage(memSum / totalBrokers);
            systemOverview.setDiskUsage(diskSum / totalBrokers);
        } else {
            systemOverview.setCpuUsage(0);
            systemOverview.setMemoryUsage(0);
            systemOverview.setDiskUsage(0);
        }

        // 健康状态: >=80% GREEN, >=50% WARNING, else CRITICAL, 0 broker → UNKNOWN
        if (totalBrokers == 0) {
            systemOverview.setHealthStatus("UNKNOWN");
        } else {
            double healthRatio = (double) healthyBrokers / totalBrokers;
            if (healthRatio >= 0.8) {
                systemOverview.setHealthStatus("HEALTHY");
            } else if (healthRatio >= 0.5) {
                systemOverview.setHealthStatus("WARNING");
            } else {
                systemOverview.setHealthStatus("CRITICAL");
            }
        }
    }

    private void updateClusterHealth() {
        List<Map<String, Object>> brokers;
        synchronized (cacheLock) {
            brokers = this.cachedBrokers;
        }

        int totalBrokers = brokers.size();
        int healthyBrokers = 0;
        long now = System.currentTimeMillis();

        for (Map<String, Object> b : brokers) {
            long lastUpdate = getLong(b, "lastUpdateTimestamp");
            if (lastUpdate > 0 && (now - lastUpdate) < HEALTHY_THRESHOLD_MS) {
                healthyBrokers++;
            }
        }

        // 找 Master
        String masterName = null;
        for (Map<String, Object> b : brokers) {
            if ("Master".equals(b.get("role"))) {
                masterName = (String) b.get("brokerName");
                break;
            }
        }

        clusterHealth.setTotalNodes(totalBrokers);
        clusterHealth.setHealthyNodes(healthyBrokers);
        clusterHealth.setHealthRatio(totalBrokers == 0 ? 0.0 : (double) healthyBrokers / totalBrokers);
        clusterHealth.setMasterBroker(masterName != null ? masterName : "none");
        clusterHealth.setActiveConnections(0);
        clusterHealth.setLastCheckTime(LocalDateTime.now());

        if (totalBrokers == 0) {
            clusterHealth.setOverallStatus("UNKNOWN");
        } else {
            double ratio = clusterHealth.getHealthRatio();
            if (ratio >= 0.8) {
                clusterHealth.setOverallStatus("HEALTHY");
            } else if (ratio >= 0.5) {
                clusterHealth.setOverallStatus("WARNING");
            } else {
                clusterHealth.setOverallStatus("CRITICAL");
            }
        }
    }

    private void updatePerformanceMetrics() {
        // 暂不在此次范围 — 保留默认值
    }

    private void updateTpsStatistics() {
        tpsStatistics.addDataPoint(systemOverview.getCurrentTps());
    }

    private void updateStorageStatistics() {
        // 暂不在此次范围 — 保留默认值
    }

    private void checkSystemAlerts() {
        // 清理旧告警
        while (systemAlerts.size() > 10) {
            systemAlerts.poll();
        }

        List<Map<String, Object>> brokers;
        synchronized (cacheLock) {
            brokers = this.cachedBrokers;
        }

        long now = System.currentTimeMillis();

        for (Map<String, Object> b : brokers) {
            long lastUpdate = getLong(b, "lastUpdateTimestamp");
            String brokerName = (String) b.getOrDefault("brokerName", "unknown");

            if (lastUpdate > 0 && (now - lastUpdate) >= HEALTHY_THRESHOLD_MS) {
                systemAlerts.offer(new SystemAlert("WARNING",
                        "Broker Unhealthy: " + brokerName,
                        "Broker " + brokerName + " has not reported for over 30 seconds",
                        "MonitorService"));
            }

            double memUsage = getDouble(b, "memoryUsage");
            if (memUsage > 0.9) {
                systemAlerts.offer(new SystemAlert("WARNING",
                        "High Memory Usage: " + brokerName,
                        "Broker " + brokerName + " memory usage at "
                                + String.format("%.1f%%", memUsage * 100),
                        "MonitorService"));
            }
        }
    }

    // ======================== MonitorService 接口方法 ========================

    @Override
    public SystemOverview getSystemOverview() {
        return systemOverview;
    }

    @Override
    public List<BrokerStatus> getBrokerStatusList() {
        List<Map<String, Object>> brokers;
        synchronized (cacheLock) {
            brokers = this.cachedBrokers;
        }

        List<BrokerStatus> result = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (Map<String, Object> b : brokers) {
            BrokerStatus s = new BrokerStatus();
            s.setBrokerName(b.get("brokerName") != null ? (String) b.get("brokerName") : "");
            s.setBrokerAddr(b.get("brokerAddr") != null ? (String) b.get("brokerAddr") : "");
            s.setClusterName(b.get("clusterName") != null ? (String) b.get("clusterName") : "");
            s.setRole(b.get("role") != null ? (String) b.get("role") : "");
            s.setStatus("RUNNING");
            s.setCpuUsage(getDouble(b, "cpuUsage"));
            s.setMemoryUsage(getDouble(b, "memoryUsage"));
            s.setDiskUsage(getDouble(b, "diskUsage"));
            s.setTotalMessages(getLong(b, "totalMessages"));
            s.setCurrentTps(getDouble(b, "currentTps"));

            long lastUpdate = getLong(b, "lastUpdateTimestamp");
            s.setHealthy(lastUpdate > 0 && (now - lastUpdate) < HEALTHY_THRESHOLD_MS);

            result.add(s);
        }

        return result;
    }

    @Override
    public List<TopicStats> getTopicStatsList() {
        int topicCount, queueCount;
        synchronized (cacheLock) {
            topicCount = this.cachedTopicCount;
            queueCount = this.cachedQueueCount;
        }

        List<TopicStats> result = new ArrayList<>();
        if (topicCount > 0) {
            TopicStats stats = new TopicStats("cluster-topics", queueCount);
            stats.setTotalMessages(0);
            stats.setCurrentTps(systemOverview.getCurrentTps());
            result.add(stats);
        }
        return result;
    }

    @Override
    public List<ConsumerGroupStatus> getConsumerGroupStatusList() {
        // 暂不在此次范围
        return new ArrayList<>();
    }

    @Override
    public PerformanceMetrics getPerformanceMetrics() {
        return performanceMetrics;
    }

    @Override
    public ClusterHealth getClusterHealth() {
        return clusterHealth;
    }

    @Override
    public List<SystemAlert> getSystemAlerts() {
        return new ArrayList<>(systemAlerts);
    }

    @Override
    public TpsStatistics getTpsStatistics() {
        return tpsStatistics;
    }

    @Override
    public StorageStatistics getStorageStatistics() {
        return storageStatistics;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    // ======================== 辅助方法 ========================

    private static double getDouble(Map<String, Object> map, String key) {
        Object val = map.get(key);
        return val instanceof Number ? ((Number) val).doubleValue() : 0.0;
    }

    private static long getLong(Map<String, Object> map, String key) {
        Object val = map.get(key);
        return val instanceof Number ? ((Number) val).longValue() : 0L;
    }
}
