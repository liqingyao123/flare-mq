package com.ruyuan.mq.broker.registry;

import com.ruyuan.mq.protocol.client.NettyClient;
import com.ruyuan.mq.protocol.ProtocolMessage;
import com.ruyuan.mq.protocol.MessageType;
import com.ruyuan.mq.common.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
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
    
    public BrokerRegistration(String clusterName, String brokerName, String brokerAddr, long brokerId) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.brokerAddr = brokerAddr;
        this.brokerId = brokerId;
        this.haServerAddr = brokerAddr.replace("10911", "10912"); // 简化的HA地址
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

            java.io.File storeDir = new java.io.File(System.getProperty("user.home"), "ruyuan-mq-store");
            if (storeDir.exists()) {
                long totalSpace = storeDir.getTotalSpace();
                long usableSpace = storeDir.getUsableSpace();
                request.diskUsage = totalSpace > 0 ? 1.0 - (double) usableSpace / totalSpace : 0.0;
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
            return;
        }
        
        try {
            ProtocolMessage heartbeat = ProtocolMessage.createHeartbeatRequest();
            ProtocolMessage response = nameServerClient.sendSync(heartbeat, 3000);
            
            if (response == null || response.getStatus().getCode() != 0) {
                logger.warn("Heartbeat failed to NameServer: brokerName={}", brokerName);
            }
            
        } catch (Exception e) {
            logger.warn("Error sending heartbeat to NameServer: brokerName=" + brokerName, e);
        }
    }
    
    // ===== DTO Classes =====
    static class RegisterBrokerRequest {
        public String clusterName;
        public String brokerAddr;
        public String brokerName;
        public long brokerId;
        public String haServerAddr;
        public Object topicConfigWrapper; // 简化实现
        public List<String> filterServerList;
        public boolean compressed;
        public double cpuUsage;
        public double memoryUsage;
        public double diskUsage;
        public long totalMessages;
        public double currentTps;
    }
}
