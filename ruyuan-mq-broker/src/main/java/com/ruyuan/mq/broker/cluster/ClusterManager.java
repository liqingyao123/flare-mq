package com.ruyuan.mq.broker.cluster;

import com.ruyuan.mq.protocol.server.NettyServer;
import com.ruyuan.mq.broker.BrokerRequestHandler;
import com.ruyuan.mq.broker.ack.AckManager;
import com.ruyuan.mq.broker.queue.QueueManager;
import com.ruyuan.mq.broker.topic.TopicManager;
import com.ruyuan.mq.broker.registry.BrokerRegistration;
import com.ruyuan.mq.broker.offset.ConsumerOffsetManager;
import com.ruyuan.mq.store.DefaultMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;

/**
 * 集群管理器 - 负责Broker集群的管理和协调
 * 
 * @author RuYuan MQ Team
 */
public class ClusterManager {
    
    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);
    
    private final String clusterName;
    private final String brokerName;
    final ClusterConfig clusterConfig;
    
    // 集群节点信息
    private final ConcurrentHashMap<String, BrokerNode> clusterNodes;
    
    // 主从复制管理器
    private final ReplicationManager replicationManager;
    
    // 故障转移管理器
    private final FailoverManager failoverManager;
    
    // 负载均衡器
    private final LoadBalancer loadBalancer;

    // 网络服务器
    private final NettyServer nettyServer;

    // 定时任务执行器
    private final ScheduledExecutorService scheduledExecutor;

    // Topic管理器
    private TopicManager topicManager;

    // Offset管理器
    private ConsumerOffsetManager offsetManager;

    // Ack管理器
    private AckManager ackManager;

    // Broker注册管理器
    private BrokerRegistration brokerRegistration;

    // 集群状态
    private volatile ClusterState clusterState;
    private final AtomicLong stateVersion;

    // 运行状态
    private volatile boolean running;
    
    public ClusterManager(String clusterName, String brokerName, ClusterConfig config) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.clusterConfig = config;
        this.clusterNodes = new ConcurrentHashMap<>();
        this.replicationManager = new ReplicationManager(this);
        this.failoverManager = new FailoverManager(this);
        this.loadBalancer = new LoadBalancer(this);
        this.scheduledExecutor = Executors.newScheduledThreadPool(4, r -> {
            Thread t = new Thread(r, "ClusterManager-" + clusterName + "-" + System.currentTimeMillis());
            t.setDaemon(true);
            return t;
        });

        // 从brokerAddr中提取端口
        int port = extractPortFromAddress(config.getBrokerAddr());
        // 准备真实服务依赖并注入请求处理器
        TopicManager topicManager = new TopicManager();
        QueueManager queueManager = new QueueManager();
        DefaultMessageStore messageStore = new DefaultMessageStore(null);
        messageStore.start();

        // 创建Offset管理器并注入BrokerRequestHandler
        String persistDir = System.getProperty("user.dir") + "/data";
        new java.io.File(persistDir).mkdirs();
        this.offsetManager = new ConsumerOffsetManager(persistDir);

        // 创建AckManager并启动
        this.ackManager = new AckManager();
        this.ackManager.start();

        // 设置重试消息处理器
        this.ackManager.setRetryHandler((retryRecord, ackRecord) -> {
            try {
                com.ruyuan.mq.store.Message message = messageStore.getMessage(
                        ackRecord.getMessageOffset(), (int) ackRecord.getStoreSize());
                if (message != null) {
                    messageStore.putMessage(message);
                    logger.info("Retry message re-delivered: messageId={}, retryCount={}",
                            retryRecord.getMessageId(), retryRecord.getRetryCount());
                } else {
                    logger.warn("Retry message not found in CommitLog: messageId={}, offset={}",
                            retryRecord.getMessageId(), ackRecord.getMessageOffset());
                }
            } catch (Exception e) {
                logger.error("Retry re-delivery failed: messageId="
                        + retryRecord.getMessageId(), e);
            }
        });

        this.nettyServer = new NettyServer(port,
                new BrokerRequestHandler(topicManager, queueManager, messageStore, offsetManager, this.ackManager));

        // 保存TopicManager引用以便后续初始化
        this.topicManager = topicManager;

        this.clusterState = ClusterState.INITIALIZING;
        this.stateVersion = new AtomicLong(0);
        this.running = false;
        
        logger.info("ClusterManager initialized: cluster={}, broker={}", clusterName, brokerName);
    }
    
    /**
     * 启动集群管理器
     */
    public void start() {
        if (running) {
            logger.warn("ClusterManager already running");
            return;
        }
        
        try {
            // 这里需要NameServer地址，暂时使用默认值
            String nameServerAddr = "localhost:9876"; // TODO: 从配置中获取

            // 初始化TopicManager（连接到NameServer）
            if (topicManager != null) {
                topicManager.initialize(nameServerAddr, brokerName, clusterConfig.getBrokerAddr());

                // 初始化默认Topic
                topicManager.initializeDefaultTopics();
            }

            // 初始化并启动Broker注册管理器
            brokerRegistration = new BrokerRegistration(
                clusterName,
                brokerName,
                clusterConfig.getBrokerAddr(),
                clusterConfig.getBrokerId()
            );
            brokerRegistration.initialize(nameServerAddr);
            brokerRegistration.start();

            // 启动Netty服务器
            nettyServer.start();
            logger.info("Broker Netty server started on port: {}", nettyServer.getPort());

            // 启动复制管理器
            replicationManager.start();

            // 启动故障转移管理器
            failoverManager.start();

            // 启动负载均衡器
            loadBalancer.start();

            // 启动定时任务
            startScheduledTasks();

            // 加入集群
            joinCluster();

            running = true;
            clusterState = ClusterState.RUNNING;
            stateVersion.incrementAndGet();

            logger.info("ClusterManager started successfully: cluster={}, broker={}", clusterName, brokerName);
            
        } catch (Exception e) {
            logger.error("Failed to start ClusterManager", e);
            throw new RuntimeException("Failed to start ClusterManager", e);
        }
    }
    
    /**
     * 关闭集群管理器
     */
    public void shutdown() {
        if (!running) {
            return;
        }
        
        logger.info("Shutting down ClusterManager: cluster={}, broker={}", clusterName, brokerName);
        
        try {
            // 离开集群
            leaveCluster();
            
            // 更新状态
            clusterState = ClusterState.SHUTTING_DOWN;
            stateVersion.incrementAndGet();
            running = false;
            
            // 关闭定时任务
            scheduledExecutor.shutdown();
            if (!scheduledExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduledExecutor.shutdownNow();
            }
            
            // 关闭各个组件
            loadBalancer.shutdown();
            failoverManager.shutdown();
            replicationManager.shutdown();

            // 关闭TopicManager
            if (topicManager != null) {
                topicManager.shutdown();
            }

            // 关闭offset管理器
            if (offsetManager != null) {
                offsetManager.shutdown();
            }

            // 关闭Ack管理器
            if (ackManager != null) {
                ackManager.shutdown();
            }

            // 关闭Broker注册管理器
            if (brokerRegistration != null) {
                brokerRegistration.shutdown();
            }

            // 关闭Netty服务器
            nettyServer.shutdown();
            logger.info("Broker Netty server shutdown completed");
            
            clusterState = ClusterState.STOPPED;
            stateVersion.incrementAndGet();
            
            logger.info("ClusterManager shutdown completed");
            
        } catch (Exception e) {
            logger.error("Error during ClusterManager shutdown", e);
        }
    }
    
    /**
     * 加入集群
     */
    private void joinCluster() {
        logger.info("Joining cluster: {}", clusterName);
        
        // 创建当前节点信息
        BrokerNode currentNode = new BrokerNode(
            brokerName,
            clusterConfig.getBrokerAddr(),
            clusterConfig.getBrokerId(),
            BrokerRole.SLAVE, // 默认以Slave身份加入
            System.currentTimeMillis()
        );

        // 设置节点状态为运行中
        currentNode.setStatus(BrokerStatus.RUNNING);

        // 添加到集群节点列表
        clusterNodes.put(brokerName, currentNode);
        
        // 如果是第一个节点或配置为Master，则尝试成为Master
        if (clusterNodes.size() == 1 || clusterConfig.isMasterCandidate()) {
            tryBecomeMaster();
        }
        
        logger.info("Successfully joined cluster: {}, role: {}", clusterName, currentNode.getRole());
    }
    
    /**
     * 离开集群
     */
    private void leaveCluster() {
        logger.info("Leaving cluster: {}", clusterName);
        
        BrokerNode currentNode = clusterNodes.get(brokerName);
        if (currentNode != null) {
            // 如果是Master，需要触发选举
            if (currentNode.getRole() == BrokerRole.MASTER) {
                triggerMasterElection();
            }
            
            // 从集群中移除
            clusterNodes.remove(brokerName);
        }
        
        logger.info("Successfully left cluster: {}", clusterName);
    }
    
    /**
     * 尝试成为Master
     */
    private void tryBecomeMaster() {
        BrokerNode currentNode = clusterNodes.get(brokerName);
        if (currentNode == null) {
            return;
        }
        
        // 检查是否已有Master
        boolean hasMaster = clusterNodes.values().stream()
                .anyMatch(node -> node.getRole() == BrokerRole.MASTER);
        
        if (!hasMaster) {
            // 成为Master
            currentNode.setRole(BrokerRole.MASTER);
            currentNode.setLastUpdateTime(System.currentTimeMillis());
            
            logger.info("Became cluster master: cluster={}, broker={}", clusterName, brokerName);
            
            // 通知其他组件
            onBecameMaster();
        }
    }
    
    /**
     * 触发Master选举
     */
    private void triggerMasterElection() {
        logger.info("Triggering master election for cluster: {}", clusterName);
        
        // 简单的选举算法：选择brokerId最小的活跃节点
        BrokerNode newMaster = clusterNodes.values().stream()
                .filter(node -> node.getRole() == BrokerRole.SLAVE)
                .filter(this::isNodeHealthy)
                .min((n1, n2) -> Long.compare(n1.getBrokerId(), n2.getBrokerId()))
                .orElse(null);
        
        if (newMaster != null) {
            newMaster.setRole(BrokerRole.MASTER);
            newMaster.setLastUpdateTime(System.currentTimeMillis());
            
            logger.info("New master elected: cluster={}, broker={}", clusterName, newMaster.getBrokerName());
        } else {
            logger.warn("No suitable candidate for master election in cluster: {}", clusterName);
        }
    }
    
    /**
     * 成为Master后的处理
     */
    private void onBecameMaster() {
        // 启动Master特有的功能
        replicationManager.startAsmaster();
        
        // 更新集群状态
        stateVersion.incrementAndGet();
    }
    
    /**
     * 检查节点是否健康
     */
    private boolean isNodeHealthy(BrokerNode node) {
        long currentTime = System.currentTimeMillis();
        long lastUpdate = node.getLastUpdateTime();
        return (currentTime - lastUpdate) < clusterConfig.getNodeTimeoutMs();
    }
    
    /**
     * 启动定时任务
     */
    private void startScheduledTasks() {
        // 定期检查集群健康状态
        scheduledExecutor.scheduleAtFixedRate(this::checkClusterHealth, 
                                            10, 30, TimeUnit.SECONDS);
        
        // 定期同步集群状态
        scheduledExecutor.scheduleAtFixedRate(this::syncClusterState, 
                                            5, 15, TimeUnit.SECONDS);
        
        // 定期清理过期节点
        scheduledExecutor.scheduleAtFixedRate(this::cleanupExpiredNodes, 
                                            60, 60, TimeUnit.SECONDS);
        
        logger.info("Cluster scheduled tasks started");
    }
    
    /**
     * 检查集群健康状态
     */
    private void checkClusterHealth() {
        try {
            logger.debug("Checking cluster health: {}", clusterName);
            
            int totalNodes = clusterNodes.size();
            int healthyNodes = 0;
            
            for (BrokerNode node : clusterNodes.values()) {
                if (isNodeHealthy(node)) {
                    healthyNodes++;
                }
            }
            
            double healthRatio = totalNodes > 0 ? (double) healthyNodes / totalNodes : 0.0;
            
            if (healthRatio < clusterConfig.getMinHealthRatio()) {
                logger.warn("Cluster health degraded: cluster={}, healthy={}/{}, ratio={:.2f}", 
                           clusterName, healthyNodes, totalNodes, healthRatio);
                
                // 触发故障处理
                failoverManager.handleClusterDegradation(healthRatio);
            }
            
        } catch (Exception e) {
            logger.error("Error checking cluster health", e);
        }
    }
    
    /**
     * 同步集群状态
     */
    private void syncClusterState() {
        try {
            // 更新当前节点的心跳时间
            BrokerNode currentNode = clusterNodes.get(brokerName);
            if (currentNode != null) {
                currentNode.setLastUpdateTime(System.currentTimeMillis());
            }
            
            // 同步到其他节点（简化实现）
            logger.debug("Synced cluster state: cluster={}, nodes={}", clusterName, clusterNodes.size());
            
        } catch (Exception e) {
            logger.error("Error syncing cluster state", e);
        }
    }
    
    /**
     * 清理过期节点
     */
    private void cleanupExpiredNodes() {
        try {
            long currentTime = System.currentTimeMillis();
            int removedCount = 0;
            
            clusterNodes.entrySet().removeIf(entry -> {
                BrokerNode node = entry.getValue();
                boolean expired = (currentTime - node.getLastUpdateTime()) > clusterConfig.getNodeExpireMs();
                
                if (expired && !entry.getKey().equals(brokerName)) {
                    logger.info("Removed expired node: cluster={}, broker={}", clusterName, entry.getKey());
                    return true;
                }
                return false;
            });
            
            if (removedCount > 0) {
                stateVersion.incrementAndGet();
                logger.info("Cleaned up {} expired nodes from cluster: {}", removedCount, clusterName);
            }
            
        } catch (Exception e) {
            logger.error("Error cleaning up expired nodes", e);
        }
    }
    
    // Getters
    public String getClusterName() { return clusterName; }
    public String getBrokerName() { return brokerName; }
    public ClusterState getClusterState() { return clusterState; }
    public long getStateVersion() { return stateVersion.get(); }
    public boolean isRunning() { return running; }
    public Map<String, BrokerNode> getClusterNodes() { return new ConcurrentHashMap<>(clusterNodes); }
    public ReplicationManager getReplicationManager() { return replicationManager; }
    public FailoverManager getFailoverManager() { return failoverManager; }
    public LoadBalancer getLoadBalancer() { return loadBalancer; }
    
    /**
     * 获取当前节点角色
     */
    public BrokerRole getCurrentRole() {
        BrokerNode currentNode = clusterNodes.get(brokerName);
        return currentNode != null ? currentNode.getRole() : BrokerRole.SLAVE;
    }
    
    /**
     * 检查是否为Master
     */
    public boolean isMaster() {
        return getCurrentRole() == BrokerRole.MASTER;
    }
    
    /**
     * 获取Master节点
     */
    public BrokerNode getMasterNode() {
        return clusterNodes.values().stream()
                .filter(node -> node.getRole() == BrokerRole.MASTER)
                .findFirst()
                .orElse(null);
    }
    
    /**
     * 获取集群统计信息
     */
    public ClusterStatistics getStatistics() {
        int totalNodes = clusterNodes.size();
        int healthyNodes = (int) clusterNodes.values().stream()
                .filter(this::isNodeHealthy)
                .count();
        
        BrokerNode master = getMasterNode();
        String masterBroker = master != null ? master.getBrokerName() : "none";
        
        return new ClusterStatistics(clusterName, totalNodes, healthyNodes,
                                   masterBroker, stateVersion.get(), clusterState);
    }

    /**
     * 从地址字符串中提取端口号
     */
    private int extractPortFromAddress(String address) {
        if (address == null || address.trim().isEmpty()) {
            return 10911; // 默认端口
        }

        String[] parts = address.split(":");
        if (parts.length >= 2) {
            try {
                return Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
                logger.warn("Invalid port in address: {}, using default port 10911", address);
                return 10911;
            }
        }

        logger.warn("No port found in address: {}, using default port 10911", address);
        return 10911;
    }

}
