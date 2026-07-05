package com.flare.mq.broker.cluster;

import com.flare.mq.store.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 数据复制管理器 - 负责Master-Slave数据同步
 * 
 * @author FlareMQ Team
 */
public class ReplicationManager {
    
    private static final Logger logger = LoggerFactory.getLogger(ReplicationManager.class);
    
    private final ClusterManager clusterManager;
    private final ExecutorService replicationExecutor;
    private final ScheduledExecutorService scheduledExecutor;
    
    // 复制队列
    private final BlockingQueue<ReplicationTask> replicationQueue;
    
    // 复制状态
    private volatile boolean running;
    private volatile boolean isMaster;
    
    // 统计信息
    private final AtomicLong replicatedMessages;
    private final AtomicLong replicationFailures;
    private final AtomicLong totalReplicationTime;
    
    // 复制配置
    private final ClusterConfig config;
    
    public ReplicationManager(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
        this.config = clusterManager.clusterConfig;
        this.replicationExecutor = Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "ReplicationManager-" + System.currentTimeMillis());
            t.setDaemon(true);
            return t;
        });
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ReplicationManager-Scheduled");
            t.setDaemon(true);
            return t;
        });
        this.replicationQueue = new LinkedBlockingQueue<>(10000);
        this.running = false;
        this.isMaster = false;
        this.replicatedMessages = new AtomicLong(0);
        this.replicationFailures = new AtomicLong(0);
        this.totalReplicationTime = new AtomicLong(0);
        
        logger.info("ReplicationManager initialized");
    }
    
    /**
     * 启动复制管理器
     */
    public void start() {
        if (running) {
            logger.warn("ReplicationManager already running");
            return;
        }
        
        running = true;
        
        // 启动复制工作线程
        for (int i = 0; i < 4; i++) {
            replicationExecutor.submit(this::replicationWorker);
        }
        
        // 启动定期同步任务
        scheduledExecutor.scheduleAtFixedRate(this::periodicSync, 
                                            10, 30, TimeUnit.SECONDS);
        
        logger.info("ReplicationManager started");
    }
    
    /**
     * 以Master身份启动
     */
    public void startAsmaster() {
        this.isMaster = true;
        logger.info("ReplicationManager started as Master");
    }
    
    /**
     * 关闭复制管理器
     */
    public void shutdown() {
        if (!running) {
            return;
        }
        
        logger.info("Shutting down ReplicationManager...");
        running = false;
        
        // 关闭线程池
        replicationExecutor.shutdown();
        scheduledExecutor.shutdown();
        
        try {
            if (!replicationExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                replicationExecutor.shutdownNow();
            }
            if (!scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduledExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            replicationExecutor.shutdownNow();
            scheduledExecutor.shutdownNow();
        }
        
        // 清空复制队列
        replicationQueue.clear();
        
        logger.info("ReplicationManager shutdown completed");
    }
    
    /**
     * 复制消息到Slave节点
     */
    public CompletableFuture<Boolean> replicateMessage(Message message) {
        if (!running || !isMaster || !config.isEnableReplication()) {
            return CompletableFuture.completedFuture(true);
        }
        
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        ReplicationTask task = new SingleReplicationTask(message, future);
        
        if (!replicationQueue.offer(task)) {
            logger.warn("Replication queue is full, dropping message: {}", message.getKeys());
            future.complete(false);
            replicationFailures.incrementAndGet();
        }
        
        return future;
    }
    
    /**
     * 批量复制消息
     */
    public CompletableFuture<Boolean> replicateMessages(List<Message> messages) {
        if (!running || !isMaster || !config.isEnableReplication()) {
            return CompletableFuture.completedFuture(true);
        }
        
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        BatchReplicationTask task = new BatchReplicationTask(messages, future);
        
        if (!replicationQueue.offer(task)) {
            logger.warn("Replication queue is full, dropping {} messages", messages.size());
            future.complete(false);
            replicationFailures.incrementAndGet();
        }
        
        return future;
    }
    
    /**
     * 复制工作线程
     */
    private void replicationWorker() {
        logger.info("Replication worker started");
        
        while (running) {
            try {
                ReplicationTask task = replicationQueue.poll(1, TimeUnit.SECONDS);
                if (task != null) {
                    processReplicationTask(task);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in replication worker", e);
            }
        }
        
        logger.info("Replication worker stopped");
    }
    
    /**
     * 处理复制任务
     */
    private void processReplicationTask(ReplicationTask task) {
        long startTime = System.currentTimeMillis();
        
        try {
            // 获取可用的Slave节点
            List<BrokerNode> slaveNodes = getAvailableSlaveNodes();
            
            if (slaveNodes.isEmpty()) {
                logger.debug("No available slave nodes for replication");
                task.complete(true); // 没有Slave节点时认为复制成功
                return;
            }
            
            // 执行复制
            boolean success = executeReplication(task, slaveNodes);
            task.complete(success);
            
            if (success) {
                replicatedMessages.incrementAndGet();
            } else {
                replicationFailures.incrementAndGet();
            }
            
        } catch (Exception e) {
            logger.error("Error processing replication task", e);
            task.complete(false);
            replicationFailures.incrementAndGet();
        } finally {
            long processingTime = System.currentTimeMillis() - startTime;
            totalReplicationTime.addAndGet(processingTime);
        }
    }
    
    /**
     * 执行复制操作
     */
    private boolean executeReplication(ReplicationTask task, List<BrokerNode> slaveNodes) {
        int requiredReplicas = Math.min(config.getReplicationFactor() - 1, slaveNodes.size());
        if (requiredReplicas <= 0) {
            return true;
        }
        
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        
        // 选择前N个Slave节点进行复制
        for (int i = 0; i < requiredReplicas; i++) {
            BrokerNode slaveNode = slaveNodes.get(i);
            CompletableFuture<Boolean> future = replicateToSlave(task, slaveNode);
            futures.add(future);
        }
        
        // 等待所有复制完成
        try {
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0]));
            
            allFutures.get(config.getReplicationTimeoutMs(), TimeUnit.MILLISECONDS);
            
            // 检查成功的复制数量
            long successCount = futures.stream()
                    .mapToLong(f -> {
                        try {
                            return f.get() ? 1 : 0;
                        } catch (Exception e) {
                            return 0;
                        }
                    })
                    .sum();
            
            // 至少一半的复制成功才认为整体成功
            return successCount >= (requiredReplicas + 1) / 2;
            
        } catch (TimeoutException e) {
            logger.warn("Replication timeout for task: {}", task);
            return false;
        } catch (Exception e) {
            logger.error("Error waiting for replication completion", e);
            return false;
        }
    }
    
    /**
     * 复制到指定Slave节点
     */
    private CompletableFuture<Boolean> replicateToSlave(ReplicationTask task, BrokerNode slaveNode) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // 这里简化实现，实际应该通过网络发送到Slave节点
                logger.debug("Replicating to slave: {} -> {}", task, slaveNode.getBrokerName());
                
                // 模拟网络延迟
                Thread.sleep(slaveNode.getNetworkLatency());
                
                // 模拟复制成功率（95%）
                return Math.random() > 0.05;
                
            } catch (Exception e) {
                logger.error("Error replicating to slave: {}", slaveNode.getBrokerName(), e);
                return false;
            }
        }, replicationExecutor);
    }
    
    /**
     * 获取可用的Slave节点
     */
    private List<BrokerNode> getAvailableSlaveNodes() {
        return clusterManager.getClusterNodes().values().stream()
                .filter(node -> node.isSlave())
                .filter(BrokerNode::isAvailable)
                .filter(node -> node.isActive(config.getNodeTimeoutMs()))
                .sorted((n1, n2) -> Double.compare(n1.calculateLoadScore(), n2.calculateLoadScore()))
                .collect(Collectors.toList());
    }
    
    /**
     * 定期同步
     */
    private void periodicSync() {
        if (!running || !isMaster) {
            return;
        }
        
        try {
            logger.debug("Performing periodic sync...");
            
            // 检查复制队列大小
            int queueSize = replicationQueue.size();
            if (queueSize > 5000) {
                logger.warn("Replication queue size is high: {}", queueSize);
            }
            
            // 检查Slave节点状态
            List<BrokerNode> slaveNodes = getAvailableSlaveNodes();
            logger.debug("Available slave nodes: {}", slaveNodes.size());
            
        } catch (Exception e) {
            logger.error("Error in periodic sync", e);
        }
    }
    
    /**
     * 获取复制统计信息
     */
    public ReplicationStatistics getStatistics() {
        long replicated = replicatedMessages.get();
        long failures = replicationFailures.get();
        long totalTime = totalReplicationTime.get();
        
        double successRate = replicated + failures > 0 ? 
                (double) replicated / (replicated + failures) : 1.0;
        
        double avgTime = replicated > 0 ? (double) totalTime / replicated : 0.0;
        
        return new ReplicationStatistics(
            replicated, failures, successRate, avgTime, 
            replicationQueue.size(), getAvailableSlaveNodes().size()
        );
    }
    
    // Getters
    public boolean isRunning() { return running; }
    public boolean isMaster() { return isMaster; }
    
    /**
     * 复制任务基类
     */
    private static abstract class ReplicationTask {
        protected final CompletableFuture<Boolean> future;
        protected final long createTime;
        
        public ReplicationTask(CompletableFuture<Boolean> future) {
            this.future = future;
            this.createTime = System.currentTimeMillis();
        }
        
        public void complete(boolean success) {
            future.complete(success);
        }
        
        public abstract int getMessageCount();
    }
    
    /**
     * 单消息复制任务
     */
    private static class SingleReplicationTask extends ReplicationTask {
        private final Message message;
        
        public SingleReplicationTask(Message message, CompletableFuture<Boolean> future) {
            super(future);
            this.message = message;
        }
        
        public Message getMessage() { return message; }
        
        @Override
        public int getMessageCount() { return 1; }
        
        @Override
        public String toString() {
            return "SingleReplicationTask{message=" + message.getKeys() + "}";
        }
    }
    
    /**
     * 批量复制任务
     */
    private static class BatchReplicationTask extends ReplicationTask {
        private final List<Message> messages;
        
        public BatchReplicationTask(List<Message> messages, CompletableFuture<Boolean> future) {
            super(future);
            this.messages = messages;
        }
        
        public List<Message> getMessages() { return messages; }
        
        @Override
        public int getMessageCount() { return messages.size(); }
        
        @Override
        public String toString() {
            return "BatchReplicationTask{messageCount=" + messages.size() + "}";
        }
    }
}

/**
 * 复制统计信息
 */
class ReplicationStatistics {
    private final long replicatedMessages;
    private final long failedMessages;
    private final double successRate;
    private final double averageTime;
    private final int queueSize;
    private final int slaveCount;
    
    public ReplicationStatistics(long replicatedMessages, long failedMessages, 
                               double successRate, double averageTime, 
                               int queueSize, int slaveCount) {
        this.replicatedMessages = replicatedMessages;
        this.failedMessages = failedMessages;
        this.successRate = successRate;
        this.averageTime = averageTime;
        this.queueSize = queueSize;
        this.slaveCount = slaveCount;
    }
    
    // Getters
    public long getReplicatedMessages() { return replicatedMessages; }
    public long getFailedMessages() { return failedMessages; }
    public double getSuccessRate() { return successRate; }
    public double getAverageTime() { return averageTime; }
    public int getQueueSize() { return queueSize; }
    public int getSlaveCount() { return slaveCount; }
    
    @Override
    public String toString() {
        return String.format("ReplicationStatistics{replicated=%d, failed=%d, successRate=%.2f%%, " +
                           "avgTime=%.2fms, queueSize=%d, slaveCount=%d}", 
                           replicatedMessages, failedMessages, successRate * 100, 
                           averageTime, queueSize, slaveCount);
    }
}
