package com.ruyuan.mq.broker.cluster;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Broker节点模型
 * 
 * @author RuYuan MQ Team
 */
public class BrokerNode {
    
    private final String brokerName;
    private final String brokerAddr;
    private final long brokerId;
    private volatile BrokerRole role;
    private volatile long lastUpdateTime;
    private volatile BrokerStatus status;
    
    // 性能指标
    private final AtomicLong messageCount;
    private final AtomicLong byteCount;
    private volatile double cpuUsage;
    private volatile double memoryUsage;
    private volatile double diskUsage;
    private volatile long networkLatency;
    
    // 健康状态
    private volatile boolean healthy;
    private volatile String healthMessage;
    private final AtomicLong failureCount;
    
    public BrokerNode(String brokerName, String brokerAddr, long brokerId, 
                     BrokerRole role, long lastUpdateTime) {
        this.brokerName = brokerName;
        this.brokerAddr = brokerAddr;
        this.brokerId = brokerId;
        this.role = role;
        this.lastUpdateTime = lastUpdateTime;
        this.status = BrokerStatus.STARTING;
        this.messageCount = new AtomicLong(0);
        this.byteCount = new AtomicLong(0);
        this.cpuUsage = 0.0;
        this.memoryUsage = 0.0;
        this.diskUsage = 0.0;
        this.networkLatency = 0L;
        this.healthy = true;
        this.healthMessage = "OK";
        this.failureCount = new AtomicLong(0);
    }
    
    // Getters and Setters
    public String getBrokerName() {
        return brokerName;
    }
    
    public String getBrokerAddr() {
        return brokerAddr;
    }
    
    public long getBrokerId() {
        return brokerId;
    }
    
    public BrokerRole getRole() {
        return role;
    }
    
    public void setRole(BrokerRole role) {
        this.role = role;
    }
    
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }
    
    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }
    
    public BrokerStatus getStatus() {
        return status;
    }
    
    public void setStatus(BrokerStatus status) {
        this.status = status;
    }
    
    public long getMessageCount() {
        return messageCount.get();
    }
    
    public void incrementMessageCount() {
        messageCount.incrementAndGet();
    }
    
    public void addMessageCount(long count) {
        messageCount.addAndGet(count);
    }
    
    public long getByteCount() {
        return byteCount.get();
    }
    
    public void incrementByteCount(long bytes) {
        byteCount.addAndGet(bytes);
    }
    
    public double getCpuUsage() {
        return cpuUsage;
    }
    
    public void setCpuUsage(double cpuUsage) {
        this.cpuUsage = cpuUsage;
    }
    
    public double getMemoryUsage() {
        return memoryUsage;
    }
    
    public void setMemoryUsage(double memoryUsage) {
        this.memoryUsage = memoryUsage;
    }
    
    public double getDiskUsage() {
        return diskUsage;
    }
    
    public void setDiskUsage(double diskUsage) {
        this.diskUsage = diskUsage;
    }
    
    public long getNetworkLatency() {
        return networkLatency;
    }
    
    public void setNetworkLatency(long networkLatency) {
        this.networkLatency = networkLatency;
    }
    
    public boolean isHealthy() {
        return healthy;
    }
    
    public void setHealthy(boolean healthy) {
        this.healthy = healthy;
    }
    
    public String getHealthMessage() {
        return healthMessage;
    }
    
    public void setHealthMessage(String healthMessage) {
        this.healthMessage = healthMessage;
    }
    
    public long getFailureCount() {
        return failureCount.get();
    }
    
    public void incrementFailureCount() {
        failureCount.incrementAndGet();
    }
    
    public void resetFailureCount() {
        failureCount.set(0);
    }
    
    /**
     * 检查节点是否活跃
     */
    public boolean isActive(long timeoutMs) {
        long currentTime = System.currentTimeMillis();
        return (currentTime - lastUpdateTime) <= timeoutMs;
    }
    
    /**
     * 检查节点是否为Master
     */
    public boolean isMaster() {
        return role == BrokerRole.MASTER;
    }
    
    /**
     * 检查节点是否为Slave
     */
    public boolean isSlave() {
        return role == BrokerRole.SLAVE;
    }
    
    /**
     * 检查节点是否可用
     */
    public boolean isAvailable() {
        return healthy && status == BrokerStatus.RUNNING;
    }
    
    /**
     * 计算负载分数（越低越好）
     */
    public double calculateLoadScore() {
        // 综合考虑CPU、内存、磁盘使用率和网络延迟
        double cpuScore = cpuUsage * 0.3;
        double memoryScore = memoryUsage * 0.3;
        double diskScore = diskUsage * 0.2;
        double latencyScore = Math.min(networkLatency / 100.0, 1.0) * 0.2;
        
        return cpuScore + memoryScore + diskScore + latencyScore;
    }
    
    /**
     * 更新性能指标
     */
    public void updateMetrics(double cpuUsage, double memoryUsage, double diskUsage, long networkLatency) {
        this.cpuUsage = cpuUsage;
        this.memoryUsage = memoryUsage;
        this.diskUsage = diskUsage;
        this.networkLatency = networkLatency;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    /**
     * 更新健康状态
     */
    public void updateHealth(boolean healthy, String message) {
        this.healthy = healthy;
        this.healthMessage = message;
        this.lastUpdateTime = System.currentTimeMillis();
        
        if (healthy) {
            resetFailureCount();
        } else {
            incrementFailureCount();
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        BrokerNode that = (BrokerNode) obj;
        return brokerId == that.brokerId && 
               brokerName.equals(that.brokerName) && 
               brokerAddr.equals(that.brokerAddr);
    }
    
    @Override
    public int hashCode() {
        int result = brokerName.hashCode();
        result = 31 * result + brokerAddr.hashCode();
        result = 31 * result + Long.hashCode(brokerId);
        return result;
    }
    
    @Override
    public String toString() {
        return "BrokerNode{" +
                "brokerName='" + brokerName + '\'' +
                ", brokerAddr='" + brokerAddr + '\'' +
                ", brokerId=" + brokerId +
                ", role=" + role +
                ", status=" + status +
                ", healthy=" + healthy +
                ", cpuUsage=" + String.format("%.2f%%", cpuUsage * 100) +
                ", memoryUsage=" + String.format("%.2f%%", memoryUsage * 100) +
                ", diskUsage=" + String.format("%.2f%%", diskUsage * 100) +
                ", networkLatency=" + networkLatency + "ms" +
                ", messageCount=" + messageCount.get() +
                ", lastUpdateTime=" + lastUpdateTime +
                '}';
    }
}

/**
 * Broker角色枚举
 */
enum BrokerRole {
    MASTER("Master", "主节点，负责写入和读取"),
    SLAVE("Slave", "从节点，负责读取和备份");
    
    private final String displayName;
    private final String description;
    
    BrokerRole(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
    }
    
    public String getDisplayName() { return displayName; }
    public String getDescription() { return description; }
}

/**
 * Broker状态枚举
 */
enum BrokerStatus {
    STARTING("启动中"),
    RUNNING("运行中"),
    STOPPING("停止中"),
    STOPPED("已停止"),
    ERROR("错误状态");
    
    private final String description;
    
    BrokerStatus(String description) {
        this.description = description;
    }
    
    public String getDescription() { return description; }
}
