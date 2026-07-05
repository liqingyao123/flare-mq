package com.flare.mq.nameserver.routing.model;

import java.util.Objects;

/**
 * Broker信息
 * 
 * @author FlareMQ Team
 */
public class BrokerInfo {
    
    private String brokerId;
    private String brokerName;
    private String host;
    private int port;
    private String clusterId;
    private int queueCount;
    private double cpuUsage;
    private double memoryUsage;
    private double diskUsage;
    private long messageCount;
    private boolean healthy;
    private long lastHeartbeatTime;
    private int weight; // 权重，用于加权负载均衡
    private int activeConnections; // 活跃连接数
    
    public BrokerInfo() {
    }
    
    public BrokerInfo(String brokerId, String brokerName, String host, int port, String clusterId) {
        this.brokerId = brokerId;
        this.brokerName = brokerName;
        this.host = host;
        this.port = port;
        this.clusterId = clusterId;
        this.healthy = true;
        this.weight = 100; // 默认权重
        this.lastHeartbeatTime = System.currentTimeMillis();
    }
    
    // Getters and Setters
    public String getBrokerId() {
        return brokerId;
    }
    
    public void setBrokerId(String brokerId) {
        this.brokerId = brokerId;
    }
    
    public String getBrokerName() {
        return brokerName;
    }
    
    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
    
    public String getHost() {
        return host;
    }
    
    public void setHost(String host) {
        this.host = host;
    }
    
    public int getPort() {
        return port;
    }
    
    public void setPort(int port) {
        this.port = port;
    }
    
    public String getClusterId() {
        return clusterId;
    }
    
    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }
    
    public int getQueueCount() {
        return queueCount;
    }
    
    public void setQueueCount(int queueCount) {
        this.queueCount = queueCount;
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
    
    public long getMessageCount() {
        return messageCount;
    }
    
    public void setMessageCount(long messageCount) {
        this.messageCount = messageCount;
    }
    
    public boolean isHealthy() {
        return healthy;
    }
    
    public void setHealthy(boolean healthy) {
        this.healthy = healthy;
    }
    
    public long getLastHeartbeatTime() {
        return lastHeartbeatTime;
    }
    
    public void setLastHeartbeatTime(long lastHeartbeatTime) {
        this.lastHeartbeatTime = lastHeartbeatTime;
    }
    
    public int getWeight() {
        return weight;
    }
    
    public void setWeight(int weight) {
        this.weight = weight;
    }
    
    public int getActiveConnections() {
        return activeConnections;
    }
    
    public void setActiveConnections(int activeConnections) {
        this.activeConnections = activeConnections;
    }
    
    /**
     * 计算Broker的负载评分
     * 评分越低表示负载越轻
     */
    public double calculateLoadScore() {
        // 综合CPU、内存、磁盘使用率和活跃连接数计算负载评分
        double cpuScore = cpuUsage * 0.3;
        double memoryScore = memoryUsage * 0.3;
        double diskScore = diskUsage * 0.2;
        double connectionScore = (activeConnections / 1000.0) * 0.2; // 假设1000为满负载连接数
        
        return cpuScore + memoryScore + diskScore + connectionScore;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrokerInfo that = (BrokerInfo) o;
        return Objects.equals(brokerId, that.brokerId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(brokerId);
    }
    
    @Override
    public String toString() {
        return "BrokerInfo{" +
                "brokerId='" + brokerId + '\'' +
                ", brokerName='" + brokerName + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", clusterId='" + clusterId + '\'' +
                ", queueCount=" + queueCount +
                ", cpuUsage=" + cpuUsage +
                ", memoryUsage=" + memoryUsage +
                ", healthy=" + healthy +
                ", weight=" + weight +
                ", activeConnections=" + activeConnections +
                '}';
    }
}
