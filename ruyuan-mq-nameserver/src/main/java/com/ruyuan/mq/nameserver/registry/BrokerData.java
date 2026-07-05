package com.ruyuan.mq.nameserver.registry;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Broker数据模型
 * 
 * @author RuYuan MQ Team
 */
public class BrokerData {
    
    private String cluster;
    private String brokerName;
    private Map<Long, String> brokerAddrs; // Key: brokerId, Value: brokerAddr
    private volatile long lastUpdateTimestamp;
    private double cpuUsage;
    private double memoryUsage;
    private double diskUsage;
    private long totalMessages;
    private double currentTps;
    private java.util.List<java.util.Map<String, Object>> topicStats;

    public BrokerData() {
        this.brokerAddrs = new ConcurrentHashMap<>();
        this.lastUpdateTimestamp = System.currentTimeMillis();
    }
    
    public BrokerData(String cluster, String brokerName) {
        this();
        this.cluster = cluster;
        this.brokerName = brokerName;
    }
    
    // Getters and Setters
    public String getCluster() {
        return cluster;
    }
    
    public void setCluster(String cluster) {
        this.cluster = cluster;
    }
    
    public String getBrokerName() {
        return brokerName;
    }
    
    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
    
    public Map<Long, String> getBrokerAddrs() {
        return brokerAddrs;
    }
    
    public void setBrokerAddrs(Map<Long, String> brokerAddrs) {
        this.brokerAddrs = brokerAddrs;
    }
    
    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }
    
    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
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

    public long getTotalMessages() {
        return totalMessages;
    }

    public void setTotalMessages(long totalMessages) {
        this.totalMessages = totalMessages;
    }

    public double getCurrentTps() {
        return currentTps;
    }

    public void setCurrentTps(double currentTps) {
        this.currentTps = currentTps;
    }

    public java.util.List<java.util.Map<String, Object>> getTopicStats() {
        return topicStats;
    }

    public void setTopicStats(java.util.List<java.util.Map<String, Object>> topicStats) {
        this.topicStats = topicStats;
    }

    /**
     * 获取Master Broker地址
     */
    public String getMasterAddr() {
        return brokerAddrs.get(0L);
    }
    
    /**
     * 检查是否有Master Broker
     */
    public boolean hasMaster() {
        return brokerAddrs.containsKey(0L);
    }
    
    /**
     * 获取Slave Broker数量
     */
    public int getSlaveCount() {
        return (int) brokerAddrs.keySet().stream().filter(id -> id > 0).count();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        BrokerData that = (BrokerData) obj;
        return brokerName != null ? brokerName.equals(that.brokerName) : that.brokerName == null;
    }
    
    @Override
    public int hashCode() {
        return brokerName != null ? brokerName.hashCode() : 0;
    }
    
    @Override
    public String toString() {
        return "BrokerData{" +
                "cluster='" + cluster + '\'' +
                ", brokerName='" + brokerName + '\'' +
                ", brokerAddrs=" + brokerAddrs +
                ", lastUpdateTimestamp=" + lastUpdateTimestamp +
                ", cpuUsage=" + cpuUsage +
                ", memoryUsage=" + memoryUsage +
                ", diskUsage=" + diskUsage +
                ", totalMessages=" + totalMessages +
                ", currentTps=" + currentTps +
                '}';
    }
}
