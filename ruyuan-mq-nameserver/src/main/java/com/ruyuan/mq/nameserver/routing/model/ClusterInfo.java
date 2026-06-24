package com.ruyuan.mq.nameserver.routing.model;

import java.util.List;
import java.util.Objects;

/**
 * 集群信息
 * 
 * @author RuYuan MQ Team
 */
public class ClusterInfo {
    
    private String clusterId;
    private String clusterName;
    private String clusterType;
    private String region;
    private List<BrokerInfo> brokers;
    private double loadFactor;
    private boolean healthy;
    private long lastUpdateTime;
    
    public ClusterInfo() {
    }
    
    public ClusterInfo(String clusterId, String clusterName, String clusterType, String region) {
        this.clusterId = clusterId;
        this.clusterName = clusterName;
        this.clusterType = clusterType;
        this.region = region;
        this.healthy = true;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    // Getters and Setters
    public String getClusterId() {
        return clusterId;
    }
    
    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }
    
    public String getClusterName() {
        return clusterName;
    }
    
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }
    
    public String getClusterType() {
        return clusterType;
    }
    
    public void setClusterType(String clusterType) {
        this.clusterType = clusterType;
    }
    
    public String getRegion() {
        return region;
    }
    
    public void setRegion(String region) {
        this.region = region;
    }
    
    public List<BrokerInfo> getBrokers() {
        return brokers;
    }
    
    public void setBrokers(List<BrokerInfo> brokers) {
        this.brokers = brokers;
    }
    
    public double getLoadFactor() {
        return loadFactor;
    }
    
    public void setLoadFactor(double loadFactor) {
        this.loadFactor = loadFactor;
    }
    
    public boolean isHealthy() {
        return healthy;
    }
    
    public void setHealthy(boolean healthy) {
        this.healthy = healthy;
    }
    
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }
    
    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterInfo that = (ClusterInfo) o;
        return Objects.equals(clusterId, that.clusterId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(clusterId);
    }
    
    @Override
    public String toString() {
        return "ClusterInfo{" +
                "clusterId='" + clusterId + '\'' +
                ", clusterName='" + clusterName + '\'' +
                ", clusterType='" + clusterType + '\'' +
                ", region='" + region + '\'' +
                ", brokerCount=" + (brokers != null ? brokers.size() : 0) +
                ", loadFactor=" + loadFactor +
                ", healthy=" + healthy +
                ", lastUpdateTime=" + lastUpdateTime +
                '}';
    }
}
