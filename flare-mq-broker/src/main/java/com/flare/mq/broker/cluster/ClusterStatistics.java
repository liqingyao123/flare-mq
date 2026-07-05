package com.flare.mq.broker.cluster;

/**
 * 集群统计信息
 * 
 * @author FlareMQ Team
 */
public class ClusterStatistics {
    
    private final String clusterName;
    private final int totalNodes;
    private final int healthyNodes;
    private final String masterBroker;
    private final long stateVersion;
    private final ClusterState clusterState;
    private final long timestamp;
    
    public ClusterStatistics(String clusterName, int totalNodes, int healthyNodes, 
                           String masterBroker, long stateVersion, ClusterState clusterState) {
        this.clusterName = clusterName;
        this.totalNodes = totalNodes;
        this.healthyNodes = healthyNodes;
        this.masterBroker = masterBroker;
        this.stateVersion = stateVersion;
        this.clusterState = clusterState;
        this.timestamp = System.currentTimeMillis();
    }
    
    // Getters
    public String getClusterName() {
        return clusterName;
    }
    
    public int getTotalNodes() {
        return totalNodes;
    }
    
    public int getHealthyNodes() {
        return healthyNodes;
    }
    
    public int getUnhealthyNodes() {
        return totalNodes - healthyNodes;
    }
    
    public String getMasterBroker() {
        return masterBroker;
    }
    
    public long getStateVersion() {
        return stateVersion;
    }
    
    public ClusterState getClusterState() {
        return clusterState;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    /**
     * 计算健康比例
     */
    public double getHealthRatio() {
        return totalNodes > 0 ? (double) healthyNodes / totalNodes : 0.0;
    }
    
    /**
     * 检查集群是否健康
     */
    public boolean isHealthy() {
        return clusterState.isHealthy() && getHealthRatio() >= 0.5;
    }
    
    /**
     * 检查是否有Master
     */
    public boolean hasMaster() {
        return masterBroker != null && !masterBroker.equals("none");
    }
    
    /**
     * 获取集群健康等级
     */
    public HealthLevel getHealthLevel() {
        double ratio = getHealthRatio();
        
        if (!clusterState.isActive()) {
            return HealthLevel.CRITICAL;
        } else if (ratio >= 0.8) {
            return HealthLevel.EXCELLENT;
        } else if (ratio >= 0.6) {
            return HealthLevel.GOOD;
        } else if (ratio >= 0.4) {
            return HealthLevel.WARNING;
        } else {
            return HealthLevel.CRITICAL;
        }
    }
    
    @Override
    public String toString() {
        return String.format("ClusterStatistics{" +
                "cluster='%s', nodes=%d/%d(%.1f%%), master='%s', " +
                "state=%s, version=%d, health=%s}", 
                clusterName, healthyNodes, totalNodes, getHealthRatio() * 100, 
                masterBroker, clusterState.getDisplayName(), stateVersion, 
                getHealthLevel());
    }
    
    /**
     * 健康等级枚举
     */
    public enum HealthLevel {
        EXCELLENT("优秀", "集群运行状态优秀"),
        GOOD("良好", "集群运行状态良好"),
        WARNING("警告", "集群运行状态需要关注"),
        CRITICAL("严重", "集群运行状态严重，需要立即处理");
        
        private final String displayName;
        private final String description;
        
        HealthLevel(String displayName, String description) {
            this.displayName = displayName;
            this.description = description;
        }
        
        public String getDisplayName() { return displayName; }
        public String getDescription() { return description; }
        
        @Override
        public String toString() {
            return displayName;
        }
    }
}
