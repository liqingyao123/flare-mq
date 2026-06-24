package com.ruyuan.mq.console.model;

import java.time.LocalDateTime;

/**
 * 集群健康状态
 * 
 * @author RuYuan
 * @version 1.0.0
 */
public class ClusterHealth {
    private String overallStatus; // HEALTHY, WARNING, CRITICAL
    private int totalNodes;
    private int healthyNodes;
    private double healthRatio;
    private String masterBroker;
    private int activeConnections;
    private LocalDateTime lastCheckTime;
    
    public ClusterHealth() {
        this.lastCheckTime = LocalDateTime.now();
    }
    
    // Getters and Setters
    public String getOverallStatus() { return overallStatus; }
    public void setOverallStatus(String overallStatus) { this.overallStatus = overallStatus; }
    
    public int getTotalNodes() { return totalNodes; }
    public void setTotalNodes(int totalNodes) { this.totalNodes = totalNodes; }
    
    public int getHealthyNodes() { return healthyNodes; }
    public void setHealthyNodes(int healthyNodes) { this.healthyNodes = healthyNodes; }
    
    public double getHealthRatio() { return healthRatio; }
    public void setHealthRatio(double healthRatio) { this.healthRatio = healthRatio; }
    
    public String getMasterBroker() { return masterBroker; }
    public void setMasterBroker(String masterBroker) { this.masterBroker = masterBroker; }
    
    public int getActiveConnections() { return activeConnections; }
    public void setActiveConnections(int activeConnections) { this.activeConnections = activeConnections; }
    
    public LocalDateTime getLastCheckTime() { return lastCheckTime; }
    public void setLastCheckTime(LocalDateTime lastCheckTime) { this.lastCheckTime = lastCheckTime; }
    
    @Override
    public String toString() {
        return String.format("ClusterHealth{status='%s', nodes=%d/%d, ratio=%.1f%%, master='%s'}",
                overallStatus, healthyNodes, totalNodes, healthRatio * 100, masterBroker);
    }
}
