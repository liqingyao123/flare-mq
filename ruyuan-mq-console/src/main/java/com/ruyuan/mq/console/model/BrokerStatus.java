package com.ruyuan.mq.console.model;

import java.time.LocalDateTime;

/**
 * Broker状态信息
 * 
 * @author RuYuan
 * @version 1.0.0
 */
public class BrokerStatus {
    
    private String brokerName;
    private String brokerAddr;
    private String clusterName;
    private String role; // MASTER, SLAVE
    private String status; // RUNNING, STOPPED, ERROR
    private LocalDateTime lastUpdateTime;
    private double cpuUsage;
    private double memoryUsage;
    private double diskUsage;
    private int activeConnections;
    private long totalMessages;
    private double currentTps;
    private boolean healthy;
    
    public BrokerStatus() {}
    
    public BrokerStatus(String brokerName, String brokerAddr, String clusterName) {
        this.brokerName = brokerName;
        this.brokerAddr = brokerAddr;
        this.clusterName = clusterName;
        this.lastUpdateTime = LocalDateTime.now();
    }
    
    // Getters and Setters
    public String getBrokerName() { return brokerName; }
    public void setBrokerName(String brokerName) { this.brokerName = brokerName; }
    
    public String getBrokerAddr() { return brokerAddr; }
    public void setBrokerAddr(String brokerAddr) { this.brokerAddr = brokerAddr; }
    
    public String getClusterName() { return clusterName; }
    public void setClusterName(String clusterName) { this.clusterName = clusterName; }
    
    public String getRole() { return role; }
    public void setRole(String role) { this.role = role; }
    
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    
    public LocalDateTime getLastUpdateTime() { return lastUpdateTime; }
    public void setLastUpdateTime(LocalDateTime lastUpdateTime) { this.lastUpdateTime = lastUpdateTime; }
    
    public double getCpuUsage() { return cpuUsage; }
    public void setCpuUsage(double cpuUsage) { this.cpuUsage = cpuUsage; }
    
    public double getMemoryUsage() { return memoryUsage; }
    public void setMemoryUsage(double memoryUsage) { this.memoryUsage = memoryUsage; }
    
    public double getDiskUsage() { return diskUsage; }
    public void setDiskUsage(double diskUsage) { this.diskUsage = diskUsage; }
    
    public int getActiveConnections() { return activeConnections; }
    public void setActiveConnections(int activeConnections) { this.activeConnections = activeConnections; }
    
    public long getTotalMessages() { return totalMessages; }
    public void setTotalMessages(long totalMessages) { this.totalMessages = totalMessages; }
    
    public double getCurrentTps() { return currentTps; }
    public void setCurrentTps(double currentTps) { this.currentTps = currentTps; }
    
    public boolean isHealthy() { return healthy; }
    public void setHealthy(boolean healthy) { this.healthy = healthy; }
    
    @Override
    public String toString() {
        return String.format("BrokerStatus{name='%s', addr='%s', cluster='%s', role='%s', status='%s', healthy=%s, tps=%.2f}",
                brokerName, brokerAddr, clusterName, role, status, healthy, currentTps);
    }
}
