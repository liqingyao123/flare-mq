package com.flare.mq.console.model;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 系统概览信息
 * 
 * @author FlareMQ
 * @version 1.0.0
 */
public class SystemOverview {
    
    private String systemName;
    private String version;
    private LocalDateTime startTime;
    private long uptime;
    private int totalBrokers;
    private int healthyBrokers;
    private int totalTopics;
    private int totalQueues;
    private long totalMessages;
    private double currentTps;
    private String healthStatus;
    private double memoryUsage;
    private double cpuUsage;
    private double diskUsage;
    
    public SystemOverview() {
        this.systemName = "FlareMQ";
        this.version = "1.0.0";
        this.startTime = LocalDateTime.now();
    }
    
    // Getters and Setters
    public String getSystemName() { return systemName; }
    public void setSystemName(String systemName) { this.systemName = systemName; }
    
    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }
    
    public LocalDateTime getStartTime() { return startTime; }
    public void setStartTime(LocalDateTime startTime) { this.startTime = startTime; }
    
    public long getUptime() { return uptime; }
    public void setUptime(long uptime) { this.uptime = uptime; }
    
    public int getTotalBrokers() { return totalBrokers; }
    public void setTotalBrokers(int totalBrokers) { this.totalBrokers = totalBrokers; }
    
    public int getHealthyBrokers() { return healthyBrokers; }
    public void setHealthyBrokers(int healthyBrokers) { this.healthyBrokers = healthyBrokers; }
    
    public int getTotalTopics() { return totalTopics; }
    public void setTotalTopics(int totalTopics) { this.totalTopics = totalTopics; }
    
    public int getTotalQueues() { return totalQueues; }
    public void setTotalQueues(int totalQueues) { this.totalQueues = totalQueues; }
    
    public long getTotalMessages() { return totalMessages; }
    public void setTotalMessages(long totalMessages) { this.totalMessages = totalMessages; }
    
    public double getCurrentTps() { return currentTps; }
    public void setCurrentTps(double currentTps) { this.currentTps = currentTps; }
    
    public String getHealthStatus() { return healthStatus; }
    public void setHealthStatus(String healthStatus) { this.healthStatus = healthStatus; }
    
    public double getMemoryUsage() { return memoryUsage; }
    public void setMemoryUsage(double memoryUsage) { this.memoryUsage = memoryUsage; }
    
    public double getCpuUsage() { return cpuUsage; }
    public void setCpuUsage(double cpuUsage) { this.cpuUsage = cpuUsage; }
    
    public double getDiskUsage() { return diskUsage; }
    public void setDiskUsage(double diskUsage) { this.diskUsage = diskUsage; }
    
    /**
     * 格式化显示
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== FlareMQ System Overview ===\n");
        sb.append("System Name: ").append(systemName).append(" v").append(version).append("\n");
        sb.append("Start Time: ").append(startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))).append("\n");
        sb.append("Uptime: ").append(formatUptime(uptime)).append("\n");
        sb.append("Health Status: ").append(healthStatus).append("\n");
        sb.append("\n--- Cluster Status ---\n");
        sb.append("Broker Nodes: ").append(healthyBrokers).append("/").append(totalBrokers).append(" (healthy/total)\n");
        sb.append("Topic Count: ").append(totalTopics).append("\n");
        sb.append("Queue Count: ").append(totalQueues).append("\n");
        sb.append("Total Messages: ").append(totalMessages).append("\n");
        sb.append("Current TPS: ").append(String.format("%.2f", currentTps)).append("\n");
        sb.append("\n--- Resource Usage ---\n");
        sb.append("Memory Usage: ").append(String.format("%.1f%%", memoryUsage * 100)).append("\n");
        sb.append("CPU Usage: ").append(String.format("%.1f%%", cpuUsage * 100)).append("\n");
        sb.append("Disk Usage: ").append(String.format("%.1f%%", diskUsage * 100)).append("\n");

        return sb.toString();
    }
    
    /**
     * 格式化运行时长
     */
    private String formatUptime(long uptimeMs) {
        long seconds = uptimeMs / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;
        long days = hours / 24;

        if (days > 0) {
            return String.format("%d days %d hours %d minutes", days, hours % 24, minutes % 60);
        } else if (hours > 0) {
            return String.format("%d hours %d minutes", hours, minutes % 60);
        } else {
            return String.format("%d minutes", minutes);
        }
    }
}
