package com.flare.mq.console.model;

import java.time.LocalDateTime;

/**
 * 性能指标
 * 
 * @author RuYuan
 * @version 1.0.0
 */
public class PerformanceMetrics {
    
    private double avgReadLatency;
    private double avgWriteLatency;
    private double storageUtilization;
    private double cacheHitRate;
    private double compressionRatio;
    private double networkThroughput;
    private long totalOperations;
    private LocalDateTime timestamp;
    
    public PerformanceMetrics() {
        this.timestamp = LocalDateTime.now();
    }
    
    public PerformanceMetrics(double avgReadLatency, double avgWriteLatency, 
                            double storageUtilization, double cacheHitRate, double compressionRatio) {
        this.avgReadLatency = avgReadLatency;
        this.avgWriteLatency = avgWriteLatency;
        this.storageUtilization = storageUtilization;
        this.cacheHitRate = cacheHitRate;
        this.compressionRatio = compressionRatio;
        this.timestamp = LocalDateTime.now();
    }
    
    // Getters and Setters
    public double getAvgReadLatency() { return avgReadLatency; }
    public void setAvgReadLatency(double avgReadLatency) { this.avgReadLatency = avgReadLatency; }
    
    public double getAvgWriteLatency() { return avgWriteLatency; }
    public void setAvgWriteLatency(double avgWriteLatency) { this.avgWriteLatency = avgWriteLatency; }
    
    public double getStorageUtilization() { return storageUtilization; }
    public void setStorageUtilization(double storageUtilization) { this.storageUtilization = storageUtilization; }
    
    public double getCacheHitRate() { return cacheHitRate; }
    public void setCacheHitRate(double cacheHitRate) { this.cacheHitRate = cacheHitRate; }
    
    public double getCompressionRatio() { return compressionRatio; }
    public void setCompressionRatio(double compressionRatio) { this.compressionRatio = compressionRatio; }
    
    public double getNetworkThroughput() { return networkThroughput; }
    public void setNetworkThroughput(double networkThroughput) { this.networkThroughput = networkThroughput; }
    
    public long getTotalOperations() { return totalOperations; }
    public void setTotalOperations(long totalOperations) { this.totalOperations = totalOperations; }
    
    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }
    
    @Override
    public String toString() {
        return String.format("PerformanceMetrics{readLatency=%.2fms, writeLatency=%.2fms, " +
                           "storage=%.1f%%, cacheHit=%.1f%%, compression=%.2fx, operations=%d}",
                avgReadLatency, avgWriteLatency, storageUtilization * 100,
                cacheHitRate * 100, compressionRatio, totalOperations);
    }
}
