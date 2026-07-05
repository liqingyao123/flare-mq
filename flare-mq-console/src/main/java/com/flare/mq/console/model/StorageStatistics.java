package com.flare.mq.console.model;

import java.time.LocalDateTime;

/**
 * 存储统计信息
 * 
 * @author FlareMQ
 * @version 1.0.0
 */
public class StorageStatistics {
    private long totalSize;
    private long usedSize;
    private long freeSize;
    private double usageRatio;
    private int totalFiles;
    private long totalMessages;
    private double compressionRatio;
    private LocalDateTime lastUpdateTime;
    
    public StorageStatistics() {
        this.lastUpdateTime = LocalDateTime.now();
    }
    
    // Getters and Setters
    public long getTotalSize() { return totalSize; }
    public void setTotalSize(long totalSize) { 
        this.totalSize = totalSize;
        updateUsageRatio();
    }
    
    public long getUsedSize() { return usedSize; }
    public void setUsedSize(long usedSize) { 
        this.usedSize = usedSize;
        updateUsageRatio();
    }
    
    public long getFreeSize() { return freeSize; }
    public void setFreeSize(long freeSize) { this.freeSize = freeSize; }
    
    public double getUsageRatio() { return usageRatio; }
    
    private void updateUsageRatio() {
        if (totalSize > 0) {
            this.usageRatio = (double) usedSize / totalSize;
            this.freeSize = totalSize - usedSize;
        }
    }
    
    public int getTotalFiles() { return totalFiles; }
    public void setTotalFiles(int totalFiles) { this.totalFiles = totalFiles; }
    
    public long getTotalMessages() { return totalMessages; }
    public void setTotalMessages(long totalMessages) { this.totalMessages = totalMessages; }
    
    public double getCompressionRatio() { return compressionRatio; }
    public void setCompressionRatio(double compressionRatio) { this.compressionRatio = compressionRatio; }
    
    public LocalDateTime getLastUpdateTime() { return lastUpdateTime; }
    public void setLastUpdateTime(LocalDateTime lastUpdateTime) { this.lastUpdateTime = lastUpdateTime; }
    
    /**
     * 格式化大小显示
     */
    public String formatSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }
    
    @Override
    public String toString() {
        return String.format("StorageStatistics{used=%s, total=%s, usage=%.1f%%, files=%d, messages=%d}",
                formatSize(usedSize), formatSize(totalSize), usageRatio * 100, totalFiles, totalMessages);
    }
}
