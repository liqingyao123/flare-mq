package com.ruyuan.mq.store;

/**
 * 自适应存储策略接口
 * 
 * @author RuYuan MQ Team
 */
public interface AdaptiveStorageStrategy {
    
    /**
     * 选择存储类型
     */
    StorageType selectStorageType(MessageMetrics metrics);
    
    /**
     * 选择压缩类型
     */
    CompressionType selectCompression(MessagePattern pattern);
    
    /**
     * 调整策略
     */
    void adjustStrategy(PerformanceMetrics performance);
    
    /**
     * 是否需要迁移存储层级
     */
    boolean shouldMigrate(MessageMetrics metrics, StorageType currentType);
    
    /**
     * 获取目标存储类型
     */
    StorageType getTargetStorageType(MessageMetrics metrics);
}

/**
 * 存储类型枚举
 */
enum StorageType {
    /**
     * 热存储：SSD + 内存缓存
     */
    HOT_STORAGE,
    
    /**
     * 温存储：SSD
     */
    WARM_STORAGE,
    
    /**
     * 冷存储：HDD + 压缩
     */
    COLD_STORAGE,
    
    /**
     * 归档存储：对象存储
     */
    ARCHIVE_STORAGE
}

/**
 * 性能指标
 */
class PerformanceMetrics {
    /**
     * 平均读取延迟（毫秒）
     */
    private double avgReadLatency;
    
    /**
     * 平均写入延迟（毫秒）
     */
    private double avgWriteLatency;
    
    /**
     * 存储空间使用率
     */
    private double storageUtilization;
    
    /**
     * 缓存命中率
     */
    private double cacheHitRate;
    
    /**
     * 压缩比
     */
    private double compressionRatio;
    
    /**
     * 构造函数
     */
    public PerformanceMetrics() {
    }
    
    public PerformanceMetrics(double avgReadLatency, double avgWriteLatency, 
                            double storageUtilization, double cacheHitRate, 
                            double compressionRatio) {
        this.avgReadLatency = avgReadLatency;
        this.avgWriteLatency = avgWriteLatency;
        this.storageUtilization = storageUtilization;
        this.cacheHitRate = cacheHitRate;
        this.compressionRatio = compressionRatio;
    }
    
    // ========== Getter和Setter方法 ==========
    
    public double getAvgReadLatency() {
        return avgReadLatency;
    }
    
    public void setAvgReadLatency(double avgReadLatency) {
        this.avgReadLatency = avgReadLatency;
    }
    
    public double getAvgWriteLatency() {
        return avgWriteLatency;
    }
    
    public void setAvgWriteLatency(double avgWriteLatency) {
        this.avgWriteLatency = avgWriteLatency;
    }
    
    public double getStorageUtilization() {
        return storageUtilization;
    }
    
    public void setStorageUtilization(double storageUtilization) {
        this.storageUtilization = storageUtilization;
    }
    
    public double getCacheHitRate() {
        return cacheHitRate;
    }
    
    public void setCacheHitRate(double cacheHitRate) {
        this.cacheHitRate = cacheHitRate;
    }
    
    public double getCompressionRatio() {
        return compressionRatio;
    }
    
    public void setCompressionRatio(double compressionRatio) {
        this.compressionRatio = compressionRatio;
    }
    
    @Override
    public String toString() {
        return "PerformanceMetrics{" +
                "avgReadLatency=" + avgReadLatency +
                ", avgWriteLatency=" + avgWriteLatency +
                ", storageUtilization=" + storageUtilization +
                ", cacheHitRate=" + cacheHitRate +
                ", compressionRatio=" + compressionRatio +
                '}';
    }
}
