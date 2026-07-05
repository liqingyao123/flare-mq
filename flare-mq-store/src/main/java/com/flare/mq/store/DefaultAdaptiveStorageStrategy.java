package com.flare.mq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 默认自适应存储策略实现
 * 
 * @author FlareMQ Team
 */
public class DefaultAdaptiveStorageStrategy implements AdaptiveStorageStrategy {
    
    private static final Logger logger = LoggerFactory.getLogger(DefaultAdaptiveStorageStrategy.class);
    
    /**
     * 消息热度分析器
     */
    private final MessageHeatAnalyzer heatAnalyzer;
    
    /**
     * 动态调整参数
     */
    private volatile double hotThreshold = StoreConstants.HOT_STORAGE_THRESHOLD;
    private volatile double warmThreshold = StoreConstants.WARM_STORAGE_THRESHOLD;
    private volatile double compressionThreshold = 1024; // 1KB以上才考虑压缩
    
    /**
     * 构造函数
     */
    public DefaultAdaptiveStorageStrategy(MessageHeatAnalyzer heatAnalyzer) {
        this.heatAnalyzer = heatAnalyzer;
    }
    
    @Override
    public StorageType selectStorageType(MessageMetrics metrics) {
        if (metrics == null) {
            return StorageType.WARM_STORAGE; // 默认温存储
        }
        
        double heatScore = heatAnalyzer.calculateHeatScore(metrics);
        
        if (heatScore >= hotThreshold) {
            logger.debug("选择热存储: topic={}, queueId={}, heatScore={:.2f}", 
                        metrics.getTopic(), metrics.getQueueId(), heatScore);
            return StorageType.HOT_STORAGE;
        } else if (heatScore >= warmThreshold) {
            logger.debug("选择温存储: topic={}, queueId={}, heatScore={:.2f}", 
                        metrics.getTopic(), metrics.getQueueId(), heatScore);
            return StorageType.WARM_STORAGE;
        } else {
            // 检查是否需要归档
            if (shouldArchive(metrics)) {
                logger.debug("选择归档存储: topic={}, queueId={}, heatScore={:.2f}", 
                            metrics.getTopic(), metrics.getQueueId(), heatScore);
                return StorageType.ARCHIVE_STORAGE;
            } else {
                logger.debug("选择冷存储: topic={}, queueId={}, heatScore={:.2f}", 
                            metrics.getTopic(), metrics.getQueueId(), heatScore);
                return StorageType.COLD_STORAGE;
            }
        }
    }
    
    @Override
    public CompressionType selectCompression(MessagePattern pattern) {
        if (pattern == null || pattern.getSize() < compressionThreshold) {
            return CompressionType.NONE;
        }
        
        CompressionRecommendation recommendation = pattern.getCompressionRecommendation();
        CompressionType recommendedType = recommendation.getType();
        
        logger.debug("压缩选择: size={}, type={}, reason={}", 
                    pattern.getSize(), recommendedType, recommendation.getReason());
        
        return recommendedType;
    }
    
    @Override
    public void adjustStrategy(PerformanceMetrics performance) {
        if (performance == null) {
            return;
        }
        
        logger.info("调整存储策略: {}", performance);
        
        // 根据性能指标动态调整阈值
        adjustThresholds(performance);
        
        // 根据压缩效果调整压缩策略
        adjustCompressionStrategy(performance);
    }
    
    @Override
    public boolean shouldMigrate(MessageMetrics metrics, StorageType currentType) {
        if (metrics == null || currentType == null) {
            return false;
        }
        
        StorageType targetType = getTargetStorageType(metrics);
        boolean shouldMigrate = !currentType.equals(targetType);
        
        if (shouldMigrate) {
            logger.info("需要迁移存储: topic={}, queueId={}, from={}, to={}", 
                       metrics.getTopic(), metrics.getQueueId(), currentType, targetType);
        }
        
        return shouldMigrate;
    }
    
    @Override
    public StorageType getTargetStorageType(MessageMetrics metrics) {
        return selectStorageType(metrics);
    }
    
    /**
     * 是否需要归档
     */
    private boolean shouldArchive(MessageMetrics metrics) {
        // 超过保留时间且访问频率极低的消息需要归档
        double ageDays = metrics.getAgeDays();
        double accessFrequency = metrics.getAccessFrequency();
        double lastAccessHours = metrics.getLastAccessHours();
        
        // 归档条件：
        // 1. 消息年龄超过7天
        // 2. 访问频率低于0.1次/天
        // 3. 最近7天未访问
        return ageDays > 7 && accessFrequency < 0.1 && lastAccessHours > 168; // 168小时 = 7天
    }
    
    /**
     * 调整阈值
     */
    private void adjustThresholds(PerformanceMetrics performance) {
        double readLatency = performance.getAvgReadLatency();
        double storageUtilization = performance.getStorageUtilization();
        double cacheHitRate = performance.getCacheHitRate();
        
        // 如果读取延迟过高，降低热存储阈值，让更多数据进入热存储
        if (readLatency > 10.0) { // 10ms
            hotThreshold = Math.max(6.0, hotThreshold - 0.5);
            warmThreshold = Math.max(3.0, warmThreshold - 0.5);
            logger.info("读取延迟过高，降低存储阈值: hot={:.1f}, warm={:.1f}", hotThreshold, warmThreshold);
        }
        
        // 如果存储利用率过高，提高阈值，减少热存储使用
        if (storageUtilization > 0.85) {
            hotThreshold = Math.min(9.0, hotThreshold + 0.5);
            warmThreshold = Math.min(6.0, warmThreshold + 0.5);
            logger.info("存储利用率过高，提高存储阈值: hot={:.1f}, warm={:.1f}", hotThreshold, warmThreshold);
        }
        
        // 如果缓存命中率过低，调整策略
        if (cacheHitRate < 0.7) {
            hotThreshold = Math.max(5.0, hotThreshold - 1.0);
            logger.info("缓存命中率过低，降低热存储阈值: hot={:.1f}", hotThreshold);
        }
    }
    
    /**
     * 调整压缩策略
     */
    private void adjustCompressionStrategy(PerformanceMetrics performance) {
        double compressionRatio = performance.getCompressionRatio();
        double writeLatency = performance.getAvgWriteLatency();
        
        // 如果压缩比过低，提高压缩阈值
        if (compressionRatio < 1.2) { // 压缩比小于1.2
            compressionThreshold = Math.min(4096, compressionThreshold * 2); // 最大4KB
            logger.info("压缩比过低，提高压缩阈值: {:.0f} bytes", compressionThreshold);
        }
        
        // 如果写入延迟过高，可能是压缩导致的，提高压缩阈值
        if (writeLatency > 5.0) { // 5ms
            compressionThreshold = Math.min(4096, compressionThreshold * 1.5);
            logger.info("写入延迟过高，提高压缩阈值: {:.0f} bytes", compressionThreshold);
        }
        
        // 如果压缩效果很好，降低压缩阈值
        if (compressionRatio > 3.0) { // 压缩比大于3.0
            compressionThreshold = Math.max(512, compressionThreshold * 0.8); // 最小512字节
            logger.info("压缩效果良好，降低压缩阈值: {:.0f} bytes", compressionThreshold);
        }
    }
    
    /**
     * 获取存储策略统计信息
     */
    public StorageStrategyStats getStats() {
        return new StorageStrategyStats(hotThreshold, warmThreshold, compressionThreshold);
    }
    
    // ========== Getter和Setter方法 ==========
    
    public double getHotThreshold() {
        return hotThreshold;
    }
    
    public void setHotThreshold(double hotThreshold) {
        this.hotThreshold = hotThreshold;
    }
    
    public double getWarmThreshold() {
        return warmThreshold;
    }
    
    public void setWarmThreshold(double warmThreshold) {
        this.warmThreshold = warmThreshold;
    }
    
    public double getCompressionThreshold() {
        return compressionThreshold;
    }
    
    public void setCompressionThreshold(double compressionThreshold) {
        this.compressionThreshold = compressionThreshold;
    }
}

/**
 * 存储策略统计信息
 */
class StorageStrategyStats {
    private final double hotThreshold;
    private final double warmThreshold;
    private final double compressionThreshold;
    
    public StorageStrategyStats(double hotThreshold, double warmThreshold, double compressionThreshold) {
        this.hotThreshold = hotThreshold;
        this.warmThreshold = warmThreshold;
        this.compressionThreshold = compressionThreshold;
    }
    
    public double getHotThreshold() {
        return hotThreshold;
    }
    
    public double getWarmThreshold() {
        return warmThreshold;
    }
    
    public double getCompressionThreshold() {
        return compressionThreshold;
    }
    
    @Override
    public String toString() {
        return "StorageStrategyStats{" +
                "hotThreshold=" + hotThreshold +
                ", warmThreshold=" + warmThreshold +
                ", compressionThreshold=" + compressionThreshold +
                '}';
    }
}
