package com.ruyuan.mq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 性能监控器
 * 
 * 监控存储系统的性能指标，包括延迟、吞吐量、存储利用率等
 * 
 * @author RuYuan MQ Team
 */
public class PerformanceMonitor {
    
    private static final Logger logger = LoggerFactory.getLogger(PerformanceMonitor.class);
    
    /**
     * 读取操作计数
     */
    private final LongAdder readCount = new LongAdder();
    
    /**
     * 写入操作计数
     */
    private final LongAdder writeCount = new LongAdder();
    
    /**
     * 读取总延迟（纳秒）
     */
    private final LongAdder totalReadLatency = new LongAdder();
    
    /**
     * 写入总延迟（纳秒）
     */
    private final LongAdder totalWriteLatency = new LongAdder();
    
    /**
     * 缓存命中次数
     */
    private final LongAdder cacheHits = new LongAdder();
    
    /**
     * 缓存访问次数
     */
    private final LongAdder cacheAccess = new LongAdder();
    
    /**
     * 压缩前总大小
     */
    private final LongAdder totalUncompressedSize = new LongAdder();
    
    /**
     * 压缩后总大小
     */
    private final LongAdder totalCompressedSize = new LongAdder();
    
    /**
     * 最后一次性能指标
     */
    private volatile PerformanceMetrics lastMetrics = new PerformanceMetrics();
    
    /**
     * 定时任务执行器
     */
    private final ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);
    
    /**
     * 是否已启动
     */
    private volatile boolean started = false;
    
    /**
     * 启动性能监控
     */
    public void start() {
        if (started) {
            return;
        }
        
        // 定期计算性能指标
        scheduledExecutor.scheduleAtFixedRate(
                this::calculateMetrics,
                60000, // 1分钟
                60000,
                TimeUnit.MILLISECONDS
        );
        
        started = true;
        logger.info("PerformanceMonitor启动成功");
    }
    
    /**
     * 记录读取操作
     */
    public void recordRead(long latencyNanos) {
        readCount.increment();
        totalReadLatency.add(latencyNanos);
    }
    
    /**
     * 记录写入操作
     */
    public void recordWrite(long latencyNanos) {
        writeCount.increment();
        totalWriteLatency.add(latencyNanos);
    }
    
    /**
     * 记录缓存命中
     */
    public void recordCacheHit() {
        cacheHits.increment();
        cacheAccess.increment();
    }
    
    /**
     * 记录缓存未命中
     */
    public void recordCacheMiss() {
        cacheAccess.increment();
    }
    
    /**
     * 记录压缩操作
     */
    public void recordCompression(long uncompressedSize, long compressedSize) {
        totalUncompressedSize.add(uncompressedSize);
        totalCompressedSize.add(compressedSize);
    }
    
    /**
     * 获取当前性能指标
     */
    public PerformanceMetrics getPerformanceMetrics() {
        return lastMetrics;
    }
    
    /**
     * 计算性能指标
     */
    private void calculateMetrics() {
        try {
            long reads = readCount.sum();
            long writes = writeCount.sum();
            long readLatency = totalReadLatency.sum();
            long writeLatency = totalWriteLatency.sum();
            long hits = cacheHits.sum();
            long access = cacheAccess.sum();
            long uncompressed = totalUncompressedSize.sum();
            long compressed = totalCompressedSize.sum();
            
            // 计算平均延迟（转换为毫秒）
            double avgReadLatency = reads > 0 ? (double) readLatency / reads / 1_000_000 : 0.0;
            double avgWriteLatency = writes > 0 ? (double) writeLatency / writes / 1_000_000 : 0.0;
            
            // 计算缓存命中率
            double cacheHitRate = access > 0 ? (double) hits / access : 0.0;
            
            // 计算压缩比
            double compressionRatio = compressed > 0 ? (double) uncompressed / compressed : 1.0;
            
            // 计算存储利用率（简化实现，实际应该从文件系统获取）
            double storageUtilization = calculateStorageUtilization();
            
            // 更新性能指标
            lastMetrics = new PerformanceMetrics(avgReadLatency, avgWriteLatency, 
                                               storageUtilization, cacheHitRate, compressionRatio);
            
            logger.debug("性能指标更新: {}", lastMetrics);
            
        } catch (Exception e) {
            logger.error("计算性能指标异常", e);
        }
    }
    
    /**
     * 计算存储利用率
     */
    private double calculateStorageUtilization() {
        try {
            // 简化实现：模拟存储利用率
            // 实际实现应该从文件系统获取真实的存储使用情况
            Runtime runtime = Runtime.getRuntime();
            long totalMemory = runtime.totalMemory();
            long freeMemory = runtime.freeMemory();
            long usedMemory = totalMemory - freeMemory;
            
            return (double) usedMemory / totalMemory;
            
        } catch (Exception e) {
            logger.warn("计算存储利用率异常", e);
            return 0.5; // 默认50%
        }
    }
    
    /**
     * 重置统计数据
     */
    public void reset() {
        readCount.reset();
        writeCount.reset();
        totalReadLatency.reset();
        totalWriteLatency.reset();
        cacheHits.reset();
        cacheAccess.reset();
        totalUncompressedSize.reset();
        totalCompressedSize.reset();
        
        logger.info("性能监控统计数据已重置");
    }
    
    /**
     * 获取详细统计信息
     */
    public PerformanceStats getDetailedStats() {
        return new PerformanceStats(
                readCount.sum(),
                writeCount.sum(),
                totalReadLatency.sum(),
                totalWriteLatency.sum(),
                cacheHits.sum(),
                cacheAccess.sum(),
                totalUncompressedSize.sum(),
                totalCompressedSize.sum(),
                lastMetrics
        );
    }
    
    /**
     * 关闭性能监控
     */
    public void shutdown() {
        if (!started) {
            return;
        }
        
        scheduledExecutor.shutdown();
        try {
            if (!scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduledExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduledExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        started = false;
        logger.info("PerformanceMonitor关闭完成");
    }
    
    // ========== Getter方法 ==========
    
    public boolean isStarted() {
        return started;
    }
}

/**
 * 详细性能统计信息
 */
class PerformanceStats {
    private final long totalReads;
    private final long totalWrites;
    private final long totalReadLatency;
    private final long totalWriteLatency;
    private final long totalCacheHits;
    private final long totalCacheAccess;
    private final long totalUncompressedSize;
    private final long totalCompressedSize;
    private final PerformanceMetrics currentMetrics;
    
    public PerformanceStats(long totalReads, long totalWrites, long totalReadLatency, 
                          long totalWriteLatency, long totalCacheHits, long totalCacheAccess,
                          long totalUncompressedSize, long totalCompressedSize,
                          PerformanceMetrics currentMetrics) {
        this.totalReads = totalReads;
        this.totalWrites = totalWrites;
        this.totalReadLatency = totalReadLatency;
        this.totalWriteLatency = totalWriteLatency;
        this.totalCacheHits = totalCacheHits;
        this.totalCacheAccess = totalCacheAccess;
        this.totalUncompressedSize = totalUncompressedSize;
        this.totalCompressedSize = totalCompressedSize;
        this.currentMetrics = currentMetrics;
    }
    
    // ========== Getter方法 ==========
    
    public long getTotalReads() {
        return totalReads;
    }
    
    public long getTotalWrites() {
        return totalWrites;
    }
    
    public long getTotalReadLatency() {
        return totalReadLatency;
    }
    
    public long getTotalWriteLatency() {
        return totalWriteLatency;
    }
    
    public long getTotalCacheHits() {
        return totalCacheHits;
    }
    
    public long getTotalCacheAccess() {
        return totalCacheAccess;
    }
    
    public long getTotalUncompressedSize() {
        return totalUncompressedSize;
    }
    
    public long getTotalCompressedSize() {
        return totalCompressedSize;
    }
    
    public PerformanceMetrics getCurrentMetrics() {
        return currentMetrics;
    }
    
    @Override
    public String toString() {
        return "PerformanceStats{" +
                "totalReads=" + totalReads +
                ", totalWrites=" + totalWrites +
                ", totalCacheHits=" + totalCacheHits +
                ", totalCacheAccess=" + totalCacheAccess +
                ", currentMetrics=" + currentMetrics +
                '}';
    }
}
