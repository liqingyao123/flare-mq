package com.ruyuan.mq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 智能存储管理器
 * 
 * 整合消息热度分析、自适应存储策略、存储层级迁移等智能特性
 * 
 * @author RuYuan MQ Team
 */
public class IntelligentStorageManager {
    
    private static final Logger logger = LoggerFactory.getLogger(IntelligentStorageManager.class);
    
    /**
     * 消息热度分析器
     */
    private final MessageHeatAnalyzer heatAnalyzer;
    
    /**
     * 自适应存储策略
     */
    private final AdaptiveStorageStrategy storageStrategy;
    
    /**
     * 定时任务执行器
     */
    private final ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(2);
    
    /**
     * 性能监控器
     */
    private final PerformanceMonitor performanceMonitor;
    
    /**
     * 是否已启动
     */
    private volatile boolean started = false;
    
    /**
     * 构造函数
     */
    public IntelligentStorageManager() {
        this.heatAnalyzer = new MessageHeatAnalyzer();
        this.storageStrategy = new DefaultAdaptiveStorageStrategy(heatAnalyzer);
        this.performanceMonitor = new PerformanceMonitor();
    }
    
    /**
     * 启动智能存储管理器
     */
    public void start() {
        if (started) {
            return;
        }
        
        // 启动热度分析器
        heatAnalyzer.start();
        
        // 启动性能监控
        performanceMonitor.start();
        
        // 启动存储层级迁移任务
        scheduledExecutor.scheduleAtFixedRate(
                this::performStorageMigration,
                StoreConstants.STORAGE_MIGRATION_CHECK_INTERVAL,
                StoreConstants.STORAGE_MIGRATION_CHECK_INTERVAL,
                TimeUnit.MILLISECONDS
        );
        
        // 启动策略调优任务
        scheduledExecutor.scheduleAtFixedRate(
                this::performStrategyOptimization,
                60 * 60 * 1000, // 1小时
                60 * 60 * 1000,
                TimeUnit.MILLISECONDS
        );
        
        started = true;
        logger.info("IntelligentStorageManager启动成功");
    }
    
    /**
     * 分析消息并选择存储策略
     */
    public StorageDecision analyzeMessage(Message message) {
        if (!started) {
            return new StorageDecision(StorageType.WARM_STORAGE, CompressionType.NONE, "服务未启动");
        }
        
        try {
            // 记录消息访问
            heatAnalyzer.recordMessage(message.getTopic(), message.getQueueId(), 
                                     message.getBody() != null ? message.getBody().length : 0);
            
            // 获取消息指标
            MessageMetrics metrics = heatAnalyzer.getMetrics(message.getTopic(), message.getQueueId());
            if (metrics == null) {
                metrics = new MessageMetrics(message.getTopic(), message.getQueueId());
            }
            
            // 分析消息模式
            MessagePattern pattern = new MessagePattern(message.getBody());
            
            // 选择存储类型
            StorageType storageType = storageStrategy.selectStorageType(metrics);
            
            // 选择压缩类型
            CompressionType compressionType = storageStrategy.selectCompression(pattern);
            
            String reason = String.format("热度评分: %.2f, 存储类型: %s, 压缩类型: %s", 
                                        heatAnalyzer.calculateHeatScore(metrics), storageType, compressionType);
            
            logger.debug("消息存储决策: topic={}, queueId={}, {}", 
                        message.getTopic(), message.getQueueId(), reason);
            
            return new StorageDecision(storageType, compressionType, reason);
            
        } catch (Exception e) {
            logger.error("分析消息存储策略异常", e);
            return new StorageDecision(StorageType.WARM_STORAGE, CompressionType.NONE, "分析异常，使用默认策略");
        }
    }
    
    /**
     * 记录消息访问
     */
    public void recordMessageAccess(String topic, int queueId) {
        if (started) {
            heatAnalyzer.recordAccess(topic, queueId);
        }
    }
    
    /**
     * 设置业务优先级
     */
    public void setBusinessPriority(String topic, int queueId, int priority) {
        if (started) {
            heatAnalyzer.setBusinessPriority(topic, queueId, priority);
        }
    }
    
    /**
     * 执行存储层级迁移
     */
    private void performStorageMigration() {
        try {
            logger.debug("开始执行存储层级迁移检查");
            
            int migrationCount = 0;
            
            // 检查所有消息指标
            for (MessageMetrics metrics : heatAnalyzer.getAllMetrics().values()) {
                // 这里应该检查当前存储类型并决定是否迁移
                // 简化实现，仅记录需要迁移的数量
                StorageType targetType = storageStrategy.getTargetStorageType(metrics);
                
                // 假设当前都是温存储，检查是否需要迁移
                if (storageStrategy.shouldMigrate(metrics, StorageType.WARM_STORAGE)) {
                    migrationCount++;
                    logger.debug("发现需要迁移的消息: topic={}, queueId={}, target={}", 
                               metrics.getTopic(), metrics.getQueueId(), targetType);
                }
            }
            
            if (migrationCount > 0) {
                logger.info("存储迁移检查完成，发现{}个需要迁移的队列", migrationCount);
            }
            
        } catch (Exception e) {
            logger.error("执行存储层级迁移异常", e);
        }
    }
    
    /**
     * 执行策略优化
     */
    private void performStrategyOptimization() {
        try {
            logger.debug("开始执行策略优化");
            
            // 获取性能指标
            PerformanceMetrics performance = performanceMonitor.getPerformanceMetrics();
            
            // 调整存储策略
            storageStrategy.adjustStrategy(performance);
            
            logger.info("策略优化完成: {}", performance);
            
        } catch (Exception e) {
            logger.error("执行策略优化异常", e);
        }
    }
    
    /**
     * 获取智能存储统计信息
     */
    public IntelligentStorageStats getStats() {
        if (!started) {
            return new IntelligentStorageStats();
        }
        
        try {
            // 统计各存储类型的消息数量
            int hotCount = 0, warmCount = 0, coldCount = 0, archiveCount = 0;
            
            for (MessageMetrics metrics : heatAnalyzer.getAllMetrics().values()) {
                StorageType storageType = storageStrategy.selectStorageType(metrics);
                switch (storageType) {
                    case HOT_STORAGE:
                        hotCount++;
                        break;
                    case WARM_STORAGE:
                        warmCount++;
                        break;
                    case COLD_STORAGE:
                        coldCount++;
                        break;
                    case ARCHIVE_STORAGE:
                        archiveCount++;
                        break;
                }
            }
            
            return new IntelligentStorageStats(hotCount, warmCount, coldCount, archiveCount,
                                             performanceMonitor.getPerformanceMetrics());
            
        } catch (Exception e) {
            logger.error("获取智能存储统计信息异常", e);
            return new IntelligentStorageStats();
        }
    }
    
    /**
     * 关闭智能存储管理器
     */
    public void shutdown() {
        if (!started) {
            return;
        }
        
        // 关闭定时任务
        scheduledExecutor.shutdown();
        try {
            if (!scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduledExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduledExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        // 关闭组件
        heatAnalyzer.shutdown();
        performanceMonitor.shutdown();
        
        started = false;
        logger.info("IntelligentStorageManager关闭完成");
    }
    
    // ========== Getter方法 ==========
    
    public MessageHeatAnalyzer getHeatAnalyzer() {
        return heatAnalyzer;
    }
    
    public AdaptiveStorageStrategy getStorageStrategy() {
        return storageStrategy;
    }
    
    public PerformanceMonitor getPerformanceMonitor() {
        return performanceMonitor;
    }
    
    public boolean isStarted() {
        return started;
    }
}

/**
 * 存储决策
 */
class StorageDecision {
    private final StorageType storageType;
    private final CompressionType compressionType;
    private final String reason;
    
    public StorageDecision(StorageType storageType, CompressionType compressionType, String reason) {
        this.storageType = storageType;
        this.compressionType = compressionType;
        this.reason = reason;
    }
    
    public StorageType getStorageType() {
        return storageType;
    }
    
    public CompressionType getCompressionType() {
        return compressionType;
    }
    
    public String getReason() {
        return reason;
    }
    
    @Override
    public String toString() {
        return "StorageDecision{" +
                "storageType=" + storageType +
                ", compressionType=" + compressionType +
                ", reason='" + reason + '\'' +
                '}';
    }
}

/**
 * 智能存储统计信息
 */
class IntelligentStorageStats {
    private final int hotStorageCount;
    private final int warmStorageCount;
    private final int coldStorageCount;
    private final int archiveStorageCount;
    private final PerformanceMetrics performanceMetrics;
    
    public IntelligentStorageStats() {
        this(0, 0, 0, 0, new PerformanceMetrics());
    }
    
    public IntelligentStorageStats(int hotStorageCount, int warmStorageCount, 
                                 int coldStorageCount, int archiveStorageCount,
                                 PerformanceMetrics performanceMetrics) {
        this.hotStorageCount = hotStorageCount;
        this.warmStorageCount = warmStorageCount;
        this.coldStorageCount = coldStorageCount;
        this.archiveStorageCount = archiveStorageCount;
        this.performanceMetrics = performanceMetrics;
    }
    
    public int getTotalCount() {
        return hotStorageCount + warmStorageCount + coldStorageCount + archiveStorageCount;
    }
    
    // ========== Getter方法 ==========
    
    public int getHotStorageCount() {
        return hotStorageCount;
    }
    
    public int getWarmStorageCount() {
        return warmStorageCount;
    }
    
    public int getColdStorageCount() {
        return coldStorageCount;
    }
    
    public int getArchiveStorageCount() {
        return archiveStorageCount;
    }
    
    public PerformanceMetrics getPerformanceMetrics() {
        return performanceMetrics;
    }
    
    @Override
    public String toString() {
        return "IntelligentStorageStats{" +
                "hotStorageCount=" + hotStorageCount +
                ", warmStorageCount=" + warmStorageCount +
                ", coldStorageCount=" + coldStorageCount +
                ", archiveStorageCount=" + archiveStorageCount +
                ", totalCount=" + getTotalCount() +
                ", performanceMetrics=" + performanceMetrics +
                '}';
    }
}
