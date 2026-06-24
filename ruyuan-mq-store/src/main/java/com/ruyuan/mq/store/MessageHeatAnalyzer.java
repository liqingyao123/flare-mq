package com.ruyuan.mq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 消息热度分析器
 * 
 * 基于访问频率、时间衰减、业务优先级的综合评分
 * 
 * @author RuYuan MQ Team
 */
public class MessageHeatAnalyzer {
    
    private static final Logger logger = LoggerFactory.getLogger(MessageHeatAnalyzer.class);
    
    /**
     * Topic和Queue的指标映射表
     * key: topic + "-" + queueId
     */
    private final ConcurrentMap<String, MessageMetrics> metricsTable = new ConcurrentHashMap<>();
    
    /**
     * 定时任务执行器
     */
    private final ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);
    
    /**
     * 是否已启动
     */
    private volatile boolean started = false;
    
    /**
     * 启动热度分析器
     */
    public void start() {
        if (started) {
            return;
        }
        
        // 定期更新热度评分
        scheduledExecutor.scheduleAtFixedRate(
                this::updateHeatScores,
                StoreConstants.HEAT_SCORE_UPDATE_INTERVAL,
                StoreConstants.HEAT_SCORE_UPDATE_INTERVAL,
                TimeUnit.MILLISECONDS
        );
        
        // 定期清理过期指标
        scheduledExecutor.scheduleAtFixedRate(
                this::cleanupExpiredMetrics,
                60 * 60 * 1000, // 1小时
                60 * 60 * 1000,
                TimeUnit.MILLISECONDS
        );
        
        started = true;
        logger.info("MessageHeatAnalyzer启动成功");
    }
    
    /**
     * 记录消息访问
     */
    public void recordAccess(String topic, int queueId) {
        String key = buildKey(topic, queueId);
        MessageMetrics metrics = metricsTable.computeIfAbsent(key, k -> new MessageMetrics(topic, queueId));
        metrics.updateAccess();
        
        logger.debug("记录访问: topic={}, queueId={}, accessCount={}", 
                    topic, queueId, metrics.getAccessCount());
    }
    
    /**
     * 记录消息写入
     */
    public void recordMessage(String topic, int queueId, int messageSize) {
        String key = buildKey(topic, queueId);
        MessageMetrics metrics = metricsTable.computeIfAbsent(key, k -> new MessageMetrics(topic, queueId));
        metrics.updateMessageStats(messageSize);
        
        logger.debug("记录消息: topic={}, queueId={}, totalMessages={}, avgSize={}", 
                    topic, queueId, metrics.getTotalMessages(), metrics.getAvgMessageSize());
    }
    
    /**
     * 设置业务优先级
     */
    public void setBusinessPriority(String topic, int queueId, int priority) {
        String key = buildKey(topic, queueId);
        MessageMetrics metrics = metricsTable.computeIfAbsent(key, k -> new MessageMetrics(topic, queueId));
        metrics.setBusinessPriority(priority);
        
        logger.info("设置业务优先级: topic={}, queueId={}, priority={}", topic, queueId, priority);
    }
    
    /**
     * 分析热度等级
     */
    public HeatLevel analyzeHeat(String topic, int queueId) {
        double heatScore = calculateHeatScore(topic, queueId);
        
        if (heatScore >= StoreConstants.HOT_STORAGE_THRESHOLD) {
            return HeatLevel.HOT;
        } else if (heatScore >= StoreConstants.WARM_STORAGE_THRESHOLD) {
            return HeatLevel.WARM;
        } else {
            return HeatLevel.COLD;
        }
    }
    
    /**
     * 计算热度评分
     * 
     * 热度评分 = 访问频率权重 * 时间衰减权重 * 业务优先级权重
     * 评分范围：0-10
     */
    public double calculateHeatScore(String topic, int queueId) {
        String key = buildKey(topic, queueId);
        MessageMetrics metrics = metricsTable.get(key);
        
        if (metrics == null) {
            return 0.0; // 没有访问记录，热度为0
        }
        
        return calculateHeatScore(metrics);
    }
    
    /**
     * 计算热度评分
     */
    public double calculateHeatScore(MessageMetrics metrics) {
        // 1. 访问频率评分 (0-10)
        double frequencyScore = calculateFrequencyScore(metrics);
        
        // 2. 时间衰减评分 (0-1)
        double timeDecayScore = calculateTimeDecayScore(metrics);
        
        // 3. 业务优先级评分 (0-1)
        double priorityScore = metrics.getBusinessPriority() / 10.0;
        
        // 4. 消息量评分 (0-1)
        double volumeScore = calculateVolumeScore(metrics);
        
        // 综合评分：频率50% + 时间衰减20% + 业务优先级20% + 消息量10%
        double heatScore = frequencyScore * 0.5 + 
                          (frequencyScore * timeDecayScore) * 0.2 + 
                          (frequencyScore * priorityScore) * 0.2 + 
                          (frequencyScore * volumeScore) * 0.1;
        
        // 确保评分在0-10范围内
        return Math.max(0.0, Math.min(10.0, heatScore));
    }
    
    /**
     * 计算访问频率评分
     */
    private double calculateFrequencyScore(MessageMetrics metrics) {
        double frequency = metrics.getAccessFrequency();
        
        // 使用对数函数，避免评分过高
        if (frequency <= 0) {
            return 0.0;
        } else if (frequency <= 1) {
            return frequency * 2; // 0-2分
        } else if (frequency <= 10) {
            return 2 + Math.log10(frequency) * 2; // 2-4分
        } else if (frequency <= 100) {
            return 4 + Math.log10(frequency / 10) * 3; // 4-7分
        } else {
            return 7 + Math.log10(frequency / 100) * 3; // 7-10分
        }
    }
    
    /**
     * 计算时间衰减评分
     */
    private double calculateTimeDecayScore(MessageMetrics metrics) {
        double lastAccessHours = metrics.getLastAccessHours();
        
        // 时间衰减函数：最近访问的权重更高
        if (lastAccessHours <= 1) {
            return 1.0; // 1小时内访问，满分
        } else if (lastAccessHours <= 24) {
            return Math.exp(-lastAccessHours / 24.0); // 24小时内指数衰减
        } else {
            double days = lastAccessHours / 24.0;
            return Math.exp(-days * 0.1); // 超过1天，缓慢衰减
        }
    }
    
    /**
     * 计算消息量评分
     */
    private double calculateVolumeScore(MessageMetrics metrics) {
        long totalMessages = metrics.getTotalMessages();
        
        if (totalMessages <= 0) {
            return 0.0;
        } else if (totalMessages <= 100) {
            return totalMessages / 100.0; // 0-1分
        } else {
            return 1.0; // 超过100条消息，满分
        }
    }
    
    /**
     * 获取指标信息
     */
    public MessageMetrics getMetrics(String topic, int queueId) {
        String key = buildKey(topic, queueId);
        return metricsTable.get(key);
    }
    
    /**
     * 获取所有指标
     */
    public ConcurrentMap<String, MessageMetrics> getAllMetrics() {
        return new ConcurrentHashMap<>(metricsTable);
    }
    
    /**
     * 定期更新热度评分
     */
    private void updateHeatScores() {
        try {
            int hotCount = 0, warmCount = 0, coldCount = 0;
            
            for (MessageMetrics metrics : metricsTable.values()) {
                double heatScore = calculateHeatScore(metrics);
                HeatLevel heatLevel = analyzeHeat(metrics.getTopic(), metrics.getQueueId());
                
                switch (heatLevel) {
                    case HOT:
                        hotCount++;
                        break;
                    case WARM:
                        warmCount++;
                        break;
                    case COLD:
                        coldCount++;
                        break;
                }
                
                logger.debug("热度更新: topic={}, queueId={}, score={:.2f}, level={}", 
                           metrics.getTopic(), metrics.getQueueId(), heatScore, heatLevel);
            }
            
            logger.info("热度统计更新: 热存储={}, 温存储={}, 冷存储={}, 总计={}", 
                       hotCount, warmCount, coldCount, metricsTable.size());
            
        } catch (Exception e) {
            logger.error("更新热度评分异常", e);
        }
    }
    
    /**
     * 清理过期指标
     */
    private void cleanupExpiredMetrics() {
        try {
            long currentTime = System.currentTimeMillis();
            int removedCount = 0;
            
            metricsTable.entrySet().removeIf(entry -> {
                MessageMetrics metrics = entry.getValue();
                // 清理7天未访问的指标
                long lastAccessTime = metrics.getLastAccessTime();
                boolean shouldRemove = (currentTime - lastAccessTime) > (7 * 24 * 60 * 60 * 1000L);
                
                if (shouldRemove) {
                    logger.debug("清理过期指标: topic={}, queueId={}, lastAccess={}", 
                               metrics.getTopic(), metrics.getQueueId(), 
                               (currentTime - lastAccessTime) / (24 * 60 * 60 * 1000L) + "天前");
                }
                
                return shouldRemove;
            });
            
            if (removedCount > 0) {
                logger.info("清理过期指标完成，删除{}个指标", removedCount);
            }
            
        } catch (Exception e) {
            logger.error("清理过期指标异常", e);
        }
    }
    
    /**
     * 关闭热度分析器
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
        logger.info("MessageHeatAnalyzer关闭完成");
    }
    
    /**
     * 构建key
     */
    private String buildKey(String topic, int queueId) {
        return topic + "-" + queueId;
    }
}

/**
 * 热度等级枚举
 */
enum HeatLevel {
    /**
     * 热存储：频繁访问的消息
     */
    HOT,
    
    /**
     * 温存储：中等访问频率的消息
     */
    WARM,
    
    /**
     * 冷存储：很少访问的消息
     */
    COLD
}
