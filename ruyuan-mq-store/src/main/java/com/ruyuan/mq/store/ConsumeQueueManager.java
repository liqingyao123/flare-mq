package com.ruyuan.mq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * ConsumeQueue管理器
 * 
 * 负责管理所有Topic和QueueId对应的ConsumeQueue
 * 
 * @author RuYuan MQ Team
 */
public class ConsumeQueueManager {
    
    private static final Logger logger = LoggerFactory.getLogger(ConsumeQueueManager.class);
    
    /**
     * 存储路径
     */
    private final String storePath;
    
    /**
     * ConsumeQueue映射表
     * key: topic + "-" + queueId
     * value: ConsumeQueue
     */
    private final ConcurrentMap<String, ConsumeQueue> consumeQueueTable = new ConcurrentHashMap<>();
    
    /**
     * 构造函数
     */
    public ConsumeQueueManager(String storePath) {
        this.storePath = storePath;
        logger.info("ConsumeQueueManager初始化完成: storePath={}", storePath);
    }
    
    /**
     * 获取或创建ConsumeQueue
     */
    public ConsumeQueue getOrCreateConsumeQueue(String topic, int queueId) {
        String key = buildKey(topic, queueId);
        
        ConsumeQueue consumeQueue = consumeQueueTable.get(key);
        if (consumeQueue == null) {
            ConsumeQueue newConsumeQueue = new ConsumeQueue(topic, queueId, storePath);
            ConsumeQueue existingConsumeQueue = consumeQueueTable.putIfAbsent(key, newConsumeQueue);
            
            if (existingConsumeQueue != null) {
                // 已存在，关闭新创建的
                newConsumeQueue.shutdown();
                consumeQueue = existingConsumeQueue;
            } else {
                consumeQueue = newConsumeQueue;
                logger.info("创建新的ConsumeQueue: topic={}, queueId={}", topic, queueId);
            }
        }
        
        return consumeQueue;
    }
    
    /**
     * 获取ConsumeQueue
     */
    public ConsumeQueue getConsumeQueue(String topic, int queueId) {
        String key = buildKey(topic, queueId);
        return consumeQueueTable.get(key);
    }
    
    /**
     * 添加消息索引
     */
    public boolean putMessageIndex(String topic, int queueId, long commitLogOffset, int size, long tagsHashCode) {
        ConsumeQueue consumeQueue = getOrCreateConsumeQueue(topic, queueId);
        return consumeQueue.putMessageIndex(commitLogOffset, size, tagsHashCode);
    }
    
    /**
     * 根据逻辑偏移量获取ConsumeQueue单元
     */
    public ConsumeQueueUnit getConsumeQueueUnit(String topic, int queueId, long offset) {
        ConsumeQueue consumeQueue = getConsumeQueue(topic, queueId);
        if (consumeQueue == null) {
            logger.debug("ConsumeQueue不存在: topic={}, queueId={}", topic, queueId);
            return null;
        }
        
        return consumeQueue.getConsumeQueueUnit(offset);
    }
    
    /**
     * 获取指定范围的ConsumeQueue单元
     */
    public List<ConsumeQueueUnit> getConsumeQueueUnits(String topic, int queueId, long startOffset, int maxCount) {
        ConsumeQueue consumeQueue = getConsumeQueue(topic, queueId);
        if (consumeQueue == null) {
            logger.debug("ConsumeQueue不存在: topic={}, queueId={}", topic, queueId);
            return null;
        }
        
        return consumeQueue.getConsumeQueueUnits(startOffset, maxCount);
    }
    
    /**
     * 根据Tags过滤获取ConsumeQueue单元
     */
    public List<ConsumeQueueUnit> getConsumeQueueUnitsByTags(String topic, int queueId, 
                                                            long startOffset, int maxCount, String tags) {
        ConsumeQueue consumeQueue = getConsumeQueue(topic, queueId);
        if (consumeQueue == null) {
            logger.debug("ConsumeQueue不存在: topic={}, queueId={}", topic, queueId);
            return null;
        }
        
        return consumeQueue.getConsumeQueueUnitsByTags(startOffset, maxCount, tags);
    }
    
    /**
     * 获取队列的最大偏移量
     */
    public long getMaxOffset(String topic, int queueId) {
        ConsumeQueue consumeQueue = getConsumeQueue(topic, queueId);
        if (consumeQueue == null) {
            return 0;
        }
        
        return consumeQueue.getMaxOffset();
    }
    
    /**
     * 获取队列的最小偏移量
     */
    public long getMinOffset(String topic, int queueId) {
        // 简化实现，返回0
        // 实际实现中需要考虑文件清理后的最小偏移量
        return 0;
    }
    
    /**
     * 刷盘所有ConsumeQueue
     */
    public void flushAll() {
        for (ConsumeQueue consumeQueue : consumeQueueTable.values()) {
            try {
                consumeQueue.flush();
            } catch (Exception e) {
                logger.error("刷盘ConsumeQueue失败: topic={}, queueId={}", 
                           consumeQueue.getTopic(), consumeQueue.getQueueId(), e);
            }
        }
    }
    
    /**
     * 刷盘指定的ConsumeQueue
     */
    public void flush(String topic, int queueId) {
        ConsumeQueue consumeQueue = getConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            consumeQueue.flush();
        }
    }
    
    /**
     * 删除Topic的所有ConsumeQueue
     */
    public void deleteTopic(String topic) {
        // 找到所有相关的ConsumeQueue
        consumeQueueTable.entrySet().removeIf(entry -> {
            String key = entry.getKey();
            ConsumeQueue consumeQueue = entry.getValue();
            
            if (consumeQueue.getTopic().equals(topic)) {
                consumeQueue.shutdown();
                logger.info("删除ConsumeQueue: topic={}, queueId={}", 
                           consumeQueue.getTopic(), consumeQueue.getQueueId());
                return true;
            }
            return false;
        });
    }
    
    /**
     * 获取Topic的所有队列数量
     */
    public int getQueueCount(String topic) {
        int count = 0;
        for (ConsumeQueue consumeQueue : consumeQueueTable.values()) {
            if (consumeQueue.getTopic().equals(topic)) {
                count++;
            }
        }
        return count;
    }
    
    /**
     * 获取所有ConsumeQueue的统计信息
     */
    public ConsumeQueueStats getStats() {
        ConsumeQueueStats stats = new ConsumeQueueStats();
        
        for (ConsumeQueue consumeQueue : consumeQueueTable.values()) {
            stats.addQueue(consumeQueue.getTopic(), consumeQueue.getQueueId(), 
                          consumeQueue.getMaxOffset(), consumeQueue.getMappedFileCount());
        }
        
        return stats;
    }
    
    /**
     * 关闭所有ConsumeQueue
     */
    public void shutdown() {
        logger.info("开始关闭ConsumeQueueManager...");
        
        for (ConsumeQueue consumeQueue : consumeQueueTable.values()) {
            try {
                consumeQueue.shutdown();
            } catch (Exception e) {
                logger.error("关闭ConsumeQueue失败: topic={}, queueId={}", 
                           consumeQueue.getTopic(), consumeQueue.getQueueId(), e);
            }
        }
        
        consumeQueueTable.clear();
        logger.info("ConsumeQueueManager关闭完成");
    }
    
    /**
     * 构建ConsumeQueue的key
     */
    private String buildKey(String topic, int queueId) {
        return topic + "-" + queueId;
    }
    
    // ========== Getter方法 ==========
    
    public String getStorePath() {
        return storePath;
    }
    
    public int getConsumeQueueCount() {
        return consumeQueueTable.size();
    }
}

/**
 * ConsumeQueue统计信息
 */
class ConsumeQueueStats {
    private int totalQueues = 0;
    private long totalMessages = 0;
    private int totalFiles = 0;
    
    public void addQueue(String topic, int queueId, long maxOffset, int fileCount) {
        totalQueues++;
        totalMessages += maxOffset;
        totalFiles += fileCount;
    }
    
    public int getTotalQueues() {
        return totalQueues;
    }
    
    public long getTotalMessages() {
        return totalMessages;
    }
    
    public int getTotalFiles() {
        return totalFiles;
    }
    
    @Override
    public String toString() {
        return "ConsumeQueueStats{" +
                "totalQueues=" + totalQueues +
                ", totalMessages=" + totalMessages +
                ", totalFiles=" + totalFiles +
                '}';
    }
}
