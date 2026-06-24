package com.ruyuan.mq.broker.queue;

import com.ruyuan.mq.broker.topic.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

/**
 * Queue管理器
 * 
 * 负责Queue的创建、删除、查询等管理功能
 * 
 * @author RuYuan MQ Team
 */
public class QueueManager {
    
    private static final Logger logger = LoggerFactory.getLogger(QueueManager.class);
    
    /**
     * Queue配置缓存
     * Key: topicName:queueId, Value: QueueConfig
     */
    private final ConcurrentMap<String, QueueConfig> queueConfigTable = new ConcurrentHashMap<>();
    
    /**
     * Topic到Queue的映射
     * Key: topicName, Value: List<QueueConfig>
     */
    private final ConcurrentMap<String, List<QueueConfig>> topicQueueMapping = new ConcurrentHashMap<>();
    
    /**
     * 为Topic创建Queue
     */
    public boolean createQueuesForTopic(TopicConfig topicConfig) {
        if (topicConfig == null) {
            logger.warn("TopicConfig不能为空");
            return false;
        }
        
        String topicName = topicConfig.getTopicName();
        int queueCount = topicConfig.getQueueCount();
        
        try {
            List<QueueConfig> queueConfigs = new ArrayList<>();
            
            for (int queueId = 0; queueId < queueCount; queueId++) {
                String queueKey = buildQueueKey(topicName, queueId);
                
                // 检查Queue是否已存在
                if (queueConfigTable.containsKey(queueKey)) {
                    logger.warn("Queue已存在: {}", queueKey);
                    continue;
                }
                
                QueueConfig queueConfig = new QueueConfig(topicName, queueId);
                queueConfig.setMaxMessageSize(topicConfig.getMaxMessageSize());
                queueConfig.setMessageRetentionTime(topicConfig.getMessageRetentionTime());
                
                queueConfigTable.put(queueKey, queueConfig);
                queueConfigs.add(queueConfig);
            }
            
            // 更新Topic到Queue的映射
            topicQueueMapping.put(topicName, queueConfigs);
            
            logger.info("为Topic创建Queue成功: {}, 队列数量: {}", topicName, queueCount);
            return true;
            
        } catch (Exception e) {
            logger.error("为Topic创建Queue失败: " + topicName, e);
            return false;
        }
    }
    
    /**
     * 删除Topic的所有Queue
     */
    public boolean deleteQueuesForTopic(String topicName) {
        if (topicName == null || topicName.trim().isEmpty()) {
            logger.warn("Topic名称不能为空");
            return false;
        }
        
        List<QueueConfig> queueConfigs = topicQueueMapping.remove(topicName);
        if (queueConfigs == null || queueConfigs.isEmpty()) {
            logger.warn("Topic没有对应的Queue: {}", topicName);
            return false;
        }
        
        int deletedCount = 0;
        for (QueueConfig queueConfig : queueConfigs) {
            String queueKey = buildQueueKey(queueConfig.getTopicName(), queueConfig.getQueueId());
            if (queueConfigTable.remove(queueKey) != null) {
                deletedCount++;
            }
        }
        
        logger.info("删除Topic的Queue成功: {}, 删除数量: {}", topicName, deletedCount);
        return true;
    }
    
    /**
     * 获取Topic的所有Queue
     */
    public List<QueueConfig> getQueuesForTopic(String topicName) {
        List<QueueConfig> queueConfigs = topicQueueMapping.get(topicName);
        return queueConfigs != null ? new ArrayList<>(queueConfigs) : new ArrayList<>();
    }
    
    /**
     * 获取指定的Queue配置
     */
    public QueueConfig getQueueConfig(String topicName, int queueId) {
        String queueKey = buildQueueKey(topicName, queueId);
        return queueConfigTable.get(queueKey);
    }
    
    /**
     * 检查Queue是否存在
     */
    public boolean queueExists(String topicName, int queueId) {
        String queueKey = buildQueueKey(topicName, queueId);
        return queueConfigTable.containsKey(queueKey);
    }
    
    /**
     * 获取Topic的Queue数量
     */
    public int getQueueCountForTopic(String topicName) {
        List<QueueConfig> queueConfigs = topicQueueMapping.get(topicName);
        return queueConfigs != null ? queueConfigs.size() : 0;
    }
    
    /**
     * 更新Queue配置
     */
    public boolean updateQueueConfig(String topicName, int queueId, QueueConfig newConfig) {
        String queueKey = buildQueueKey(topicName, queueId);
        QueueConfig existingConfig = queueConfigTable.get(queueKey);
        
        if (existingConfig == null) {
            logger.warn("Queue不存在，无法更新: {}", queueKey);
            return false;
        }
        
        try {
            queueConfigTable.put(queueKey, newConfig);
            
            // 更新Topic映射中的配置
            List<QueueConfig> queueConfigs = topicQueueMapping.get(topicName);
            if (queueConfigs != null) {
                for (int i = 0; i < queueConfigs.size(); i++) {
                    if (queueConfigs.get(i).getQueueId() == queueId) {
                        queueConfigs.set(i, newConfig);
                        break;
                    }
                }
            }
            
            logger.info("更新Queue配置成功: {}", queueKey);
            return true;
            
        } catch (Exception e) {
            logger.error("更新Queue配置失败: " + queueKey, e);
            return false;
        }
    }
    
    /**
     * 获取所有Queue配置
     */
    public Map<String, QueueConfig> getAllQueueConfigs() {
        return new ConcurrentHashMap<>(queueConfigTable);
    }
    
    /**
     * 获取Queue统计信息
     */
    public QueueStats getQueueStats() {
        QueueStats stats = new QueueStats();
        stats.setTotalQueueCount(queueConfigTable.size());
        stats.setTopicCount(topicQueueMapping.size());
        
        // 计算平均每个Topic的Queue数量
        if (topicQueueMapping.size() > 0) {
            double avgQueuePerTopic = (double) queueConfigTable.size() / topicQueueMapping.size();
            stats.setAverageQueuePerTopic(avgQueuePerTopic);
        }
        
        return stats;
    }
    
    /**
     * 清空所有Queue
     */
    public void clearAllQueues() {
        int queueCount = queueConfigTable.size();
        int topicCount = topicQueueMapping.size();
        
        queueConfigTable.clear();
        topicQueueMapping.clear();
        
        logger.info("清空所有Queue，共删除: {} 个Queue，涉及 {} 个Topic", queueCount, topicCount);
    }
    
    /**
     * 获取Queue总数
     */
    public int getTotalQueueCount() {
        return queueConfigTable.size();
    }
    
    /**
     * 构建Queue的唯一标识
     */
    private String buildQueueKey(String topicName, int queueId) {
        return topicName + ":" + queueId;
    }
    
    /**
     * 验证Queue ID是否有效
     */
    public boolean isValidQueueId(int queueId) {
        return queueId >= 0;
    }
    
    /**
     * 获取Topic的最大Queue ID
     */
    public int getMaxQueueIdForTopic(String topicName) {
        List<QueueConfig> queueConfigs = topicQueueMapping.get(topicName);
        if (queueConfigs == null || queueConfigs.isEmpty()) {
            return -1;
        }
        
        return queueConfigs.stream()
                .mapToInt(QueueConfig::getQueueId)
                .max()
                .orElse(-1);
    }
    
    /**
     * 选择负载最低的Queue
     */
    public QueueConfig selectLeastLoadedQueue(String topicName) {
        List<QueueConfig> queueConfigs = topicQueueMapping.get(topicName);
        if (queueConfigs == null || queueConfigs.isEmpty()) {
            return null;
        }
        
        return queueConfigs.stream()
                .min((q1, q2) -> Long.compare(q1.getMessageCount(), q2.getMessageCount()))
                .orElse(null);
    }
}
