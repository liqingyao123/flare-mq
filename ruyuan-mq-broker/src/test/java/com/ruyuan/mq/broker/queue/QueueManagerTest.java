package com.ruyuan.mq.broker.queue;

import com.ruyuan.mq.broker.topic.TopicConfig;
import com.ruyuan.mq.broker.topic.TopicPermission;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * QueueManager测试类
 * 
 * @author RuYuan MQ Team
 */
@DisplayName("Queue管理器测试")
class QueueManagerTest {
    
    private QueueManager queueManager;
    
    @BeforeEach
    void setUp() {
        queueManager = new QueueManager();
    }
    
    @Test
    @DisplayName("为Topic创建Queue - 正常情况")
    void testCreateQueuesForTopic_Success() {
        // 创建Topic配置
        TopicConfig topicConfig = new TopicConfig("test-topic", 4, TopicPermission.READ_WRITE);
        
        // 为Topic创建Queue
        boolean result = queueManager.createQueuesForTopic(topicConfig);
        assertTrue(result, "为Topic创建Queue应该成功");
        
        // 验证Queue数量
        assertEquals(4, queueManager.getQueueCountForTopic("test-topic"));
        
        // 验证每个Queue都存在
        for (int i = 0; i < 4; i++) {
            assertTrue(queueManager.queueExists("test-topic", i));
            
            QueueConfig queueConfig = queueManager.getQueueConfig("test-topic", i);
            assertNotNull(queueConfig);
            assertEquals("test-topic", queueConfig.getTopicName());
            assertEquals(i, queueConfig.getQueueId());
            assertEquals(QueueStatus.ACTIVE, queueConfig.getStatus());
        }
    }
    
    @Test
    @DisplayName("为Topic创建Queue - 参数验证")
    void testCreateQueuesForTopic_Validation() {
        // 测试空TopicConfig
        assertFalse(queueManager.createQueuesForTopic(null));
    }
    
    @Test
    @DisplayName("删除Topic的所有Queue")
    void testDeleteQueuesForTopic() {
        // 先创建Queue
        TopicConfig topicConfig = new TopicConfig("delete-topic", 4, TopicPermission.READ_WRITE);
        queueManager.createQueuesForTopic(topicConfig);
        assertEquals(4, queueManager.getQueueCountForTopic("delete-topic"));
        
        // 删除Queue
        boolean result = queueManager.deleteQueuesForTopic("delete-topic");
        assertTrue(result, "删除Topic的Queue应该成功");
        assertEquals(0, queueManager.getQueueCountForTopic("delete-topic"));
        
        // 验证Queue不存在
        for (int i = 0; i < 4; i++) {
            assertFalse(queueManager.queueExists("delete-topic", i));
        }
        
        // 删除不存在的Topic
        assertFalse(queueManager.deleteQueuesForTopic("non-existent-topic"));
    }
    
    @Test
    @DisplayName("获取Topic的所有Queue")
    void testGetQueuesForTopic() {
        // 创建Queue
        TopicConfig topicConfig = new TopicConfig("queue-topic", 3, TopicPermission.READ_WRITE);
        queueManager.createQueuesForTopic(topicConfig);
        
        // 获取Queue列表
        List<QueueConfig> queues = queueManager.getQueuesForTopic("queue-topic");
        assertEquals(3, queues.size());
        
        // 验证Queue ID
        for (int i = 0; i < 3; i++) {
            QueueConfig queue = queues.get(i);
            assertEquals("queue-topic", queue.getTopicName());
            assertEquals(i, queue.getQueueId());
        }
        
        // 获取不存在Topic的Queue
        List<QueueConfig> emptyQueues = queueManager.getQueuesForTopic("non-existent");
        assertTrue(emptyQueues.isEmpty());
    }
    
    @Test
    @DisplayName("获取指定Queue配置")
    void testGetQueueConfig() {
        // 创建Queue
        TopicConfig topicConfig = new TopicConfig("config-topic", 2, TopicPermission.READ_WRITE);
        queueManager.createQueuesForTopic(topicConfig);
        
        // 获取指定Queue配置
        QueueConfig queueConfig = queueManager.getQueueConfig("config-topic", 1);
        assertNotNull(queueConfig);
        assertEquals("config-topic", queueConfig.getTopicName());
        assertEquals(1, queueConfig.getQueueId());
        
        // 获取不存在的Queue配置
        assertNull(queueManager.getQueueConfig("config-topic", 5));
        assertNull(queueManager.getQueueConfig("non-existent", 0));
    }
    
    @Test
    @DisplayName("更新Queue配置")
    void testUpdateQueueConfig() {
        // 创建Queue
        TopicConfig topicConfig = new TopicConfig("update-topic", 2, TopicPermission.READ_WRITE);
        queueManager.createQueuesForTopic(topicConfig);
        
        // 创建新的Queue配置
        QueueConfig newConfig = new QueueConfig("update-topic", 0);
        newConfig.setEnabled(false);
        newConfig.setStatus(QueueStatus.PAUSED);
        newConfig.setDescription("Updated queue");
        
        // 更新配置
        boolean result = queueManager.updateQueueConfig("update-topic", 0, newConfig);
        assertTrue(result, "更新Queue配置应该成功");
        
        // 验证配置已更新
        QueueConfig updatedConfig = queueManager.getQueueConfig("update-topic", 0);
        assertFalse(updatedConfig.isEnabled());
        assertEquals(QueueStatus.PAUSED, updatedConfig.getStatus());
        assertEquals("Updated queue", updatedConfig.getDescription());
        
        // 更新不存在的Queue
        assertFalse(queueManager.updateQueueConfig("non-existent", 0, newConfig));
    }
    
    @Test
    @DisplayName("获取Queue统计信息")
    void testGetQueueStats() {
        // 初始统计
        QueueStats stats = queueManager.getQueueStats();
        assertEquals(0, stats.getTotalQueueCount());
        assertEquals(0, stats.getTopicCount());
        
        // 创建Queue后的统计
        TopicConfig topic1 = new TopicConfig("stats-topic1", 4, TopicPermission.READ_WRITE);
        TopicConfig topic2 = new TopicConfig("stats-topic2", 6, TopicPermission.READ_WRITE);
        queueManager.createQueuesForTopic(topic1);
        queueManager.createQueuesForTopic(topic2);
        
        stats = queueManager.getQueueStats();
        assertEquals(10, stats.getTotalQueueCount()); // 4 + 6
        assertEquals(2, stats.getTopicCount());
        assertEquals(5.0, stats.getAverageQueuePerTopic(), 0.01); // (4 + 6) / 2
    }
    
    @Test
    @DisplayName("获取Topic的最大Queue ID")
    void testGetMaxQueueIdForTopic() {
        // 创建Queue
        TopicConfig topicConfig = new TopicConfig("max-topic", 5, TopicPermission.READ_WRITE);
        queueManager.createQueuesForTopic(topicConfig);
        
        // 获取最大Queue ID
        int maxQueueId = queueManager.getMaxQueueIdForTopic("max-topic");
        assertEquals(4, maxQueueId); // Queue ID从0开始，所以最大是4
        
        // 不存在的Topic
        assertEquals(-1, queueManager.getMaxQueueIdForTopic("non-existent"));
    }
    
    @Test
    @DisplayName("选择负载最低的Queue")
    void testSelectLeastLoadedQueue() {
        // 创建Queue
        TopicConfig topicConfig = new TopicConfig("load-topic", 3, TopicPermission.READ_WRITE);
        queueManager.createQueuesForTopic(topicConfig);
        
        // 模拟不同的消息数量
        QueueConfig queue0 = queueManager.getQueueConfig("load-topic", 0);
        QueueConfig queue1 = queueManager.getQueueConfig("load-topic", 1);
        QueueConfig queue2 = queueManager.getQueueConfig("load-topic", 2);
        
        // 增加消息计数
        queue0.incrementMessageCount();
        queue0.incrementMessageCount();
        queue1.incrementMessageCount();
        // queue2保持0
        
        // 选择负载最低的Queue
        QueueConfig leastLoaded = queueManager.selectLeastLoadedQueue("load-topic");
        assertNotNull(leastLoaded);
        assertEquals(2, leastLoaded.getQueueId()); // queue2应该是负载最低的
        
        // 不存在的Topic
        assertNull(queueManager.selectLeastLoadedQueue("non-existent"));
    }
    
    @Test
    @DisplayName("Queue ID验证")
    void testIsValidQueueId() {
        assertTrue(queueManager.isValidQueueId(0));
        assertTrue(queueManager.isValidQueueId(1));
        assertTrue(queueManager.isValidQueueId(100));
        
        assertFalse(queueManager.isValidQueueId(-1));
        assertFalse(queueManager.isValidQueueId(-100));
    }
    
    @Test
    @DisplayName("清空所有Queue")
    void testClearAllQueues() {
        // 创建一些Queue
        TopicConfig topic1 = new TopicConfig("clear-topic1", 2, TopicPermission.READ_WRITE);
        TopicConfig topic2 = new TopicConfig("clear-topic2", 3, TopicPermission.READ_WRITE);
        queueManager.createQueuesForTopic(topic1);
        queueManager.createQueuesForTopic(topic2);
        
        assertEquals(5, queueManager.getTotalQueueCount());
        
        // 清空所有Queue
        queueManager.clearAllQueues();
        assertEquals(0, queueManager.getTotalQueueCount());
        
        // 验证Topic映射也被清空
        assertTrue(queueManager.getQueuesForTopic("clear-topic1").isEmpty());
        assertTrue(queueManager.getQueuesForTopic("clear-topic2").isEmpty());
    }
    
    @Test
    @DisplayName("获取所有Queue配置")
    void testGetAllQueueConfigs() {
        // 创建Queue
        TopicConfig topicConfig = new TopicConfig("all-topic", 2, TopicPermission.READ_WRITE);
        queueManager.createQueuesForTopic(topicConfig);
        
        // 获取所有Queue配置
        Map<String, QueueConfig> allConfigs = queueManager.getAllQueueConfigs();
        assertEquals(2, allConfigs.size());
        
        assertTrue(allConfigs.containsKey("all-topic:0"));
        assertTrue(allConfigs.containsKey("all-topic:1"));
        
        QueueConfig config0 = allConfigs.get("all-topic:0");
        assertEquals("all-topic", config0.getTopicName());
        assertEquals(0, config0.getQueueId());
    }
}
