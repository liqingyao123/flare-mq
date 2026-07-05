package com.flare.mq.broker.topic;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TopicManager测试类
 * 
 * @author FlareMQ Team
 */
@DisplayName("Topic管理器测试")
class TopicManagerTest {
    
    private TopicManager topicManager;
    
    @BeforeEach
    void setUp() {
        topicManager = new TopicManager();
    }
    
    @Test
    @DisplayName("创建Topic - 正常情况")
    void testCreateTopic_Success() {
        // 测试创建Topic
        boolean result = topicManager.createTopic("test-topic", 4, TopicPermission.READ_WRITE);
        assertTrue(result, "创建Topic应该成功");
        
        // 验证Topic是否存在
        assertTrue(topicManager.topicExists("test-topic"), "Topic应该存在");
        
        // 验证Topic配置
        TopicConfig config = topicManager.getTopicConfig("test-topic");
        assertNotNull(config, "Topic配置不应该为空");
        assertEquals("test-topic", config.getTopicName());
        assertEquals(4, config.getQueueCount());
        assertEquals(TopicPermission.READ_WRITE, config.getPermission());
    }
    
    @Test
    @DisplayName("创建Topic - 使用默认配置")
    void testCreateTopic_DefaultConfig() {
        boolean result = topicManager.createTopic("default-topic");
        assertTrue(result, "使用默认配置创建Topic应该成功");
        
        TopicConfig config = topicManager.getTopicConfig("default-topic");
        assertNotNull(config);
        assertEquals(4, config.getQueueCount()); // 默认队列数
        assertEquals(TopicPermission.READ_WRITE, config.getPermission()); // 默认权限
    }
    
    @Test
    @DisplayName("创建Topic - 参数验证")
    void testCreateTopic_Validation() {
        // 测试空Topic名称
        assertFalse(topicManager.createTopic(null, 4, TopicPermission.READ_WRITE));
        assertFalse(topicManager.createTopic("", 4, TopicPermission.READ_WRITE));
        assertFalse(topicManager.createTopic("  ", 4, TopicPermission.READ_WRITE));
        
        // 测试无效队列数量
        assertFalse(topicManager.createTopic("test-topic", 0, TopicPermission.READ_WRITE));
        assertFalse(topicManager.createTopic("test-topic", -1, TopicPermission.READ_WRITE));
    }
    
    @Test
    @DisplayName("创建Topic - 重复创建")
    void testCreateTopic_Duplicate() {
        // 第一次创建应该成功
        assertTrue(topicManager.createTopic("duplicate-topic", 4, TopicPermission.READ_WRITE));
        
        // 第二次创建应该失败
        assertFalse(topicManager.createTopic("duplicate-topic", 8, TopicPermission.READ_ONLY));
        
        // 验证配置没有被覆盖
        TopicConfig config = topicManager.getTopicConfig("duplicate-topic");
        assertEquals(4, config.getQueueCount());
        assertEquals(TopicPermission.READ_WRITE, config.getPermission());
    }
    
    @Test
    @DisplayName("删除Topic")
    void testDeleteTopic() {
        // 先创建Topic
        topicManager.createTopic("delete-topic", 4, TopicPermission.READ_WRITE);
        assertTrue(topicManager.topicExists("delete-topic"));
        
        // 删除Topic
        boolean result = topicManager.deleteTopic("delete-topic");
        assertTrue(result, "删除Topic应该成功");
        assertFalse(topicManager.topicExists("delete-topic"), "Topic应该不存在");
        
        // 删除不存在的Topic
        assertFalse(topicManager.deleteTopic("non-existent-topic"));
    }
    
    @Test
    @DisplayName("更新Topic配置")
    void testUpdateTopicConfig() {
        // 先创建Topic
        topicManager.createTopic("update-topic", 4, TopicPermission.READ_WRITE);
        
        // 更新配置
        boolean result = topicManager.updateTopicConfig("update-topic", 8, TopicPermission.READ_ONLY);
        assertTrue(result, "更新Topic配置应该成功");
        
        // 验证配置已更新
        TopicConfig config = topicManager.getTopicConfig("update-topic");
        assertEquals(8, config.getQueueCount());
        assertEquals(TopicPermission.READ_ONLY, config.getPermission());
        
        // 更新不存在的Topic
        assertFalse(topicManager.updateTopicConfig("non-existent", 4, TopicPermission.READ_WRITE));
    }
    
    @Test
    @DisplayName("获取所有Topic")
    void testGetAllTopics() {
        // 初始状态应该为空
        List<String> topics = topicManager.getAllTopics();
        assertTrue(topics.isEmpty());
        
        // 创建几个Topic
        topicManager.createTopic("topic1", 4, TopicPermission.READ_WRITE);
        topicManager.createTopic("topic2", 8, TopicPermission.READ_ONLY);
        topicManager.createTopic("topic3", 2, TopicPermission.WRITE_ONLY);
        
        // 验证Topic列表
        topics = topicManager.getAllTopics();
        assertEquals(3, topics.size());
        assertTrue(topics.contains("topic1"));
        assertTrue(topics.contains("topic2"));
        assertTrue(topics.contains("topic3"));
    }
    
    @Test
    @DisplayName("获取Topic统计信息")
    void testGetTopicStats() {
        // 初始统计
        TopicStats stats = topicManager.getTopicStats();
        assertEquals(0, stats.getTotalTopicCount());
        assertEquals(0, stats.getTotalQueueCount());
        
        // 创建Topic后的统计
        topicManager.createTopic("stats-topic1", 4, TopicPermission.READ_WRITE);
        topicManager.createTopic("stats-topic2", 8, TopicPermission.READ_ONLY);
        
        stats = topicManager.getTopicStats();
        assertEquals(2, stats.getTotalTopicCount());
        assertEquals(12, stats.getTotalQueueCount()); // 4 + 8
    }
    
    @Test
    @DisplayName("初始化默认Topic")
    void testInitializeDefaultTopics() {
        // 初始化前应该为空
        assertEquals(0, topicManager.getTopicCount());
        
        // 初始化默认Topic
        topicManager.initializeDefaultTopics();
        
        // 验证默认Topic已创建
        assertTrue(topicManager.getTopicCount() > 0);
        assertTrue(topicManager.topicExists("default-topic"));
        assertTrue(topicManager.topicExists("order-topic"));
        assertTrue(topicManager.topicExists("log-topic"));
        assertTrue(topicManager.topicExists("notification-topic"));
    }
    
    @Test
    @DisplayName("Topic名称验证")
    void testIsValidTopicName() {
        // 有效的Topic名称
        assertTrue(topicManager.isValidTopicName("valid-topic"));
        assertTrue(topicManager.isValidTopicName("topic_123"));
        assertTrue(topicManager.isValidTopicName("topic.name"));
        assertTrue(topicManager.isValidTopicName("Topic-Name_123.test"));
        
        // 无效的Topic名称
        assertFalse(topicManager.isValidTopicName(null));
        assertFalse(topicManager.isValidTopicName(""));
        assertFalse(topicManager.isValidTopicName("  "));
        assertFalse(topicManager.isValidTopicName("topic with space"));
        assertFalse(topicManager.isValidTopicName("topic@name"));
        assertFalse(topicManager.isValidTopicName("topic#name"));
        
        // 长度超限的Topic名称
        String longName = "a".repeat(256);
        assertFalse(topicManager.isValidTopicName(longName));
    }
    
    @Test
    @DisplayName("清空所有Topic")
    void testClearAllTopics() {
        // 创建一些Topic
        topicManager.createTopic("topic1", 4, TopicPermission.READ_WRITE);
        topicManager.createTopic("topic2", 8, TopicPermission.READ_ONLY);
        assertEquals(2, topicManager.getTopicCount());
        
        // 清空所有Topic
        topicManager.clearAllTopics();
        assertEquals(0, topicManager.getTopicCount());
        assertTrue(topicManager.getAllTopics().isEmpty());
    }
    
    @Test
    @DisplayName("获取所有Topic配置")
    void testGetAllTopicConfigs() {
        // 创建Topic
        topicManager.createTopic("config-topic1", 4, TopicPermission.READ_WRITE);
        topicManager.createTopic("config-topic2", 8, TopicPermission.READ_ONLY);
        
        // 获取所有配置
        Map<String, TopicConfig> configs = topicManager.getAllTopicConfigs();
        assertEquals(2, configs.size());
        assertTrue(configs.containsKey("config-topic1"));
        assertTrue(configs.containsKey("config-topic2"));
        
        // 验证配置内容
        TopicConfig config1 = configs.get("config-topic1");
        assertEquals(4, config1.getQueueCount());
        assertEquals(TopicPermission.READ_WRITE, config1.getPermission());
    }
}
