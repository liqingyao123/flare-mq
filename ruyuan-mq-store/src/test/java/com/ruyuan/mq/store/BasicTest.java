package com.ruyuan.mq.store;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 基础功能测试
 * 
 * @author RuYuan MQ Team
 */
class BasicTest {
    
    private static final Logger logger = LoggerFactory.getLogger(BasicTest.class);
    
    @Test
    void testMessageSerialization() {
        logger.info("开始消息序列化测试");
        
        // 创建测试消息
        Message message = new Message("test-topic", "test-tags", "test-key", "Hello World".getBytes());
        message.setQueueId(1);
        message.setFlag(0);
        message.setBornTimestamp(System.currentTimeMillis());
        
        // 序列化
        byte[] serialized = MessageSerializer.serialize(message);
        assertNotNull(serialized, "序列化结果不应为空");
        assertTrue(serialized.length > 0, "序列化结果应有内容");
        
        logger.info("消息序列化成功，大小: {} bytes", serialized.length);
        
        // 反序列化
        Message deserialized = MessageSerializer.deserialize(serialized);
        assertNotNull(deserialized, "反序列化结果不应为空");
        
        // 验证内容
        assertEquals(message.getTopic(), deserialized.getTopic());
        assertEquals(message.getTags(), deserialized.getTags());
        assertEquals(message.getKeys(), deserialized.getKeys());
        assertEquals(message.getQueueId(), deserialized.getQueueId());
        assertEquals(message.getFlag(), deserialized.getFlag());
        assertArrayEquals(message.getBody(), deserialized.getBody());
        
        logger.info("消息序列化测试通过");
    }
    
    @Test
    void testConsumeQueueUnit() {
        logger.info("开始ConsumeQueue单元测试");
        
        // 创建ConsumeQueue单元
        ConsumeQueueUnit unit = new ConsumeQueueUnit(12345L, 256, 98765L);
        
        // 序列化
        byte[] serialized = unit.serialize();
        assertNotNull(serialized, "序列化结果不应为空");
        assertEquals(StoreConstants.CONSUME_QUEUE_UNIT_SIZE, serialized.length, 
                    "序列化大小应为" + StoreConstants.CONSUME_QUEUE_UNIT_SIZE + "字节");
        
        // 反序列化
        ConsumeQueueUnit deserialized = ConsumeQueueUnit.deserialize(serialized);
        assertNotNull(deserialized, "反序列化结果不应为空");
        
        // 验证内容
        assertEquals(unit.getCommitLogOffset(), deserialized.getCommitLogOffset());
        assertEquals(unit.getSize(), deserialized.getSize());
        assertEquals(unit.getTagsHashCode(), deserialized.getTagsHashCode());
        
        logger.info("ConsumeQueue单元测试通过");
    }
    
    @Test
    void testStoreConstants() {
        logger.info("开始常量测试");
        
        // 验证常量值
        assertTrue(StoreConstants.COMMIT_LOG_FILE_SIZE > 0, "CommitLog文件大小应大于0");
        assertTrue(StoreConstants.CONSUME_QUEUE_FILE_SIZE > 0, "ConsumeQueue文件大小应大于0");
        assertTrue(StoreConstants.CONSUME_QUEUE_UNIT_SIZE == 20, "ConsumeQueue单元大小应为20字节");
        assertTrue(StoreConstants.MESSAGE_MIN_SIZE > 0, "消息最小大小应大于0");
        
        logger.info("常量测试通过");
        logger.info("CommitLog文件大小: {} MB", StoreConstants.COMMIT_LOG_FILE_SIZE / 1024 / 1024);
        logger.info("ConsumeQueue文件大小: {} KB", StoreConstants.CONSUME_QUEUE_FILE_SIZE / 1024);
        logger.info("ConsumeQueue单元大小: {} bytes", StoreConstants.CONSUME_QUEUE_UNIT_SIZE);
    }
    
    @Test
    void testMessageCalculateSize() {
        logger.info("开始消息大小计算测试");
        
        // 创建测试消息
        Message message = new Message("test-topic", "test-tags", "test-key", "Hello World".getBytes());
        
        // 计算大小
        int calculatedSize = message.calculateStoreSize();
        assertTrue(calculatedSize > StoreConstants.MESSAGE_MIN_SIZE, 
                  "计算的消息大小应大于最小消息大小");
        
        logger.info("消息大小计算结果: {} bytes", calculatedSize);
        
        // 验证序列化后的实际大小
        byte[] serialized = MessageSerializer.serialize(message);
        assertNotNull(serialized, "序列化结果不应为空");
        
        logger.info("实际序列化大小: {} bytes", serialized.length);
        logger.info("消息大小计算测试通过");
    }
}
