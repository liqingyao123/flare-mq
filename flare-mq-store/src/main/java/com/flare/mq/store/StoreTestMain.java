package com.flare.mq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * 存储引擎测试主类
 * 
 * @author FlareMQ Team
 */
public class StoreTestMain {
    
    private static final Logger logger = LoggerFactory.getLogger(StoreTestMain.class);
    
    public static void main(String[] args) {
        logger.info("开始存储引擎测试");
        
        try {
            // 测试消息序列化
            testMessageSerialization();
            
            // 测试ConsumeQueue单元
            testConsumeQueueUnit();
            
            // 测试存储引擎
            testMessageStore();
            
            logger.info("所有测试通过！");
            
        } catch (Exception e) {
            logger.error("测试失败", e);
            System.exit(1);
        }
    }
    
    private static void testMessageSerialization() {
        logger.info("=== 测试消息序列化 ===");
        
        // 创建测试消息
        Message message = new Message("test-topic", "test-tags", "test-key", "Hello World".getBytes());
        message.setQueueId(1);
        message.setFlag(0);
        message.setBornTimestamp(System.currentTimeMillis());
        
        logger.info("原始消息: {}", message);
        
        // 序列化
        byte[] serialized = MessageSerializer.serialize(message);
        if (serialized == null) {
            throw new RuntimeException("消息序列化失败");
        }
        
        logger.info("序列化成功，大小: {} bytes", serialized.length);
        
        // 反序列化
        Message deserialized = MessageSerializer.deserialize(serialized);
        if (deserialized == null) {
            throw new RuntimeException("消息反序列化失败");
        }
        
        logger.info("反序列化消息: {}", deserialized);
        
        // 验证内容
        if (!message.getTopic().equals(deserialized.getTopic()) ||
            !message.getTags().equals(deserialized.getTags()) ||
            !message.getKeys().equals(deserialized.getKeys()) ||
            message.getQueueId() != deserialized.getQueueId()) {
            throw new RuntimeException("消息内容验证失败");
        }
        
        logger.info("消息序列化测试通过");
    }
    
    private static void testConsumeQueueUnit() {
        logger.info("=== 测试ConsumeQueue单元 ===");
        
        // 创建ConsumeQueue单元
        ConsumeQueueUnit unit = new ConsumeQueueUnit(12345L, 256, 98765L);
        logger.info("原始单元: {}", unit);
        
        // 序列化
        byte[] serialized = unit.serialize();
        if (serialized == null || serialized.length != StoreConstants.CONSUME_QUEUE_UNIT_SIZE) {
            throw new RuntimeException("ConsumeQueue单元序列化失败");
        }
        
        logger.info("序列化成功，大小: {} bytes", serialized.length);
        
        // 反序列化
        ConsumeQueueUnit deserialized = ConsumeQueueUnit.deserialize(serialized);
        if (deserialized == null) {
            throw new RuntimeException("ConsumeQueue单元反序列化失败");
        }
        
        logger.info("反序列化单元: {}", deserialized);
        
        // 验证内容
        if (unit.getCommitLogOffset() != deserialized.getCommitLogOffset() ||
            unit.getSize() != deserialized.getSize() ||
            unit.getTagsHashCode() != deserialized.getTagsHashCode()) {
            throw new RuntimeException("ConsumeQueue单元内容验证失败");
        }
        
        logger.info("ConsumeQueue单元测试通过");
    }
    
    private static void testMessageStore() {
        logger.info("=== 测试消息存储引擎 ===");
        
        // 创建临时测试目录
        String testStorePath = System.getProperty("java.io.tmpdir") + File.separator + "flare-mq-test-" + System.currentTimeMillis();
        logger.info("测试存储路径: {}", testStorePath);
        
        DefaultMessageStore messageStore = null;
        try {
            // 初始化消息存储
            messageStore = new DefaultMessageStore(testStorePath);
            messageStore.start();
            logger.info("消息存储引擎启动成功");
            
            // 测试单条消息存储
            testSingleMessage(messageStore);
            
            // 测试多条消息存储
            testMultipleMessages(messageStore);
            
            // 测试性能
            testBasicPerformance(messageStore);
            
        } finally {
            if (messageStore != null) {
                messageStore.shutdown();
                logger.info("消息存储引擎关闭完成");
            }
        }
        
        logger.info("消息存储引擎测试通过");
    }
    
    private static void testSingleMessage(DefaultMessageStore messageStore) {
        logger.info("--- 测试单条消息存储 ---");
        
        // 创建测试消息
        Message message = new Message("test-topic", "test-tags", "test-key", "Hello World".getBytes());
        message.setQueueId(0);
        
        // 存储消息
        PutMessageResult putResult = messageStore.putMessage(message);
        if (!putResult.isOk()) {
            throw new RuntimeException("消息存储失败: " + putResult);
        }
        
        logger.info("消息存储成功: {}", putResult);
        
        // 读取消息
        GetMessageResult getResult = messageStore.getMessage("test-topic", 0, 0, 1);
        if (!getResult.isFound() || getResult.getMessageCount() != 1) {
            throw new RuntimeException("消息读取失败: " + getResult);
        }
        
        Message retrievedMessage = getResult.getMessageList().get(0);
        if (!message.getTopic().equals(retrievedMessage.getTopic()) ||
            !message.getTags().equals(retrievedMessage.getTags()) ||
            !message.getKeys().equals(retrievedMessage.getKeys())) {
            throw new RuntimeException("消息内容验证失败");
        }
        
        logger.info("单条消息存储测试通过");
    }
    
    private static void testMultipleMessages(DefaultMessageStore messageStore) {
        logger.info("--- 测试多条消息存储 ---");
        
        int messageCount = 100;
        
        // 存储多条消息
        for (int i = 0; i < messageCount; i++) {
            Message message = new Message("multi-topic", "tag-" + i, "key-" + i, 
                                        ("Message " + i).getBytes());
            message.setQueueId(i % 4); // 分布到4个队列
            
            PutMessageResult putResult = messageStore.putMessage(message);
            if (!putResult.isOk()) {
                throw new RuntimeException("消息 " + i + " 存储失败: " + putResult);
            }
        }
        
        logger.info("存储{}条消息完成", messageCount);
        
        // 验证每个队列的消息
        for (int queueId = 0; queueId < 4; queueId++) {
            long maxOffset = messageStore.getMaxOffset("multi-topic", queueId);
            if (maxOffset <= 0) {
                throw new RuntimeException("队列 " + queueId + " 应该有消息");
            }
            
            GetMessageResult getResult = messageStore.getMessage("multi-topic", queueId, 0, (int) maxOffset);
            if (!getResult.isFound()) {
                throw new RuntimeException("队列 " + queueId + " 消息读取失败");
            }
            
            logger.info("队列{}: 最大偏移量={}, 消息数量={}", queueId, maxOffset, getResult.getMessageCount());
        }
        
        logger.info("多条消息存储测试通过");
    }
    
    private static void testBasicPerformance(DefaultMessageStore messageStore) {
        logger.info("--- 测试基础性能 ---");
        
        int messageCount = 1000;
        long startTime = System.currentTimeMillis();
        
        // 存储消息
        for (int i = 0; i < messageCount; i++) {
            Message message = new Message("perf-topic", "perf-tag", "key-" + i, 
                                        ("Performance test " + i).getBytes());
            message.setQueueId(0);
            
            PutMessageResult putResult = messageStore.putMessage(message);
            if (!putResult.isOk()) {
                throw new RuntimeException("消息 " + i + " 存储失败: " + putResult);
            }
        }
        
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        double tps = (double) messageCount / totalTime * 1000;
        
        logger.info("性能测试结果:");
        logger.info("消息数量: {}", messageCount);
        logger.info("总耗时: {} ms", totalTime);
        logger.info("TPS: {:.2f}", tps);
        
        // 验证数据完整性
        long maxOffset = messageStore.getMaxOffset("perf-topic", 0);
        if (maxOffset != messageCount) {
            throw new RuntimeException("消息数量验证失败，期望: " + messageCount + ", 实际: " + maxOffset);
        }
        
        if (tps < 100) {
            logger.warn("TPS较低: {:.2f}, 但测试通过", tps);
        }
        
        logger.info("基础性能测试通过");
    }
}
