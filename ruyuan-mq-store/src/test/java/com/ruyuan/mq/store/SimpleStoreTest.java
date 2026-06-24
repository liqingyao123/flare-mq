package com.ruyuan.mq.store;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 简单存储测试
 * 
 * @author RuYuan MQ Team
 */
class SimpleStoreTest {
    
    private static final Logger logger = LoggerFactory.getLogger(SimpleStoreTest.class);
    
    private DefaultMessageStore messageStore;
    private String testStorePath;
    
    @BeforeEach
    void setUp() throws Exception {
        // 创建临时测试目录
        testStorePath = System.getProperty("java.io.tmpdir") + File.separator + "ruyuan-mq-simple-test-" + System.currentTimeMillis();
        Files.createDirectories(Paths.get(testStorePath));
        
        // 初始化消息存储
        messageStore = new DefaultMessageStore(testStorePath);
        messageStore.start();
        
        logger.info("测试环境初始化完成: {}", testStorePath);
    }
    
    @AfterEach
    void tearDown() throws Exception {
        if (messageStore != null) {
            messageStore.shutdown();
        }
        
        // 清理测试目录
        deleteDirectory(Paths.get(testStorePath));
        logger.info("测试环境清理完成");
    }
    
    @Test
    void testSingleMessage() {
        logger.info("开始单消息测试");
        
        // 创建测试消息
        Message message = new Message("test-topic", "test-tags", "test-key", "Hello World".getBytes());
        message.setQueueId(0);
        
        // 存储消息
        PutMessageResult putResult = messageStore.putMessage(message);
        logger.info("存储结果: {}", putResult);
        
        assertTrue(putResult.isOk(), "消息存储应该成功");
        assertNotNull(putResult.getAppendMessageResult(), "应该有追加结果");
        
        // 读取消息
        GetMessageResult getResult = messageStore.getMessage("test-topic", 0, 0, 1);
        logger.info("读取结果: {}", getResult);
        
        assertTrue(getResult.isFound(), "应该找到消息");
        assertEquals(1, getResult.getMessageCount(), "应该有1条消息");
        
        Message retrievedMessage = getResult.getMessageList().get(0);
        assertEquals("test-topic", retrievedMessage.getTopic());
        assertEquals("test-tags", retrievedMessage.getTags());
        assertEquals("test-key", retrievedMessage.getKeys());
        assertArrayEquals("Hello World".getBytes(), retrievedMessage.getBody());
        
        logger.info("单消息测试通过");
    }
    
    @Test
    void testMultipleMessages() {
        logger.info("开始多消息测试");
        
        int messageCount = 10;
        
        // 存储多条消息
        for (int i = 0; i < messageCount; i++) {
            Message message = new Message("multi-topic", "tag-" + i, "key-" + i, 
                                        ("Message " + i).getBytes());
            message.setQueueId(0);
            
            PutMessageResult putResult = messageStore.putMessage(message);
            assertTrue(putResult.isOk(), "消息 " + i + " 存储应该成功");
        }
        
        logger.info("存储{}条消息完成", messageCount);
        
        // 验证消息数量
        long maxOffset = messageStore.getMaxOffset("multi-topic", 0);
        assertEquals(messageCount, maxOffset, "应该有" + messageCount + "条消息");
        
        // 读取所有消息
        GetMessageResult getResult = messageStore.getMessage("multi-topic", 0, 0, messageCount);
        assertTrue(getResult.isFound(), "应该找到消息");
        assertEquals(messageCount, getResult.getMessageCount(), "应该有" + messageCount + "条消息");
        
        // 验证消息内容
        for (int i = 0; i < messageCount; i++) {
            Message message = getResult.getMessageList().get(i);
            assertEquals("multi-topic", message.getTopic());
            assertEquals("tag-" + i, message.getTags());
            assertEquals("key-" + i, message.getKeys());
            assertArrayEquals(("Message " + i).getBytes(), message.getBody());
        }
        
        logger.info("多消息测试通过");
    }
    
    @Test
    void testBasicPerformance() {
        logger.info("开始基础性能测试");
        
        int messageCount = 1000;
        long startTime = System.currentTimeMillis();
        
        // 存储消息
        for (int i = 0; i < messageCount; i++) {
            Message message = new Message("perf-topic", "perf-tag", "key-" + i, 
                                        ("Performance test " + i).getBytes());
            message.setQueueId(0);
            
            PutMessageResult putResult = messageStore.putMessage(message);
            assertTrue(putResult.isOk(), "消息 " + i + " 存储应该成功");
        }
        
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        double tps = (double) messageCount / totalTime * 1000;
        
        logger.info("基础性能测试结果:");
        logger.info("消息数量: {}", messageCount);
        logger.info("总耗时: {} ms", totalTime);
        logger.info("TPS: {:.2f}", tps);
        
        // 验证数据完整性
        long maxOffset = messageStore.getMaxOffset("perf-topic", 0);
        assertEquals(messageCount, maxOffset, "应该有" + messageCount + "条消息");
        
        assertTrue(tps > 100, "TPS应该大于100");
        logger.info("基础性能测试通过");
    }
    
    /**
     * 递归删除目录
     */
    private void deleteDirectory(Path path) throws Exception {
        if (Files.exists(path)) {
            Files.walk(path)
                 .sorted((a, b) -> b.compareTo(a)) // 先删除文件，再删除目录
                 .forEach(p -> {
                     try {
                         Files.delete(p);
                     } catch (Exception e) {
                         logger.warn("删除文件失败: " + p, e);
                     }
                 });
        }
    }
}
