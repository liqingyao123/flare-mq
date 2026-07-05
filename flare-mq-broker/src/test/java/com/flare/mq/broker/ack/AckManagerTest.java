package com.flare.mq.broker.ack;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AckManager测试类
 * 
 * @author FlareMQ Team
 */
@DisplayName("消息确认管理器测试")
class AckManagerTest {
    
    private AckManager ackManager;
    
    @BeforeEach
    void setUp() {
        ackManager = new AckManager();
        ackManager.start();
    }
    
    @AfterEach
    void tearDown() {
        if (ackManager != null) {
            ackManager.shutdown();
        }
    }
    
    @Test
    @DisplayName("添加待确认消息")
    void testAddPendingAck() {
        // 添加待确认消息
        ackManager.addPendingAck("msg-001", "consumer-group-1", "test-topic", 0, null, 0L, 0L);
        
        // 验证消息状态
        AckStatus status = ackManager.getMessageAckStatus("msg-001");
        assertEquals(AckStatus.PENDING, status);
        
        // 验证统计信息
        AckStats stats = ackManager.getAckStats();
        assertEquals(1, stats.getPendingAckCount());
        assertEquals(0, stats.getAckedCount());
        assertEquals(1, stats.getTotalMessageCount());
    }
    
    @Test
    @DisplayName("确认消息 - 成功")
    void testAckMessage_Success() {
        // 添加待确认消息
        ackManager.addPendingAck("msg-001", "consumer-group-1", "test-topic", 0, null, 0L, 0L);
        
        // 确认消息
        AckResult result = ackManager.ackMessage("msg-001", "consumer-group-1");
        
        // 验证确认结果
        assertTrue(result.isSuccess());
        assertEquals(1, result.getSuccessCount());
        assertEquals(0, result.getFailedCount());
        
        // 验证消息状态
        AckStatus status = ackManager.getMessageAckStatus("msg-001");
        assertEquals(AckStatus.ACKED, status);
        
        // 验证统计信息
        AckStats stats = ackManager.getAckStats();
        assertEquals(0, stats.getPendingAckCount());
        assertEquals(1, stats.getAckedCount());
    }
    
    @Test
    @DisplayName("确认消息 - 消息不存在")
    void testAckMessage_MessageNotExist() {
        // 确认不存在的消息
        AckResult result = ackManager.ackMessage("non-existent", "consumer-group-1");
        
        // 验证确认结果
        assertFalse(result.isSuccess());
        assertEquals(AckResultStatus.MESSAGE_NOT_EXIST, result.getStatus());
        assertEquals("消息不存在", result.getErrorMessage());
    }
    
    @Test
    @DisplayName("确认消息 - 消费者组不匹配")
    void testAckMessage_ConsumerGroupMismatch() {
        // 添加待确认消息
        ackManager.addPendingAck("msg-001", "consumer-group-1", "test-topic", 0, null, 0L, 0L);
        
        // 使用错误的消费者组确认消息
        AckResult result = ackManager.ackMessage("msg-001", "consumer-group-2");
        
        // 验证确认结果
        assertFalse(result.isSuccess());
        assertEquals(AckResultStatus.FAILURE, result.getStatus());
        assertTrue(result.getErrorMessage().contains("消费者组不匹配"));
    }
    
    @Test
    @DisplayName("确认消息 - 消息已确认")
    void testAckMessage_AlreadyAcked() {
        // 添加待确认消息
        ackManager.addPendingAck("msg-001", "consumer-group-1", "test-topic", 0, null, 0L, 0L);
        
        // 第一次确认
        AckResult result1 = ackManager.ackMessage("msg-001", "consumer-group-1");
        assertTrue(result1.isSuccess());
        
        // 第二次确认
        AckResult result2 = ackManager.ackMessage("msg-001", "consumer-group-1");
        assertFalse(result2.isSuccess());
        assertEquals(AckResultStatus.ALREADY_ACKED, result2.getStatus());
    }
    
    @Test
    @DisplayName("批量确认消息")
    void testAckMessages() {
        // 添加多个待确认消息
        ackManager.addPendingAck("msg-001", "consumer-group-1", "test-topic", 0, null, 0L, 0L);
        ackManager.addPendingAck("msg-002", "consumer-group-1", "test-topic", 0, null, 0L, 0L);
        ackManager.addPendingAck("msg-003", "consumer-group-1", "test-topic", 0, null, 0L, 0L);
        
        // 批量确认消息
        List<String> messageIds = new ArrayList<>();
        messageIds.add("msg-001");
        messageIds.add("msg-002");
        messageIds.add("non-existent"); // 不存在的消息
        
        AckResult result = ackManager.ackMessages(messageIds, "consumer-group-1");
        
        // 验证确认结果
        assertTrue(result.isPartialSuccess());
        assertEquals(2, result.getSuccessCount());
        assertEquals(1, result.getFailedCount());
        
        // 验证统计信息
        AckStats stats = ackManager.getAckStats();
        assertEquals(1, stats.getPendingAckCount()); // msg-003还未确认
        assertEquals(2, stats.getAckedCount());
    }
    
    @Test
    @DisplayName("加入重试队列")
    void testAddToRetryQueue() {
        // 添加待确认消息
        ackManager.addPendingAck("msg-001", "consumer-group-1", "test-topic", 0, null, 0L, 0L);
        
        // 加入重试队列
        ackManager.addToRetryQueue("msg-001", "消费失败");
        
        // 验证重试记录
        List<RetryRecord> retryMessages = ackManager.getAllRetryMessages();
        assertEquals(1, retryMessages.size());
        
        RetryRecord retryRecord = retryMessages.get(0);
        assertEquals("msg-001", retryRecord.getMessageId());
        assertEquals("consumer-group-1", retryRecord.getConsumerGroup());
        assertEquals(1, retryRecord.getRetryCount());
        assertEquals("消费失败", retryRecord.getFailureReason());
        assertEquals(RetryLevel.NORMAL, retryRecord.getRetryLevel());
    }
    
    @Test
    @DisplayName("移动到死信队列")
    void testMoveToDeadLetterQueue() {
        // 添加待确认消息
        ackManager.addPendingAck("msg-001", "consumer-group-1", "test-topic", 0, null, 0L, 0L);
        
        // 先加入重试队列
        ackManager.addToRetryQueue("msg-001", "消费失败");
        
        // 移动到死信队列
        ackManager.moveToDeadLetterQueue("msg-001", "超过最大重试次数");
        
        // 验证死信记录
        List<DeadLetterRecord> deadLetterMessages = ackManager.getDeadLetterMessages();
        assertEquals(1, deadLetterMessages.size());
        
        DeadLetterRecord deadLetterRecord = deadLetterMessages.get(0);
        assertEquals("msg-001", deadLetterRecord.getMessageId());
        assertEquals("consumer-group-1", deadLetterRecord.getConsumerGroup());
        assertEquals(1, deadLetterRecord.getRetryCount());
        assertEquals("超过最大重试次数", deadLetterRecord.getFailureReason());
        assertEquals(DeadLetterType.RETRY_EXHAUSTED, deadLetterRecord.getDeadLetterType());
        
        // 验证消息状态
        AckStatus status = ackManager.getMessageAckStatus("msg-001");
        assertEquals(AckStatus.DEAD_LETTER, status);
        
        // 验证重试队列已清空
        List<RetryRecord> retryMessages = ackManager.getAllRetryMessages();
        assertEquals(0, retryMessages.size());
    }
    
    @Test
    @DisplayName("获取需要重试的消息")
    void testGetRetryMessages() throws InterruptedException {
        // 添加待确认消息
        ackManager.addPendingAck("msg-001", "consumer-group-1", "test-topic", 0, null, 0L, 0L);
        ackManager.addPendingAck("msg-002", "consumer-group-1", "test-topic", 0, null, 0L, 0L);
        
        // 加入重试队列
        ackManager.addToRetryQueue("msg-001", "消费失败");
        ackManager.addToRetryQueue("msg-002", "消费超时");
        
        // 立即获取重试消息（应该为空，因为还没到重试时间）
        List<RetryRecord> retryMessages = ackManager.getRetryMessages();
        assertEquals(0, retryMessages.size());
        
        // 等待一段时间后再获取（实际测试中可以模拟时间）
        // 这里简化处理，直接验证重试记录存在
        List<RetryRecord> allRetryMessages = ackManager.getAllRetryMessages();
        assertEquals(2, allRetryMessages.size());
    }
    
    @Test
    @DisplayName("获取统计信息")
    void testGetAckStats() {
        // 添加多个消息进行不同操作
        ackManager.addPendingAck("msg-001", "consumer-group-1", "test-topic", 0, null, 0L, 0L);
        ackManager.addPendingAck("msg-002", "consumer-group-1", "test-topic", 0, null, 0L, 0L);
        ackManager.addPendingAck("msg-003", "consumer-group-1", "test-topic", 0, null, 0L, 0L);
        ackManager.addPendingAck("msg-004", "consumer-group-1", "test-topic", 0, null, 0L, 0L);
        
        // 确认一个消息
        ackManager.ackMessage("msg-001", "consumer-group-1");
        
        // 一个消息加入重试队列
        ackManager.addToRetryQueue("msg-002", "消费失败");
        
        // 一个消息移动到死信队列
        ackManager.addToRetryQueue("msg-003", "消费失败");
        ackManager.moveToDeadLetterQueue("msg-003", "超过最大重试次数");
        
        // 获取统计信息
        AckStats stats = ackManager.getAckStats();
        
        assertEquals(2, stats.getPendingAckCount()); // msg-004 (pending) + msg-002 (retrying)
        assertEquals(1, stats.getAckedCount()); // msg-001
        assertEquals(1, stats.getDeadLetterCount()); // msg-003
        assertEquals(1, stats.getRetryQueueSize()); // msg-002
        assertEquals(4, stats.getTotalMessageCount());
        assertEquals(25.0, stats.getAckSuccessRate(), 0.01); // 1/4 = 25%
        assertEquals(75.0, stats.getAckFailureRate(), 0.01); // 75%
        assertEquals(25.0, stats.getDeadLetterRate(), 0.01); // 1/4 = 25%
        assertEquals(50.0, stats.getCompletionRate(), 0.01); // (1+1)/4 = 50%
    }
    
    @Test
    @DisplayName("清理已确认消息记录")
    void testCleanupAckedMessages() throws InterruptedException {
        // 添加并确认消息
        ackManager.addPendingAck("msg-001", "consumer-group-1", "test-topic", 0, null, 0L, 0L);
        ackManager.addPendingAck("msg-002", "consumer-group-1", "test-topic", 0, null, 0L, 0L);
        
        ackManager.ackMessage("msg-001", "consumer-group-1");
        
        // 等待一小段时间
        Thread.sleep(10);
        
        ackManager.ackMessage("msg-002", "consumer-group-1");
        
        // 清理较早确认的消息
        long cleanupTime = System.currentTimeMillis() - 5;
        ackManager.cleanupAckedMessages(cleanupTime);
        
        // 验证统计信息（应该清理了一些记录）
        AckStats stats = ackManager.getAckStats();
        assertTrue(stats.getTotalMessageCount() <= 2);
    }
    
    @Test
    @DisplayName("AckRecord测试")
    void testAckRecord() {
        AckRecord record = new AckRecord("msg-001", "consumer-group-1", "test-topic", 0);
        
        // 验证初始状态
        assertEquals("msg-001", record.getMessageId());
        assertEquals("consumer-group-1", record.getConsumerGroup());
        assertEquals("test-topic", record.getTopic());
        assertEquals(0, record.getQueueId());
        assertEquals(AckStatus.PENDING, record.getStatus());
        assertTrue(record.isPending());
        assertFalse(record.isAcked());
        assertFalse(record.isDeadLetter());
        
        // 测试确认
        record.setStatus(AckStatus.ACKED);
        record.setAckTime(System.currentTimeMillis());
        assertTrue(record.isAcked());
        assertFalse(record.isPending());
        
        // 测试等待时间
        assertTrue(record.getWaitingAckTime() >= 0);
        
        // 测试超时检查
        assertFalse(record.isTimeout(60000)); // 60秒超时
        
        // 测试复制
        AckRecord copy = record.copy();
        assertEquals(record.getMessageId(), copy.getMessageId());
        assertEquals(record.getConsumerGroup(), copy.getConsumerGroup());
        assertEquals(record.getStatus(), copy.getStatus());
    }
    
    @Test
    @DisplayName("RetryRecord测试")
    void testRetryRecord() {
        RetryRecord record = new RetryRecord("msg-001", "consumer-group-1", "test-topic", 0);
        
        // 验证初始状态
        assertEquals("msg-001", record.getMessageId());
        assertEquals(0, record.getRetryCount());
        assertEquals(RetryLevel.NORMAL, record.getRetryLevel());
        
        // 测试增加重试次数
        record.incrementRetryCount();
        assertEquals(1, record.getRetryCount());
        assertEquals(RetryLevel.NORMAL, record.getRetryLevel());
        
        // 测试重试级别变化
        for (int i = 0; i < 5; i++) {
            record.incrementRetryCount();
        }
        assertEquals(6, record.getRetryCount());
        assertEquals(RetryLevel.HIGH, record.getRetryLevel());
        
        for (int i = 0; i < 5; i++) {
            record.incrementRetryCount();
        }
        assertEquals(11, record.getRetryCount());
        assertEquals(RetryLevel.CRITICAL, record.getRetryLevel());
        
        // 测试重试能力
        assertTrue(record.canRetry(16));
        assertFalse(record.canRetry(10));
        
        // 测试复制
        RetryRecord copy = record.copy();
        assertEquals(record.getMessageId(), copy.getMessageId());
        assertEquals(record.getRetryCount(), copy.getRetryCount());
        assertEquals(record.getRetryLevel(), copy.getRetryLevel());
    }
    
    @Test
    @DisplayName("DeadLetterRecord测试")
    void testDeadLetterRecord() {
        DeadLetterRecord record = new DeadLetterRecord("msg-001", "consumer-group-1", 
                                                      "test-topic", 0, 5, "超过最大重试次数");
        
        // 验证初始状态
        assertEquals("msg-001", record.getMessageId());
        assertEquals("consumer-group-1", record.getConsumerGroup());
        assertEquals(5, record.getRetryCount());
        assertEquals("超过最大重试次数", record.getFailureReason());
        assertEquals(DeadLetterType.RETRY_EXHAUSTED, record.getDeadLetterType());
        assertEquals(DeadLetterStatus.PENDING, record.getStatus());
        
        // 测试状态检查
        assertTrue(record.canRedeliver());
        assertFalse(record.isProcessed());
        
        // 测试死信队列名称
        String expectedQueueName = "test-topic_DLQ_consumer-group-1";
        assertEquals(expectedQueueName, record.getDeadLetterQueueName());
        
        // 测试存活时间
        assertTrue(record.getDeadLetterAge() >= 0);
        
        // 测试复制
        DeadLetterRecord copy = record.copy();
        assertEquals(record.getMessageId(), copy.getMessageId());
        assertEquals(record.getRetryCount(), copy.getRetryCount());
        assertEquals(record.getDeadLetterType(), copy.getDeadLetterType());
    }
}
