package com.flare.mq.integration;

import com.flare.mq.broker.topic.TopicManager;
import com.flare.mq.broker.topic.TopicConfig;
import com.flare.mq.broker.topic.TopicPermission;
import com.flare.mq.broker.queue.QueueManager;
import com.flare.mq.broker.ack.AckManager;
import com.flare.mq.broker.ack.AckResult;
import com.flare.mq.broker.ack.AckStatus;
import com.flare.mq.broker.ack.AckStats;
import com.flare.mq.client.producer.Producer;
import com.flare.mq.client.producer.ProducerImpl;
import com.flare.mq.client.producer.ProducerConfig;
import com.flare.mq.client.producer.Message;
import com.flare.mq.client.consumer.Consumer;
import com.flare.mq.client.consumer.ConsumerImpl;
import com.flare.mq.client.consumer.ConsumerConfig;
import com.flare.mq.client.consumer.MessageListener;
import com.flare.mq.client.consumer.ConsumeStatus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 消息流程集成测试
 * 
 * 测试从消息发送到消费确认的完整流程
 * 
 * @author FlareMQ Team
 */
@DisplayName("消息流程集成测试")
class MessageFlowIntegrationTest {
    
    private TopicManager topicManager;
    private QueueManager queueManager;
    private AckManager ackManager;
    private Producer producer;
    private Consumer consumer;
    
    @BeforeEach
    void setUp() {
        // 初始化Broker组件
        topicManager = new TopicManager();
        queueManager = new QueueManager();
        ackManager = new AckManager();
        
        // 启动组件
        topicManager.initializeDefaultTopics();
        ackManager.start();
        
        // 初始化Producer
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setProducerGroup("TEST_PRODUCER_GROUP");
        producerConfig.setNameServerAddr("localhost:9876");
        producer = new ProducerImpl(producerConfig);
        
        // 初始化Consumer
        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setConsumerGroup("TEST_CONSUMER_GROUP");
        consumerConfig.setNameServerAddr("localhost:9876");
        consumer = new ConsumerImpl(consumerConfig);
    }
    
    @AfterEach
    void tearDown() {
        if (ackManager != null) {
            ackManager.shutdown();
        }
        if (producer != null) {
            producer.shutdown();
        }
        if (consumer != null) {
            consumer.shutdown();
        }
    }
    
    @Test
    @DisplayName("完整消息流程测试 - Topic创建到消息确认")
    void testCompleteMessageFlow() {
        // 1. 创建Topic
        String topicName = "integration-test-topic";
        boolean topicCreated = topicManager.createTopic(topicName, 4, TopicPermission.READ_WRITE);
        assertTrue(topicCreated, "Topic创建应该成功");
        
        // 2. 为Topic创建Queue
        TopicConfig topicConfig = topicManager.getTopicConfig(topicName);
        assertNotNull(topicConfig, "Topic配置不应该为空");
        
        boolean queueCreated = queueManager.createQueuesForTopic(topicConfig);
        assertTrue(queueCreated, "Queue创建应该成功");
        assertEquals(4, queueManager.getQueueCountForTopic(topicName), "应该创建4个Queue");
        
        // 3. 模拟消息发送（由于没有真实的网络层，这里模拟发送过程）
        Message message = new Message(topicName, "test-tag", "Hello Integration Test".getBytes());
        message.setMessageId("integration-msg-001");
        message.setKey("test-key");
        
        // 模拟消息到达Broker，添加到确认管理器
        ackManager.addPendingAck(message.getMessageId(), "TEST_CONSUMER_GROUP", topicName, 0, "test-consumer", 0L, 0L);
        
        // 4. 验证消息状态
        AckStatus messageStatus = ackManager.getMessageAckStatus(message.getMessageId());
        assertEquals(AckStatus.PENDING, messageStatus, "消息应该处于待确认状态");
        
        // 5. 模拟消费者确认消息
        AckResult ackResult = ackManager.ackMessage(message.getMessageId(), "TEST_CONSUMER_GROUP");
        assertTrue(ackResult.isSuccess(), "消息确认应该成功");
        
        // 6. 验证最终状态
        AckStatus finalStatus = ackManager.getMessageAckStatus(message.getMessageId());
        assertEquals(AckStatus.ACKED, finalStatus, "消息应该已确认");
        
        // 7. 验证统计信息
        AckStats ackStats = ackManager.getAckStats();
        assertEquals(0, ackStats.getPendingAckCount(), "应该没有待确认消息");
        assertEquals(1, ackStats.getAckedCount(), "应该有1个已确认消息");
        assertEquals(100.0, ackStats.getAckSuccessRate(), 0.01, "确认成功率应该是100%");
    }
    
    @Test
    @DisplayName("消息重试流程测试")
    void testMessageRetryFlow() {
        // 1. 创建Topic和Queue
        String topicName = "retry-test-topic";
        topicManager.createTopic(topicName, 2, TopicPermission.READ_WRITE);
        TopicConfig topicConfig = topicManager.getTopicConfig(topicName);
        queueManager.createQueuesForTopic(topicConfig);
        
        // 2. 添加待确认消息
        String messageId = "retry-msg-001";
        ackManager.addPendingAck(messageId, "TEST_CONSUMER_GROUP", topicName, 0, "test-consumer", 0L, 0L);
        
        // 3. 模拟消费失败，加入重试队列
        ackManager.addToRetryQueue(messageId, "消费处理异常");
        
        // 4. 验证重试状态
        AckStatus retryStatus = ackManager.getMessageAckStatus(messageId);
        assertEquals(AckStatus.RETRYING, retryStatus, "消息应该处于重试状态");
        
        // 5. 验证重试记录
        java.util.List<com.flare.mq.broker.ack.RetryRecord> retryMessages = ackManager.getAllRetryMessages();
        assertEquals(1, retryMessages.size(), "应该有1个重试消息");

        com.flare.mq.broker.ack.RetryRecord retryRecord = retryMessages.get(0);
        assertEquals(messageId, retryRecord.getMessageId());
        assertEquals(1, retryRecord.getRetryCount());
        assertEquals("消费处理异常", retryRecord.getFailureReason());
        
        // 6. 模拟重试成功，确认消息
        AckResult ackResult = ackManager.ackMessage(messageId, "TEST_CONSUMER_GROUP");
        assertTrue(ackResult.isSuccess(), "重试后确认应该成功");
        
        // 7. 验证最终状态
        AckStatus finalStatus = ackManager.getMessageAckStatus(messageId);
        assertEquals(AckStatus.ACKED, finalStatus, "消息应该已确认");
    }
    
    @Test
    @DisplayName("死信队列流程测试")
    void testDeadLetterQueueFlow() {
        // 1. 创建Topic和Queue
        String topicName = "dlq-test-topic";
        topicManager.createTopic(topicName, 1, TopicPermission.READ_WRITE);
        TopicConfig topicConfig = topicManager.getTopicConfig(topicName);
        queueManager.createQueuesForTopic(topicConfig);
        
        // 2. 添加待确认消息
        String messageId = "dlq-msg-001";
        ackManager.addPendingAck(messageId, "TEST_CONSUMER_GROUP", topicName, 0, "test-consumer", 0L, 0L);
        
        // 3. 模拟多次重试失败
        for (int i = 0; i < 3; i++) {
            ackManager.addToRetryQueue(messageId, "重试失败 #" + (i + 1));
        }
        
        // 4. 移动到死信队列
        ackManager.moveToDeadLetterQueue(messageId, "超过最大重试次数");
        
        // 5. 验证死信状态
        AckStatus deadLetterStatus = ackManager.getMessageAckStatus(messageId);
        assertEquals(AckStatus.DEAD_LETTER, deadLetterStatus, "消息应该处于死信状态");
        
        // 6. 验证死信记录
        java.util.List<com.flare.mq.broker.ack.DeadLetterRecord> deadLetterMessages = ackManager.getDeadLetterMessages();
        assertEquals(1, deadLetterMessages.size(), "应该有1个死信消息");

        com.flare.mq.broker.ack.DeadLetterRecord deadLetterRecord = deadLetterMessages.get(0);
        assertEquals(messageId, deadLetterRecord.getMessageId());
        assertEquals(3, deadLetterRecord.getRetryCount());
        assertEquals("超过最大重试次数", deadLetterRecord.getFailureReason());
        
        // 7. 验证统计信息
        AckStats ackStats = ackManager.getAckStats();
        assertEquals(1, ackStats.getDeadLetterCount(), "应该有1个死信消息");
        assertEquals(100.0, ackStats.getDeadLetterRate(), 0.01, "死信率应该是100%");
    }
    
    @Test
    @DisplayName("批量消息处理测试")
    void testBatchMessageProcessing() {
        // 1. 创建Topic和Queue
        String topicName = "batch-test-topic";
        topicManager.createTopic(topicName, 4, TopicPermission.READ_WRITE);
        TopicConfig topicConfig = topicManager.getTopicConfig(topicName);
        queueManager.createQueuesForTopic(topicConfig);
        
        // 2. 批量添加待确认消息
        int messageCount = 10;
        for (int i = 0; i < messageCount; i++) {
            String messageId = "batch-msg-" + String.format("%03d", i);
            ackManager.addPendingAck(messageId, "TEST_CONSUMER_GROUP", topicName, i % 4, "test-consumer", 0L, 0L);
        }
        
        // 3. 验证初始状态
        AckStats initialStats = ackManager.getAckStats();
        assertEquals(messageCount, initialStats.getPendingAckCount(), "应该有10个待确认消息");
        assertEquals(messageCount, initialStats.getTotalMessageCount(), "总消息数应该是10");
        
        // 4. 批量确认部分消息
        java.util.List<String> messageIds = new java.util.ArrayList<>();
        for (int i = 0; i < 5; i++) {
            messageIds.add("batch-msg-" + String.format("%03d", i));
        }
        
        AckResult batchAckResult = ackManager.ackMessages(messageIds, "TEST_CONSUMER_GROUP");
        assertTrue(batchAckResult.isSuccess(), "批量确认应该成功");
        assertEquals(5, batchAckResult.getSuccessCount(), "应该成功确认5个消息");
        
        // 5. 部分消息加入重试队列
        for (int i = 5; i < 8; i++) {
            String messageId = "batch-msg-" + String.format("%03d", i);
            ackManager.addToRetryQueue(messageId, "批量处理失败");
        }
        
        // 6. 部分消息移动到死信队列
        for (int i = 8; i < 10; i++) {
            String messageId = "batch-msg-" + String.format("%03d", i);
            ackManager.addToRetryQueue(messageId, "批量处理失败");
            ackManager.moveToDeadLetterQueue(messageId, "超过最大重试次数");
        }
        
        // 7. 验证最终统计
        AckStats finalStats = ackManager.getAckStats();
        assertEquals(3, finalStats.getPendingAckCount(), "应该有3个待确认消息（包括重试中的）");
        assertEquals(5, finalStats.getAckedCount(), "应该有5个已确认消息");
        assertEquals(2, finalStats.getDeadLetterCount(), "应该有2个死信消息");
        assertEquals(50.0, finalStats.getAckSuccessRate(), 0.01, "确认成功率应该是50%");
        assertEquals(20.0, finalStats.getDeadLetterRate(), 0.01, "死信率应该是20%");
    }
    
    @Test
    @DisplayName("Topic和Queue管理集成测试")
    void testTopicQueueManagementIntegration() {
        // 1. 记录初始状态
        com.flare.mq.broker.topic.TopicStats initialTopicStats = topicManager.getTopicStats();
        com.flare.mq.broker.queue.QueueStats initialQueueStats = queueManager.getQueueStats();

        // 2. 创建多个Topic
        String[] topicNames = {"topic-1", "topic-2", "topic-3"};
        int[] queueCounts = {2, 4, 6};

        for (int i = 0; i < topicNames.length; i++) {
            boolean created = topicManager.createTopic(topicNames[i], queueCounts[i], TopicPermission.READ_WRITE);
            assertTrue(created, "Topic " + topicNames[i] + " 创建应该成功");

            TopicConfig config = topicManager.getTopicConfig(topicNames[i]);
            boolean queueCreated = queueManager.createQueuesForTopic(config);
            assertTrue(queueCreated, "Queue创建应该成功");
            assertEquals(queueCounts[i], queueManager.getQueueCountForTopic(topicNames[i]));
        }

        // 3. 验证Topic统计（验证增量）
        com.flare.mq.broker.topic.TopicStats topicStats = topicManager.getTopicStats();
        assertEquals(initialTopicStats.getTotalTopicCount() + 3, topicStats.getTotalTopicCount(), "应该增加3个Topic");
        assertEquals(initialTopicStats.getTotalQueueCount() + 12, topicStats.getTotalQueueCount(), "应该增加12个Queue（2+4+6）");

        // 4. 验证Queue统计
        com.flare.mq.broker.queue.QueueStats queueStats = queueManager.getQueueStats();
        assertEquals(initialQueueStats.getTotalQueueCount() + 12, queueStats.getTotalQueueCount(), "应该增加12个Queue");
        assertEquals(initialQueueStats.getTopicCount() + 3, queueStats.getTopicCount(), "应该增加3个Topic");

        // 5. 删除一个Topic及其Queue
        boolean queueDeleted = queueManager.deleteQueuesForTopic("topic-2");
        assertTrue(queueDeleted, "Queue删除应该成功");

        boolean topicDeleted = topicManager.deleteTopic("topic-2");
        assertTrue(topicDeleted, "Topic删除应该成功");

        // 6. 验证删除后的统计（验证删除操作成功）
        com.flare.mq.broker.topic.TopicStats finalTopicStats = topicManager.getTopicStats();
        assertEquals(topicStats.getTotalTopicCount() - 1, finalTopicStats.getTotalTopicCount(), "删除后应该减少1个Topic");
        assertEquals(topicStats.getTotalQueueCount() - 4, finalTopicStats.getTotalQueueCount(), "删除后应该减少4个Queue");

        com.flare.mq.broker.queue.QueueStats finalQueueStats = queueManager.getQueueStats();
        assertEquals(queueStats.getTotalQueueCount() - 4, finalQueueStats.getTotalQueueCount(), "删除后应该减少4个Queue");
        assertEquals(queueStats.getTopicCount() - 1, finalQueueStats.getTopicCount(), "删除后应该减少1个Topic");
    }
    
    @Test
    @DisplayName("Producer和Consumer配置测试")
    void testProducerConsumerConfiguration() {
        // 1. 测试Producer配置
        ProducerConfig producerConfig = producer.getConfig();
        assertNotNull(producerConfig, "Producer配置不应该为空");
        assertEquals("TEST_PRODUCER_GROUP", producerConfig.getProducerGroup());
        assertTrue(producerConfig.isValid(), "Producer配置应该有效");
        
        // 2. 测试Consumer配置
        ConsumerConfig consumerConfig = consumer.getConfig();
        assertNotNull(consumerConfig, "Consumer配置不应该为空");
        assertEquals("TEST_CONSUMER_GROUP", consumerConfig.getConsumerGroup());
        assertTrue(consumerConfig.isValid(), "Consumer配置应该有效");
        
        // 3. 测试订阅功能
        MessageListener listener = message -> ConsumeStatus.CONSUME_SUCCESS;
        consumer.subscribe("test-topic", "test-tag", listener);

        java.util.Map<String, com.flare.mq.client.consumer.SubscriptionData> subscriptions = consumer.getSubscriptions();
        assertEquals(1, subscriptions.size(), "应该有1个订阅");
        assertTrue(subscriptions.containsKey("test-topic"), "应该包含test-topic订阅");
        
        // 4. 测试取消订阅
        consumer.unsubscribe("test-topic");
        java.util.Map<String, com.flare.mq.client.consumer.SubscriptionData> emptySubscriptions = consumer.getSubscriptions();
        assertTrue(emptySubscriptions.isEmpty(), "取消订阅后应该为空");
    }
    
    @Test
    @DisplayName("系统健康状态检查测试")
    void testSystemHealthCheck() {
        // 1. 创建测试环境
        String topicName = "health-check-topic";
        topicManager.createTopic(topicName, 2, TopicPermission.READ_WRITE);
        TopicConfig topicConfig = topicManager.getTopicConfig(topicName);
        queueManager.createQueuesForTopic(topicConfig);
        
        // 2. 添加一些成功的消息
        for (int i = 0; i < 95; i++) {
            String messageId = "health-msg-" + i;
            ackManager.addPendingAck(messageId, "TEST_CONSUMER_GROUP", topicName, 0, "test-consumer", 0L, 0L);
            ackManager.ackMessage(messageId, "TEST_CONSUMER_GROUP");
        }
        
        // 3. 添加一些失败的消息
        for (int i = 95; i < 100; i++) {
            String messageId = "health-msg-" + i;
            ackManager.addPendingAck(messageId, "TEST_CONSUMER_GROUP", topicName, 0, "test-consumer", 0L, 0L);
            ackManager.addToRetryQueue(messageId, "健康检查测试失败");
            ackManager.moveToDeadLetterQueue(messageId, "超过最大重试次数");
        }
        
        // 4. 检查系统健康状态
        AckStats ackStats = ackManager.getAckStats();
        assertEquals(95.0, ackStats.getAckSuccessRate(), 0.01, "确认成功率应该是95%");
        assertEquals(5.0, ackStats.getDeadLetterRate(), 0.01, "死信率应该是5%");
        
        // 5. 验证健康状态（由于死信率为5%，应该是WARNING或CRITICAL都可以接受）
        com.flare.mq.broker.ack.AckStats.HealthStatus healthStatus = ackStats.getHealthStatus();
        assertTrue(healthStatus == com.flare.mq.broker.ack.AckStats.HealthStatus.WARNING ||
                  healthStatus == com.flare.mq.broker.ack.AckStats.HealthStatus.CRITICAL,
                  "系统健康状态应该是WARNING或CRITICAL，实际是: " + healthStatus);
    }
}
