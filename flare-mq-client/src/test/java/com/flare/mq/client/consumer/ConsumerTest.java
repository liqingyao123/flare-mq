package com.flare.mq.client.consumer;

import com.flare.mq.client.producer.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Consumer测试类
 * 
 * @author FlareMQ Team
 */
@DisplayName("Consumer测试")
class ConsumerTest {
    
    private ConsumerConfig config;
    private Consumer consumer;
    
    @BeforeEach
    void setUp() {
        config = new ConsumerConfig();
        config.setConsumerGroup("TEST_CONSUMER");
        config.setNameServerAddr("localhost:9876");
        config.setPullMsgTimeout(3000);
        
        consumer = new ConsumerImpl(config);
    }
    
    @Test
    @DisplayName("Consumer配置测试")
    void testConsumerConfig() {
        // 测试默认配置
        ConsumerConfig defaultConfig = new ConsumerConfig();
        assertTrue(defaultConfig.isValid());
        assertEquals("DEFAULT_CONSUMER", defaultConfig.getConsumerGroup());
        assertEquals("localhost:9876", defaultConfig.getNameServerAddr());
        assertEquals(ConsumeMode.CLUSTERING, defaultConfig.getConsumeMode());
        assertEquals(ConsumeType.CONSUME_ACTIVELY, defaultConfig.getConsumeType());
        assertEquals(3000, defaultConfig.getPullMsgTimeout());
        assertEquals(15000, defaultConfig.getConsumeTimeout());
        
        // 测试自定义配置
        assertEquals("TEST_CONSUMER", config.getConsumerGroup());
        assertEquals("localhost:9876", config.getNameServerAddr());
        assertEquals(3000, config.getPullMsgTimeout());
        
        // 测试配置复制
        ConsumerConfig copy = config.copy();
        assertEquals(config.getConsumerGroup(), copy.getConsumerGroup());
        assertEquals(config.getNameServerAddr(), copy.getNameServerAddr());
        assertEquals(config.getPullMsgTimeout(), copy.getPullMsgTimeout());
    }
    
    @Test
    @DisplayName("Consumer配置验证")
    void testConsumerConfigValidation() {
        // 有效配置
        assertTrue(config.isValid());
        
        // 无效配置 - 空Consumer组
        ConsumerConfig invalidConfig1 = new ConsumerConfig();
        invalidConfig1.setConsumerGroup(null);
        assertFalse(invalidConfig1.isValid());
        
        invalidConfig1.setConsumerGroup("");
        assertFalse(invalidConfig1.isValid());
        
        // 无效配置 - 空NameServer地址
        ConsumerConfig invalidConfig2 = new ConsumerConfig();
        invalidConfig2.setNameServerAddr(null);
        assertFalse(invalidConfig2.isValid());
        
        // 无效配置 - 超时时间
        ConsumerConfig invalidConfig3 = new ConsumerConfig();
        invalidConfig3.setPullMsgTimeout(0);
        assertFalse(invalidConfig3.isValid());
        
        // 无效配置 - 线程数量
        ConsumerConfig invalidConfig4 = new ConsumerConfig();
        invalidConfig4.setConsumeThreadMax(5);
        invalidConfig4.setConsumeThreadNums(10);
        assertFalse(invalidConfig4.isValid());
    }
    
    @Test
    @DisplayName("Consumer状态测试")
    void testConsumerStatus() {
        // 初始状态
        assertEquals(ConsumerStatus.CREATE_JUST, consumer.getStatus());
        assertFalse(consumer.getStatus().canConsumeMessage());
        assertFalse(consumer.getStatus().isStarted());
        assertFalse(consumer.getStatus().isShutdown());
        
        // 测试状态枚举
        assertTrue(ConsumerStatus.RUNNING.canConsumeMessage());
        assertTrue(ConsumerStatus.RUNNING.isStarted());
        assertFalse(ConsumerStatus.RUNNING.isShutdown());
        assertTrue(ConsumerStatus.RUNNING.canPause());
        assertFalse(ConsumerStatus.RUNNING.canResume());
        
        assertTrue(ConsumerStatus.PAUSED.isStarted());
        assertFalse(ConsumerStatus.PAUSED.canConsumeMessage());
        assertFalse(ConsumerStatus.PAUSED.canPause());
        assertTrue(ConsumerStatus.PAUSED.canResume());
        
        assertFalse(ConsumerStatus.SHUTDOWN_ALREADY.canConsumeMessage());
        assertFalse(ConsumerStatus.SHUTDOWN_ALREADY.isStarted());
        assertTrue(ConsumerStatus.SHUTDOWN_ALREADY.isShutdown());
    }
    
    @Test
    @DisplayName("消费模式测试")
    void testConsumeMode() {
        assertEquals("CLUSTERING", ConsumeMode.CLUSTERING.getCode());
        assertEquals("集群消费", ConsumeMode.CLUSTERING.getDescription());
        assertEquals("BROADCASTING", ConsumeMode.BROADCASTING.getCode());
        assertEquals("广播消费", ConsumeMode.BROADCASTING.getDescription());
        
        // 测试从代码获取模式
        assertEquals(ConsumeMode.CLUSTERING, ConsumeMode.fromCode("CLUSTERING"));
        assertEquals(ConsumeMode.BROADCASTING, ConsumeMode.fromCode("BROADCASTING"));
        assertEquals(ConsumeMode.CLUSTERING, ConsumeMode.fromCode("UNKNOWN"));
        assertEquals(ConsumeMode.CLUSTERING, ConsumeMode.fromCode(null));
    }
    
    @Test
    @DisplayName("消费类型测试")
    void testConsumeType() {
        assertEquals("CONSUME_ACTIVELY", ConsumeType.CONSUME_ACTIVELY.getCode());
        assertEquals("主动拉取消费", ConsumeType.CONSUME_ACTIVELY.getDescription());
        assertEquals("CONSUME_PASSIVELY", ConsumeType.CONSUME_PASSIVELY.getCode());
        assertEquals("被动推送消费", ConsumeType.CONSUME_PASSIVELY.getDescription());
        
        // 测试从代码获取类型
        assertEquals(ConsumeType.CONSUME_ACTIVELY, ConsumeType.fromCode("CONSUME_ACTIVELY"));
        assertEquals(ConsumeType.CONSUME_PASSIVELY, ConsumeType.fromCode("CONSUME_PASSIVELY"));
        assertEquals(ConsumeType.CONSUME_ACTIVELY, ConsumeType.fromCode("UNKNOWN"));
        assertEquals(ConsumeType.CONSUME_ACTIVELY, ConsumeType.fromCode(null));
    }
    
    @Test
    @DisplayName("消费起始位置测试")
    void testConsumeFromWhere() {
        assertEquals("CONSUME_FROM_LAST_OFFSET", ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET.getCode());
        assertEquals("CONSUME_FROM_FIRST_OFFSET", ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET.getCode());
        assertEquals("CONSUME_FROM_TIMESTAMP", ConsumeFromWhere.CONSUME_FROM_TIMESTAMP.getCode());
        assertEquals("CONSUME_FROM_STORED_OFFSET", ConsumeFromWhere.CONSUME_FROM_STORED_OFFSET.getCode());
        
        // 测试从代码获取位置
        assertEquals(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, ConsumeFromWhere.fromCode("CONSUME_FROM_LAST_OFFSET"));
        assertEquals(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET, ConsumeFromWhere.fromCode("CONSUME_FROM_FIRST_OFFSET"));
        assertEquals(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, ConsumeFromWhere.fromCode("UNKNOWN"));
        assertEquals(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, ConsumeFromWhere.fromCode(null));
    }
    
    @Test
    @DisplayName("消费状态测试")
    void testConsumeStatus() {
        // 测试状态判断
        assertTrue(ConsumeStatus.CONSUME_SUCCESS.isSuccess());
        assertFalse(ConsumeStatus.CONSUME_SUCCESS.needRetry());
        assertFalse(ConsumeStatus.CONSUME_SUCCESS.isTimeout());
        
        assertFalse(ConsumeStatus.RECONSUME_LATER.isSuccess());
        assertTrue(ConsumeStatus.RECONSUME_LATER.needRetry());
        assertFalse(ConsumeStatus.RECONSUME_LATER.isTimeout());
        
        assertFalse(ConsumeStatus.CONSUME_EXCEPTION.isSuccess());
        assertTrue(ConsumeStatus.CONSUME_EXCEPTION.needRetry());
        assertFalse(ConsumeStatus.CONSUME_EXCEPTION.isTimeout());
        
        assertFalse(ConsumeStatus.CONSUME_TIMEOUT.isSuccess());
        assertFalse(ConsumeStatus.CONSUME_TIMEOUT.needRetry());
        assertTrue(ConsumeStatus.CONSUME_TIMEOUT.isTimeout());
        
        // 测试从代码获取状态
        assertEquals(ConsumeStatus.CONSUME_SUCCESS, ConsumeStatus.fromCode("CONSUME_SUCCESS"));
        assertEquals(ConsumeStatus.RECONSUME_LATER, ConsumeStatus.fromCode("RECONSUME_LATER"));
        assertEquals(ConsumeStatus.CONSUME_EXCEPTION, ConsumeStatus.fromCode("UNKNOWN"));
        assertEquals(ConsumeStatus.CONSUME_EXCEPTION, ConsumeStatus.fromCode(null));
    }
    
    @Test
    @DisplayName("拉取状态测试")
    void testPullStatus() {
        // 测试状态判断
        assertTrue(PullStatus.FOUND.isSuccess());
        assertTrue(PullStatus.FOUND.hasMessage());
        assertFalse(PullStatus.FOUND.needRetry());
        assertFalse(PullStatus.FOUND.isTimeout());
        
        assertTrue(PullStatus.NO_NEW_MSG.isSuccess());
        assertFalse(PullStatus.NO_NEW_MSG.hasMessage());
        assertFalse(PullStatus.NO_NEW_MSG.needRetry());
        
        assertFalse(PullStatus.OFFSET_ILLEGAL.isSuccess());
        assertFalse(PullStatus.OFFSET_ILLEGAL.hasMessage());
        assertTrue(PullStatus.OFFSET_ILLEGAL.needRetry());
        
        assertFalse(PullStatus.BROKER_TIMEOUT.isSuccess());
        assertTrue(PullStatus.BROKER_TIMEOUT.needRetry());
        assertTrue(PullStatus.BROKER_TIMEOUT.isTimeout());
        
        // 测试从代码获取状态
        assertEquals(PullStatus.FOUND, PullStatus.fromCode("FOUND"));
        assertEquals(PullStatus.NO_NEW_MSG, PullStatus.fromCode("NO_NEW_MSG"));
        assertEquals(PullStatus.PULL_FAILED, PullStatus.fromCode("UNKNOWN"));
        assertEquals(PullStatus.PULL_FAILED, PullStatus.fromCode(null));
    }
    
    @Test
    @DisplayName("确认状态测试")
    void testAckStatus() {
        // 测试状态判断
        assertTrue(AckStatus.ACK_OK.isSuccess());
        assertFalse(AckStatus.ACK_OK.isFailure());
        assertFalse(AckStatus.ACK_OK.isTimeout());
        assertFalse(AckStatus.ACK_OK.needRetry());
        
        assertFalse(AckStatus.ACK_FAILED.isSuccess());
        assertTrue(AckStatus.ACK_FAILED.isFailure());
        assertFalse(AckStatus.ACK_FAILED.isTimeout());
        assertTrue(AckStatus.ACK_FAILED.needRetry());
        
        assertFalse(AckStatus.ACK_TIMEOUT.isSuccess());
        assertFalse(AckStatus.ACK_TIMEOUT.isFailure());
        assertTrue(AckStatus.ACK_TIMEOUT.isTimeout());
        assertTrue(AckStatus.ACK_TIMEOUT.needRetry());
        
        // 测试从代码获取状态
        assertEquals(AckStatus.ACK_OK, AckStatus.fromCode("ACK_OK"));
        assertEquals(AckStatus.ACK_FAILED, AckStatus.fromCode("ACK_FAILED"));
        assertEquals(AckStatus.ACK_FAILED, AckStatus.fromCode("UNKNOWN"));
        assertEquals(AckStatus.ACK_FAILED, AckStatus.fromCode(null));
    }
    
    @Test
    @DisplayName("拉取结果测试")
    void testPullResult() {
        // 成功结果（有消息）
        List<Message> messages = new ArrayList<>();
        messages.add(new Message("test-topic", "test-tag", "Hello".getBytes()));
        PullResult foundResult = PullResult.found(100, 0, 200, messages);
        
        assertTrue(foundResult.isSuccess());
        assertTrue(foundResult.hasMessage());
        assertEquals(1, foundResult.getMessageCount());
        assertEquals(PullStatus.FOUND, foundResult.getPullStatus());
        assertEquals(100, foundResult.getNextBeginOffset());
        
        // 成功结果（无新消息）
        PullResult noNewMsgResult = PullResult.noNewMessage(50, 0, 50);
        assertTrue(noNewMsgResult.isSuccess());
        assertFalse(noNewMsgResult.hasMessage());
        assertEquals(0, noNewMsgResult.getMessageCount());
        assertEquals(PullStatus.NO_NEW_MSG, noNewMsgResult.getPullStatus());
        
        // 失败结果
        PullResult failureResult = PullResult.failure("拉取失败");
        assertFalse(failureResult.isSuccess());
        assertFalse(failureResult.hasMessage());
        assertEquals(PullStatus.PULL_FAILED, failureResult.getPullStatus());
        assertEquals("拉取失败", failureResult.getErrorMessage());
        
        // 偏移量非法结果
        PullResult offsetIllegalResult = PullResult.offsetIllegal(10, 0, 100);
        assertFalse(offsetIllegalResult.isSuccess());
        assertTrue(offsetIllegalResult.needRetry());
        assertEquals(PullStatus.OFFSET_ILLEGAL, offsetIllegalResult.getPullStatus());
        
        // 超时结果
        PullResult timeoutResult = PullResult.timeout();
        assertFalse(timeoutResult.isSuccess());
        assertTrue(timeoutResult.needRetry());
        assertEquals(PullStatus.BROKER_TIMEOUT, timeoutResult.getPullStatus());
    }
    
    @Test
    @DisplayName("确认结果测试")
    void testAckResult() {
        // 成功结果
        AckResult successResult = AckResult.success("msg-123");
        assertTrue(successResult.isSuccess());
        assertTrue(successResult.hasSuccessMessage());
        assertFalse(successResult.hasFailedMessage());
        assertEquals(1, successResult.getSuccessCount());
        assertEquals(0, successResult.getFailedCount());
        assertEquals(AckStatus.ACK_OK, successResult.getAckStatus());
        
        // 批量成功结果
        List<String> messageIds = new ArrayList<>();
        messageIds.add("msg-1");
        messageIds.add("msg-2");
        AckResult batchSuccessResult = AckResult.success(messageIds);
        assertTrue(batchSuccessResult.isSuccess());
        assertEquals(2, batchSuccessResult.getSuccessCount());
        
        // 失败结果
        AckResult failureResult = AckResult.failure("确认失败");
        assertFalse(failureResult.isSuccess());
        assertFalse(failureResult.hasSuccessMessage());
        assertEquals(AckStatus.ACK_FAILED, failureResult.getAckStatus());
        assertEquals("确认失败", failureResult.getErrorMessage());
        
        // 超时结果
        AckResult timeoutResult = AckResult.timeout();
        assertFalse(timeoutResult.isSuccess());
        assertEquals(AckStatus.ACK_TIMEOUT, timeoutResult.getAckStatus());
        
        // 消息不存在结果
        AckResult notExistResult = AckResult.messageNotExist();
        assertFalse(notExistResult.isSuccess());
        assertEquals(AckStatus.MESSAGE_NOT_EXIST, notExistResult.getAckStatus());
    }
    
    @Test
    @DisplayName("订阅数据测试")
    void testSubscriptionData() {
        MessageListener listener = message -> ConsumeStatus.CONSUME_SUCCESS;
        
        // 创建订阅数据
        SubscriptionData subscription = new SubscriptionData("test-topic", "tag1,tag2", listener);
        assertTrue(subscription.isValid());
        assertEquals("test-topic", subscription.getTopic());
        assertEquals("tag1,tag2", subscription.getSubString());
        assertEquals(listener, subscription.getMessageListener());
        assertEquals(2, subscription.getTagCount());
        assertTrue(subscription.matchTag("tag1"));
        assertTrue(subscription.matchTag("tag2"));
        assertFalse(subscription.matchTag("tag3"));
        
        // 测试订阅所有标签
        SubscriptionData allTagsSubscription = new SubscriptionData("test-topic", "*", listener);
        assertTrue(allTagsSubscription.isSubscribeAllTags());
        assertTrue(allTagsSubscription.matchTag("any-tag"));
        
        // 测试标签操作
        subscription.addTag("tag3");
        assertEquals(3, subscription.getTagCount());
        assertTrue(subscription.matchTag("tag3"));
        
        subscription.removeTag("tag1");
        assertEquals(2, subscription.getTagCount());
        assertFalse(subscription.matchTag("tag1"));
        
        subscription.clearTags();
        assertEquals(0, subscription.getTagCount());
        
        // 测试复制
        SubscriptionData copy = subscription.copy();
        assertEquals(subscription.getTopic(), copy.getTopic());
        assertEquals(subscription.getSubString(), copy.getSubString());
        assertEquals(subscription.getVersion(), copy.getVersion());
    }
    
    @Test
    @DisplayName("Consumer统计信息测试")
    void testConsumerStats() {
        ConsumerStats stats = new ConsumerStats();
        
        // 初始状态
        assertEquals(0, stats.getConsumeSuccessCount());
        assertEquals(0, stats.getConsumeFailureCount());
        assertEquals(0, stats.getConsumeTotalCount());
        assertEquals(0, stats.getPullSuccessCount());
        assertEquals(0, stats.getPullFailureCount());
        assertEquals(0.0, stats.getConsumeSuccessRate());
        assertEquals(0.0, stats.getAverageConsumeLatency());
        
        // 记录消费成功
        stats.recordConsumeSuccess(100, 1024);
        stats.recordConsumeSuccess(200, 2048);
        
        assertEquals(2, stats.getConsumeSuccessCount());
        assertEquals(0, stats.getConsumeFailureCount());
        assertEquals(2, stats.getConsumeTotalCount());
        assertEquals(3072, stats.getConsumeTotalBytes());
        assertEquals(100.0, stats.getConsumeSuccessRate());
        assertEquals(150.0, stats.getAverageConsumeLatency());
        assertEquals(1536.0, stats.getAverageMessageSize());
        assertEquals(200, stats.getMaxConsumeLatency());
        assertEquals(100, stats.getMinConsumeLatency());
        
        // 记录消费失败
        stats.recordConsumeFailure(300);
        
        assertEquals(2, stats.getConsumeSuccessCount());
        assertEquals(1, stats.getConsumeFailureCount());
        assertEquals(3, stats.getConsumeTotalCount());
        assertEquals(66.67, stats.getConsumeSuccessRate(), 0.01);
        assertEquals(33.33, stats.getConsumeFailureRate(), 0.01);
        assertEquals(200.0, stats.getAverageConsumeLatency());
        
        // 记录拉取成功
        stats.recordPullSuccess(50, 5);
        stats.recordPullSuccess(100, 3);
        
        assertEquals(2, stats.getPullSuccessCount());
        assertEquals(0, stats.getPullFailureCount());
        assertEquals(8, stats.getPullMessageCount());
        assertEquals(100.0, stats.getPullSuccessRate());
        assertEquals(75.0, stats.getAveragePullLatency());
        
        // 重置统计
        stats.reset();
        assertEquals(0, stats.getConsumeTotalCount());
        assertEquals(0, stats.getPullTotalCount());
    }
    
    @Test
    @DisplayName("Consumer构造函数验证")
    void testConsumerConstructor() {
        // 正常构造
        Consumer normalConsumer = new ConsumerImpl(config);
        assertEquals(ConsumerStatus.CREATE_JUST, normalConsumer.getStatus());
        
        // 空配置
        assertThrows(IllegalArgumentException.class, () -> new ConsumerImpl(null));
        
        // 无效配置
        ConsumerConfig invalidConfig = new ConsumerConfig();
        invalidConfig.setConsumerGroup(null);
        assertThrows(IllegalArgumentException.class, () -> new ConsumerImpl(invalidConfig));
    }
    
    @Test
    @DisplayName("订阅和取消订阅测试")
    void testSubscribeAndUnsubscribe() {
        MessageListener listener = message -> ConsumeStatus.CONSUME_SUCCESS;
        
        // 初始状态
        assertTrue(consumer.getSubscriptions().isEmpty());
        
        // 订阅Topic
        consumer.subscribe("test-topic", "tag1,tag2", listener);
        Map<String, SubscriptionData> subscriptions = consumer.getSubscriptions();
        assertEquals(1, subscriptions.size());
        assertTrue(subscriptions.containsKey("test-topic"));
        
        SubscriptionData subscription = subscriptions.get("test-topic");
        assertEquals("test-topic", subscription.getTopic());
        assertEquals("tag1,tag2", subscription.getSubString());
        assertEquals(listener, subscription.getMessageListener());
        
        // 取消订阅
        consumer.unsubscribe("test-topic");
        assertTrue(consumer.getSubscriptions().isEmpty());
        
        // 订阅参数验证
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(null, "tag", listener));
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe("", "tag", listener));
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe("topic", "tag", null));
    }
    
    @Test
    @DisplayName("消息监听器测试")
    void testMessageListener() {
        AtomicInteger consumeCount = new AtomicInteger(0);
        
        MessageListener listener = new MessageListener() {
            @Override
            public ConsumeStatus consumeMessage(Message message) {
                consumeCount.incrementAndGet();
                return ConsumeStatus.CONSUME_SUCCESS;
            }
        };
        
        // 测试单条消息消费
        Message message = new Message("test-topic", "test-tag", "Hello".getBytes());
        ConsumeStatus status = listener.consumeMessage(message);
        assertEquals(ConsumeStatus.CONSUME_SUCCESS, status);
        assertEquals(1, consumeCount.get());
        
        // 测试批量消息消费
        List<Message> messages = new ArrayList<>();
        messages.add(new Message("test-topic", "test-tag", "Hello1".getBytes()));
        messages.add(new Message("test-topic", "test-tag", "Hello2".getBytes()));
        
        ConsumeStatus batchStatus = listener.consumeMessages(messages);
        assertEquals(ConsumeStatus.CONSUME_SUCCESS, batchStatus);
        assertEquals(3, consumeCount.get()); // 1 + 2
    }
}
