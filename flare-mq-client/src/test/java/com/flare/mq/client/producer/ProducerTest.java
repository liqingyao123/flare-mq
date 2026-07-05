package com.flare.mq.client.producer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Producer测试类
 * 
 * @author FlareMQ Team
 */
@DisplayName("Producer测试")
class ProducerTest {
    
    private ProducerConfig config;
    private Producer producer;
    
    @BeforeEach
    void setUp() {
        config = new ProducerConfig();
        config.setProducerGroup("TEST_PRODUCER");
        config.setNameServerAddr("localhost:9876");
        config.setSendMsgTimeout(3000);
        
        producer = new ProducerImpl(config);
    }
    
    @Test
    @DisplayName("Producer配置测试")
    void testProducerConfig() {
        // 测试默认配置
        ProducerConfig defaultConfig = new ProducerConfig();
        assertTrue(defaultConfig.isValid());
        assertEquals("DEFAULT_PRODUCER", defaultConfig.getProducerGroup());
        assertEquals("localhost:9876", defaultConfig.getNameServerAddr());
        assertEquals(3000, defaultConfig.getSendMsgTimeout());
        assertEquals(2, defaultConfig.getRetryTimesWhenSendFailed());
        
        // 测试自定义配置
        assertEquals("TEST_PRODUCER", config.getProducerGroup());
        assertEquals("localhost:9876", config.getNameServerAddr());
        assertEquals(3000, config.getSendMsgTimeout());
        
        // 测试配置复制
        ProducerConfig copy = config.copy();
        assertEquals(config.getProducerGroup(), copy.getProducerGroup());
        assertEquals(config.getNameServerAddr(), copy.getNameServerAddr());
        assertEquals(config.getSendMsgTimeout(), copy.getSendMsgTimeout());
    }
    
    @Test
    @DisplayName("Producer配置验证")
    void testProducerConfigValidation() {
        // 有效配置
        assertTrue(config.isValid());
        
        // 无效配置 - 空Producer组
        ProducerConfig invalidConfig1 = new ProducerConfig();
        invalidConfig1.setProducerGroup(null);
        assertFalse(invalidConfig1.isValid());
        
        invalidConfig1.setProducerGroup("");
        assertFalse(invalidConfig1.isValid());
        
        // 无效配置 - 空NameServer地址
        ProducerConfig invalidConfig2 = new ProducerConfig();
        invalidConfig2.setNameServerAddr(null);
        assertFalse(invalidConfig2.isValid());
        
        invalidConfig2.setNameServerAddr("");
        assertFalse(invalidConfig2.isValid());
        
        // 无效配置 - 超时时间
        ProducerConfig invalidConfig3 = new ProducerConfig();
        invalidConfig3.setSendMsgTimeout(0);
        assertFalse(invalidConfig3.isValid());
        
        invalidConfig3.setSendMsgTimeout(-1);
        assertFalse(invalidConfig3.isValid());
    }
    
    @Test
    @DisplayName("Producer状态测试")
    void testProducerStatus() {
        // 初始状态
        assertEquals(ProducerStatus.CREATE_JUST, producer.getStatus());
        assertFalse(producer.getStatus().canSendMessage());
        assertFalse(producer.getStatus().isStarted());
        assertFalse(producer.getStatus().isShutdown());
        
        // 测试状态枚举
        assertTrue(ProducerStatus.RUNNING.canSendMessage());
        assertTrue(ProducerStatus.RUNNING.isStarted());
        assertFalse(ProducerStatus.RUNNING.isShutdown());
        
        assertFalse(ProducerStatus.SHUTDOWN_ALREADY.canSendMessage());
        assertFalse(ProducerStatus.SHUTDOWN_ALREADY.isStarted());
        assertTrue(ProducerStatus.SHUTDOWN_ALREADY.isShutdown());
    }
    
    @Test
    @DisplayName("消息创建和验证测试")
    void testMessage() {
        // 创建有效消息
        Message message = new Message("test-topic", "test-tag", "Hello World".getBytes());
        assertTrue(message.isValid());
        assertEquals("test-topic", message.getTopic());
        assertEquals("test-tag", message.getTags());
        assertArrayEquals("Hello World".getBytes(), message.getBody());
        
        // 设置消息属性
        message.setKey("test-key");
        message.setPriority(8);
        message.setDelayTime(1000);
        message.putProperty("custom", "value");
        
        assertEquals("test-key", message.getKey());
        assertEquals(8, message.getPriority());
        assertEquals(1000, message.getDelayTime());
        assertEquals("value", message.getProperty("custom"));
        
        // 测试消息大小计算
        assertTrue(message.getMessageSize() > 0);
        
        // 测试消息复制
        Message copy = message.copy();
        assertEquals(message.getTopic(), copy.getTopic());
        assertEquals(message.getTags(), copy.getTags());
        assertEquals(message.getKey(), copy.getKey());
        assertArrayEquals(message.getBody(), copy.getBody());
        assertEquals(message.getPriority(), copy.getPriority());
        assertEquals(message.getProperty("custom"), copy.getProperty("custom"));
        
        // 测试无效消息
        Message invalidMessage1 = new Message();
        assertFalse(invalidMessage1.isValid());
        
        Message invalidMessage2 = new Message(null, "tag", "body".getBytes());
        assertFalse(invalidMessage2.isValid());
        
        Message invalidMessage3 = new Message("topic", "tag", null);
        assertFalse(invalidMessage3.isValid());
    }
    
    @Test
    @DisplayName("发送结果测试")
    void testSendResult() {
        // 成功结果
        SendResult successResult = SendResult.success("msg-123", 1, 1000L);
        assertTrue(successResult.isSuccess());
        assertFalse(successResult.needRetry());
        assertEquals(SendStatus.SEND_OK, successResult.getSendStatus());
        assertEquals("msg-123", successResult.getMessageId());
        assertEquals(1, successResult.getQueueId());
        assertEquals(1000L, successResult.getQueueOffset());
        
        // 失败结果
        SendResult failureResult = SendResult.failure("发送失败");
        assertFalse(failureResult.isSuccess());
        assertFalse(failureResult.needRetry());
        assertEquals(SendStatus.SEND_FAILED, failureResult.getSendStatus());
        assertEquals("发送失败", failureResult.getErrorMessage());
        
        // 超时结果
        SendResult timeoutResult = SendResult.timeout("发送超时");
        assertFalse(timeoutResult.isSuccess());
        assertFalse(timeoutResult.needRetry());
        assertEquals(SendStatus.SEND_TIMEOUT, timeoutResult.getSendStatus());
        
        // 刷盘超时结果
        SendResult flushDiskTimeoutResult = SendResult.flushDiskTimeout();
        assertFalse(flushDiskTimeoutResult.isSuccess());
        assertTrue(flushDiskTimeoutResult.needRetry());
        assertEquals(SendStatus.FLUSH_DISK_TIMEOUT, flushDiskTimeoutResult.getSendStatus());
        
        // 从节点超时结果
        SendResult flushSlaveTimeoutResult = SendResult.flushSlaveTimeout();
        assertFalse(flushSlaveTimeoutResult.isSuccess());
        assertTrue(flushSlaveTimeoutResult.needRetry());
        assertEquals(SendStatus.FLUSH_SLAVE_TIMEOUT, flushSlaveTimeoutResult.getSendStatus());
        
        // 从节点不可用结果
        SendResult slaveNotAvailableResult = SendResult.slaveNotAvailable();
        assertFalse(slaveNotAvailableResult.isSuccess());
        assertTrue(slaveNotAvailableResult.needRetry());
        assertEquals(SendStatus.SLAVE_NOT_AVAILABLE, slaveNotAvailableResult.getSendStatus());
    }
    
    @Test
    @DisplayName("发送状态测试")
    void testSendStatus() {
        // 测试状态判断
        assertTrue(SendStatus.SEND_OK.isSuccess());
        assertFalse(SendStatus.SEND_OK.isFailure());
        assertFalse(SendStatus.SEND_OK.isTimeout());
        assertFalse(SendStatus.SEND_OK.needRetry());
        
        assertFalse(SendStatus.SEND_FAILED.isSuccess());
        assertTrue(SendStatus.SEND_FAILED.isFailure());
        assertFalse(SendStatus.SEND_FAILED.isTimeout());
        assertFalse(SendStatus.SEND_FAILED.needRetry());
        
        assertFalse(SendStatus.SEND_TIMEOUT.isSuccess());
        assertFalse(SendStatus.SEND_TIMEOUT.isFailure());
        assertTrue(SendStatus.SEND_TIMEOUT.isTimeout());
        assertFalse(SendStatus.SEND_TIMEOUT.needRetry());
        
        assertFalse(SendStatus.FLUSH_DISK_TIMEOUT.isSuccess());
        assertFalse(SendStatus.FLUSH_DISK_TIMEOUT.isFailure());
        assertTrue(SendStatus.FLUSH_DISK_TIMEOUT.isTimeout());
        assertTrue(SendStatus.FLUSH_DISK_TIMEOUT.needRetry());
        
        // 测试从代码获取状态
        assertEquals(SendStatus.SEND_OK, SendStatus.fromCode("SEND_OK"));
        assertEquals(SendStatus.SEND_FAILED, SendStatus.fromCode("SEND_FAILED"));
        assertEquals(SendStatus.SEND_FAILED, SendStatus.fromCode("UNKNOWN"));
        assertEquals(SendStatus.SEND_FAILED, SendStatus.fromCode(null));
    }
    
    @Test
    @DisplayName("Producer统计信息测试")
    void testProducerStats() {
        ProducerStats stats = new ProducerStats();
        
        // 初始状态
        assertEquals(0, stats.getSendSuccessCount());
        assertEquals(0, stats.getSendFailureCount());
        assertEquals(0, stats.getSendTimeoutCount());
        assertEquals(0, stats.getSendTotalCount());
        assertEquals(0, stats.getSendTotalBytes());
        assertEquals(0.0, stats.getSendSuccessRate());
        assertEquals(0.0, stats.getAverageSendLatency());
        
        // 记录发送成功
        stats.recordSendSuccess(100, 1024);
        stats.recordSendSuccess(200, 2048);
        
        assertEquals(2, stats.getSendSuccessCount());
        assertEquals(0, stats.getSendFailureCount());
        assertEquals(2, stats.getSendTotalCount());
        assertEquals(3072, stats.getSendTotalBytes());
        assertEquals(100.0, stats.getSendSuccessRate());
        assertEquals(150.0, stats.getAverageSendLatency());
        assertEquals(1536.0, stats.getAverageMessageSize());
        assertEquals(200, stats.getMaxSendLatency());
        assertEquals(100, stats.getMinSendLatency());
        
        // 记录发送失败
        stats.recordSendFailure(300);
        
        assertEquals(2, stats.getSendSuccessCount());
        assertEquals(1, stats.getSendFailureCount());
        assertEquals(3, stats.getSendTotalCount());
        assertEquals(66.67, stats.getSendSuccessRate(), 0.01);
        assertEquals(33.33, stats.getSendFailureRate(), 0.01);
        assertEquals(200.0, stats.getAverageSendLatency());
        
        // 记录发送超时
        stats.recordSendTimeout(400);
        
        assertEquals(2, stats.getSendSuccessCount());
        assertEquals(1, stats.getSendFailureCount());
        assertEquals(1, stats.getSendTimeoutCount());
        assertEquals(4, stats.getSendTotalCount());
        assertEquals(50.0, stats.getSendSuccessRate());
        assertEquals(50.0, stats.getSendFailureRate());
        
        // 重置统计
        stats.reset();
        assertEquals(0, stats.getSendTotalCount());
        assertEquals(0, stats.getSendTotalBytes());
    }
    
    @Test
    @DisplayName("Producer构造函数验证")
    void testProducerConstructor() {
        // 正常构造
        Producer normalProducer = new ProducerImpl(config);
        assertEquals(ProducerStatus.CREATE_JUST, normalProducer.getStatus());
        
        // 空配置
        assertThrows(IllegalArgumentException.class, () -> new ProducerImpl(null));
        
        // 无效配置
        ProducerConfig invalidConfig = new ProducerConfig();
        invalidConfig.setProducerGroup(null);
        assertThrows(IllegalArgumentException.class, () -> new ProducerImpl(invalidConfig));
    }
    
    @Test
    @DisplayName("发送回调测试")
    void testSendCallback() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<SendResult> resultRef = new AtomicReference<>();
        AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
        
        SendCallback callback = new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                resultRef.set(sendResult);
                latch.countDown();
            }
            
            @Override
            public void onException(Throwable exception) {
                exceptionRef.set(exception);
                latch.countDown();
            }
        };
        
        // 模拟成功回调
        SendResult successResult = SendResult.success("msg-123", 1, 1000L);
        callback.onSuccess(successResult);
        
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertEquals(successResult, resultRef.get());
        assertNull(exceptionRef.get());
        
        // 模拟异常回调
        CountDownLatch latch2 = new CountDownLatch(1);
        AtomicReference<Throwable> exceptionRef2 = new AtomicReference<>();
        
        SendCallback callback2 = new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                // 不应该被调用
            }
            
            @Override
            public void onException(Throwable exception) {
                exceptionRef2.set(exception);
                latch2.countDown();
            }
        };
        
        RuntimeException testException = new RuntimeException("测试异常");
        callback2.onException(testException);
        
        assertTrue(latch2.await(1, TimeUnit.SECONDS));
        assertEquals(testException, exceptionRef2.get());
    }

    @Test
    @DisplayName("isRetryableError — 可重试状态返回true")
    void testIsRetryableError_Retryable() {
        // SEND_TIMEOUT is not retryable per current SendStatus.needRetry()
        // But FLUSH_DISK_TIMEOUT, FLUSH_SLAVE_TIMEOUT, SLAVE_NOT_AVAILABLE are
        assertTrue(SendResult.flushDiskTimeout().needRetry());
        assertTrue(SendResult.flushSlaveTimeout().needRetry());
        assertTrue(SendResult.slaveNotAvailable().needRetry());
    }

    @Test
    @DisplayName("isRetryableError — SEND_FAILED 不重试")
    void testIsRetryableError_NotRetryable() {
        SendResult failureResult = SendResult.failure("Broker rejected");
        assertFalse(failureResult.needRetry());
    }

    @Test
    @DisplayName("TopicRouteInfo.selectAnotherQueue 排除指定 broker")
    void testSelectAnotherQueue_ExcludesBroker() {
        TopicRouteInfo routeInfo = new TopicRouteInfo("test-topic");
        List<TopicRouteInfo.QueueInfo> queues = new ArrayList<>();
        queues.add(new TopicRouteInfo.QueueInfo("broker-A", 0, true, true));
        queues.add(new TopicRouteInfo.QueueInfo("broker-A", 1, true, true));
        queues.add(new TopicRouteInfo.QueueInfo("broker-B", 0, true, true));
        queues.add(new TopicRouteInfo.QueueInfo("broker-B", 1, true, true));
        routeInfo.setQueueInfos(queues);

        TopicRouteInfo.QueueInfo result = routeInfo.selectAnotherQueue("broker-A");
        assertNotNull(result);
        assertEquals("broker-B", result.getBrokerName());
    }

    @Test
    @DisplayName("TopicRouteInfo.selectAnotherQueue — 排除后无备选返回 null")
    void testSelectAnotherQueue_NoAlternatives() {
        TopicRouteInfo routeInfo = new TopicRouteInfo("test-topic");
        List<TopicRouteInfo.QueueInfo> queues = new ArrayList<>();
        queues.add(new TopicRouteInfo.QueueInfo("broker-A", 0, true, true));
        routeInfo.setQueueInfos(queues);

        TopicRouteInfo.QueueInfo result = routeInfo.selectAnotherQueue("broker-A");
        assertNull(result);
    }

    @Test
    @DisplayName("SendResult.brokerAddr 被正确赋值")
    void testSendResultBrokerAddr() {
        SendResult result = SendResult.success("msg-1", 2, 4096L);
        result.setBrokerAddr("192.168.1.100:10911");
        assertEquals("192.168.1.100:10911", result.getBrokerAddr());
        assertEquals(2, result.getQueueId());
        assertEquals(4096L, result.getQueueOffset());
    }
}
