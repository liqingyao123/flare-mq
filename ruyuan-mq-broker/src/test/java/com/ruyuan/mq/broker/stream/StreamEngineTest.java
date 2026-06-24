package com.ruyuan.mq.broker.stream;

import com.ruyuan.mq.store.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 流处理引擎测试
 * 
 * @author RuYuan MQ Team
 */
public class StreamEngineTest {
    
    private static final Logger logger = LoggerFactory.getLogger(StreamEngineTest.class);
    
    private StreamAPI streamAPI;
    
    @BeforeEach
    void setUp() {
        streamAPI = new StreamAPI();
        logger.info("=== 开始流处理引擎测试 ===");
    }
    
    @AfterEach
    void tearDown() {
        if (streamAPI != null) {
            streamAPI.shutdown();
        }
        logger.info("=== 流处理引擎测试结束 ===");
    }
    
    @Test
    void testBasicStreamCreation() {
        logger.info("--- 测试基本流创建 ---");
        
        // 创建流
        StreamAPI.MessageStream stream = streamAPI.createStream("test-topic");
        assertNotNull(stream);
        assertEquals(StreamStatus.INITIALIZED, stream.getStatus());
        
        // 启动流
        stream.start();
        assertEquals(StreamStatus.RUNNING, stream.getStatus());
        
        // 停止流
        stream.stop();
        assertEquals(StreamStatus.STOPPED, stream.getStatus());
        
        logger.info("基本流创建测试通过");
    }
    
    @Test
    void testFilterOperation() {
        logger.info("--- 测试过滤操作 ---");
        
        StreamAPI.MessageStream stream = streamAPI.createStream("filter-topic")
                .filter(message -> message.getTags() != null && message.getTags().contains("important"));
        stream.to("filtered-output");
        
        assertNotNull(stream);
        stream.start();
        assertEquals(StreamStatus.RUNNING, stream.getStatus());
        
        logger.info("过滤操作测试通过");
    }
    
    @Test
    void testMapOperation() {
        logger.info("--- 测试映射操作 ---");
        
        StreamAPI.MessageStream stream = streamAPI.createStream("map-topic")
                .map(message -> {
                    // 转换消息内容
                    Message transformed = new Message();
                    transformed.setTopic(message.getTopic());
                    transformed.setTags("transformed");
                    transformed.setKeys(message.getKeys());
                    transformed.setBody(("TRANSFORMED: " + new String(message.getBody())).getBytes());
                    transformed.setBornTimestamp(System.currentTimeMillis());
                    return transformed;
                });
        stream.to("mapped-output");
        
        assertNotNull(stream);
        stream.start();
        assertEquals(StreamStatus.RUNNING, stream.getStatus());
        
        logger.info("映射操作测试通过");
    }
    
    @Test
    void testTimeWindowOperation() {
        logger.info("--- 测试时间窗口操作 ---");
        
        StreamAPI.MessageStream stream = streamAPI.createStream("window-topic")
                .window(WindowSpec.timeWindow(Duration.ofSeconds(5)))
                .aggregate(AggregateFunctions.count());
        stream.to("windowed-output");
        
        assertNotNull(stream);
        stream.start();
        assertEquals(StreamStatus.RUNNING, stream.getStatus());
        
        logger.info("时间窗口操作测试通过");
    }
    
    @Test
    void testCountWindowOperation() {
        logger.info("--- 测试计数窗口操作 ---");
        
        StreamAPI.MessageStream stream = streamAPI.createStream("count-window-topic")
                .window(WindowSpec.countWindow(10))
                .aggregate(AggregateFunctions.sum("amount"));
        stream.to("count-windowed-output");
        
        assertNotNull(stream);
        stream.start();
        assertEquals(StreamStatus.RUNNING, stream.getStatus());
        
        logger.info("计数窗口操作测试通过");
    }
    
    @Test
    void testComplexStreamPipeline() {
        logger.info("--- 测试复杂流处理管道 ---");
        
        StreamAPI.MessageStream stream = streamAPI.createStream("complex-topic")
                .filter(message -> message.getTags() != null && message.getTags().contains("order"))
                .map(message -> {
                    message.putProperty("processed", "true");
                    message.putProperty("timestamp", String.valueOf(System.currentTimeMillis()));
                    return message;
                })
                .window(WindowSpec.timeWindow(Duration.ofSeconds(10)))
                .aggregate(AggregateFunctions.count());
        stream.to("complex-output");
        
        assertNotNull(stream);
        stream.start();
        assertEquals(StreamStatus.RUNNING, stream.getStatus());
        
        logger.info("复杂流处理管道测试通过");
    }
    
    @Test
    void testStateManager() {
        logger.info("--- 测试状态管理器 ---");
        
        StreamEngine engine = new StreamEngine();
        StateManager stateManager = engine.getStateManager();
        
        // 测试状态存储和获取
        stateManager.putState("test-key", "test-value");
        String value = stateManager.getState("test-key", String.class);
        assertEquals("test-value", value);
        
        // 测试状态存在性检查
        assertTrue(stateManager.containsState("test-key"));
        assertFalse(stateManager.containsState("non-existent-key"));
        
        // 测试状态删除
        stateManager.removeState("test-key");
        assertFalse(stateManager.containsState("test-key"));
        
        // 测试状态统计
        StateStatistics stats = stateManager.getStatistics();
        assertNotNull(stats);
        assertEquals(0, stats.getStateCount());
        
        engine.shutdown();
        logger.info("状态管理器测试通过");
    }
    
    @Test
    void testWindowManager() {
        logger.info("--- 测试窗口管理器 ---");
        
        WindowManager windowManager = new WindowManager();
        
        // 创建测试消息
        Message message1 = createTestMessage("test-topic", "key1", "value1");
        Message message2 = createTestMessage("test-topic", "key2", "value2");
        
        // 创建窗口规格
        WindowSpec countWindowSpec = WindowSpec.countWindow(2);

        CountDownLatch countLatch = new CountDownLatch(1);

        // 测试计数窗口（这个能正常工作）
        windowManager.processMessage(message1, countWindowSpec, window -> {
            logger.info("Count window completed with {} messages", window.getMessageCount());
        });

        windowManager.processMessage(message2, countWindowSpec, window -> {
            logger.info("Count window completed with {} messages", window.getMessageCount());
            countLatch.countDown();
        });

        try {
            // 等待计数窗口完成
            assertTrue(countLatch.await(100, TimeUnit.MILLISECONDS), "Count window should complete immediately");

            // 测试窗口管理器的基本功能
            assertEquals(0, windowManager.getActiveWindowCount()); // 窗口应该已经被清理

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Test interrupted");
        }
        
        windowManager.shutdown();
        logger.info("窗口管理器测试通过");
    }
    
    @Test
    void testAggregateFunction() {
        logger.info("--- 测试聚合函数 ---");
        
        // 创建测试消息
        Message msg1 = createTestMessage("test-topic", "key1", "value1");
        msg1.putProperty("amount", "100");
        
        Message msg2 = createTestMessage("test-topic", "key2", "value2");
        msg2.putProperty("amount", "200");
        
        Message msg3 = createTestMessage("test-topic", "key3", "value3");
        msg3.putProperty("amount", "300");
        
        java.util.List<Message> messages = java.util.Arrays.asList(msg1, msg2, msg3);
        
        // 测试计数聚合
        AggregateFunction countFunc = AggregateFunctions.count();
        Message countResult = countFunc.aggregate(messages);
        assertEquals("3", new String(countResult.getBody()));
        assertEquals("count", countResult.getProperty("aggregate_type"));
        
        // 测试求和聚合
        AggregateFunction sumFunc = AggregateFunctions.sum("amount");
        Message sumResult = sumFunc.aggregate(messages);
        assertEquals("600.0", new String(sumResult.getBody()));
        assertEquals("sum", sumResult.getProperty("aggregate_type"));
        
        // 测试平均值聚合
        AggregateFunction avgFunc = AggregateFunctions.avg("amount");
        Message avgResult = avgFunc.aggregate(messages);
        assertEquals("200.0", new String(avgResult.getBody()));
        assertEquals("avg", avgResult.getProperty("aggregate_type"));
        
        // 测试最大值聚合
        AggregateFunction maxFunc = AggregateFunctions.max("amount");
        Message maxResult = maxFunc.aggregate(messages);
        assertEquals("300.0", new String(maxResult.getBody()));
        assertEquals("max", maxResult.getProperty("aggregate_type"));
        
        logger.info("聚合函数测试通过");
    }
    
    @Test
    void testStreamStatistics() {
        logger.info("--- 测试流统计信息 ---");
        
        StreamStatistics stats = streamAPI.getStatistics();
        assertNotNull(stats);
        
        // 初始状态检查
        assertEquals(0, stats.getProcessedMessages());
        assertEquals(0, stats.getFailedMessages());
        assertEquals(1.0, stats.getSuccessRate(), 0.01);
        
        // 模拟一些统计数据
        stats.incrementProcessedMessages();
        stats.incrementProcessedMessages();
        stats.incrementFailedMessages();
        stats.recordProcessingTime(100);
        stats.recordProcessingTime(200);
        
        assertEquals(2, stats.getProcessedMessages());
        assertEquals(1, stats.getFailedMessages());
        assertEquals(0.5, stats.getSuccessRate(), 0.01);
        assertEquals(150.0, stats.getAverageProcessingTime(), 0.01);
        
        logger.info("流统计信息: {}", stats.getSummary());
        logger.info("流统计信息测试通过");
    }
    
    /**
     * 创建测试消息
     */
    private Message createTestMessage(String topic, String key, String value) {
        Message message = new Message();
        message.setTopic(topic);
        message.setKeys(key);
        message.setBody(value.getBytes());
        message.setBornTimestamp(System.currentTimeMillis());
        return message;
    }
}
