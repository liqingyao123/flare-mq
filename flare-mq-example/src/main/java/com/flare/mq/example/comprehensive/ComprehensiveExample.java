package com.flare.mq.example.comprehensive;

import com.flare.mq.client.producer.Producer;
import com.flare.mq.client.producer.ProducerImpl;
import com.flare.mq.client.producer.ProducerConfig;
import com.flare.mq.client.producer.Message;
import com.flare.mq.client.producer.SendResult;
import com.flare.mq.client.producer.SendCallback;
import com.flare.mq.client.consumer.Consumer;
import com.flare.mq.client.consumer.ConsumerImpl;
import com.flare.mq.client.consumer.ConsumerConfig;
import com.flare.mq.client.consumer.MessageListener;
import com.flare.mq.client.consumer.ConsumeStatus;
import com.flare.mq.client.consumer.ConsumeMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Comprehensive Example
 *
 * Demonstrates the complete usage flow of FlareMQ, including:
 * - System initialization
 * - Various message sending methods
 * - Various message consumption methods
 * - Error handling
 * - Resource cleanup
 *
 * @author FlareMQ Team
 */
public class ComprehensiveExample {

    private static final Logger logger = LoggerFactory.getLogger(ComprehensiveExample.class);

    public static void main(String[] args) {
        logger.info("=== FlareMQ Comprehensive Example Started ===");

        ComprehensiveExample example = new ComprehensiveExample();

        try {
            // Run comprehensive example
            example.runComprehensiveExample();

            logger.info("=== FlareMQ Comprehensive Example Completed ===");

        } catch (Exception e) {
            logger.error("Comprehensive example execution failed", e);
        }
    }

    /**
     * Run comprehensive example
     */
    public void runComprehensiveExample() throws Exception {

        // Phase 1: Basic message sending and receiving
        logger.info("=== Phase 1: Basic Message Sending and Receiving ===");
        basicMessageExample();

        // Phase 2: Advanced features demonstration
        logger.info("=== Phase 2: Advanced Features Demonstration ===");
        advancedFeaturesExample();

        // Phase 3: Performance testing
        logger.info("=== Phase 3: Performance Testing ===");
        performanceExample();

        // Phase 4: Error handling demonstration
        logger.info("=== Phase 4: Error Handling Demonstration ===");
        errorHandlingExample();
    }

    /**
     * Basic message sending and receiving example
     */
    private void basicMessageExample() throws Exception {
        logger.info("--- Basic message sending and receiving example ---");

        // Create Producer
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setProducerGroup("comprehensive_producer_group");
        producerConfig.setNameServerAddr("localhost:9876");
        producerConfig.setSendMsgTimeout(5000);

        Producer producer = new ProducerImpl(producerConfig);

        // Create Consumer
        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setConsumerGroup("comprehensive_consumer_group");
        consumerConfig.setNameServerAddr("localhost:9876");
        consumerConfig.setConsumeMode(ConsumeMode.CLUSTERING);

        Consumer consumer = new ConsumerImpl(consumerConfig);

        try {
            // Start Producer and Consumer
            producer.start();
            consumer.start();
            logger.info("Producer and Consumer started successfully");

            // 设置消息监听器
            CountDownLatch consumeLatch = new CountDownLatch(10);
            AtomicInteger consumeCount = new AtomicInteger(0);

            consumer.subscribe("ComprehensiveTopic", "*", new MessageListener() {
                @Override
                public ConsumeStatus consumeMessage(Message message) {
                    int count = consumeCount.incrementAndGet();
                    logger.info("Consumed message[{}]: messageId={}, body={}",
                               count, message.getMessageId(), new String(message.getBody()));
                    consumeLatch.countDown();
                    return ConsumeStatus.CONSUME_SUCCESS;
                }
            });

            // 发送同步消息
            logger.info("Sending sync messages...");
            for (int i = 0; i < 5; i++) {
                Message message = new Message("ComprehensiveTopic", "SyncTag",
                                            ("Sync message-" + i).getBytes());
                message.setKey("sync-key-" + i);

                SendResult result = producer.send(message);
                logger.info("Sync send successful[{}]: messageId={}", i, result.getMessageId());
            }

            // 发送异步消息
            logger.info("Sending async messages...");
            CountDownLatch sendLatch = new CountDownLatch(5);
            
            for (int i = 0; i < 5; i++) {
                final int index = i;
                Message message = new Message("ComprehensiveTopic", "AsyncTag",
                                            ("Async message-" + i).getBytes());
                message.setKey("async-key-" + i);

                producer.sendAsync(message, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        logger.info("Async send successful[{}]: messageId={}", index, sendResult.getMessageId());
                        sendLatch.countDown();
                    }

                    @Override
                    public void onException(Throwable exception) {
                        logger.error("Async send failed[" + index + "]", exception);
                        sendLatch.countDown();
                    }
                });
            }
            
            // 等待发送和消费完成
            boolean sendSuccess = sendLatch.await(30, TimeUnit.SECONDS);
            boolean consumeSuccess = consumeLatch.await(30, TimeUnit.SECONDS);
            
            if (sendSuccess && consumeSuccess) {
                logger.info("Basic message sending and receiving example completed");
            } else {
                logger.warn("Basic message sending and receiving example timeout");
            }

        } finally {
            producer.shutdown();
            consumer.shutdown();
            logger.info("Basic example resource cleanup completed");
        }
    }
    
    /**
     * 高级特性演示
     */
    private void advancedFeaturesExample() throws Exception {
        logger.info("--- Advanced features demonstration ---");

        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setProducerGroup("advanced_producer_group");
        producerConfig.setNameServerAddr("localhost:9876");

        Producer producer = new ProducerImpl(producerConfig);

        try {
            producer.start();

            // 1. 延迟消息
            logger.info("Sending delay message...");
            Message delayMessage = new Message("AdvancedTopic", "DelayTag", "Delay message".getBytes());
            delayMessage.setDelayTime(3000); // 延迟3秒
            SendResult delayResult = producer.send(delayMessage);
            logger.info("Delay message sent successfully: messageId={}", delayResult.getMessageId());

            // 2. 带属性的消息
            logger.info("Sending message with properties...");
            Message propMessage = new Message("AdvancedTopic", "PropTag", "Property message".getBytes());
            propMessage.putProperty("userId", "12345");
            propMessage.putProperty("orderType", "VIP");
            propMessage.setPriority(8);
            SendResult propResult = producer.send(propMessage);
            logger.info("Property message sent successfully: messageId={}", propResult.getMessageId());
            
            // 3. 顺序消息
            logger.info("Sending ordered messages...");
            String orderKey = "order-98765";
            for (int i = 0; i < 3; i++) {
                Message orderMessage = new Message("AdvancedTopic", "OrderTag",
                                                 ("Ordered message-" + i).getBytes());
                orderMessage.setKey(orderKey);
                orderMessage.setOrderedMessage(true);

                SendResult orderResult = producer.send(orderMessage);
                logger.info("Ordered message sent successfully[{}]: messageId={}", i, orderResult.getMessageId());
            }

            // 4. 事务消息
            logger.info("Sending transaction message...");
            Message txMessage = new Message("AdvancedTopic", "TxTag", "Transaction message".getBytes());
            txMessage.setTransactionMessage(true);
            txMessage.setTransactionId("tx-" + System.currentTimeMillis());
            SendResult txResult = producer.send(txMessage);
            logger.info("Transaction message sent successfully: messageId={}", txResult.getMessageId());

        } finally {
            producer.shutdown();
            logger.info("Advanced features example resource cleanup completed");
        }
    }
    
    /**
     * 性能测试示例
     */
    private void performanceExample() throws Exception {
        logger.info("--- Performance test example ---");

        ProducerConfig config = new ProducerConfig();
        config.setProducerGroup("performance_producer_group");
        config.setNameServerAddr("localhost:9876");

        Producer producer = new ProducerImpl(config);

        try {
            producer.start();

            int messageCount = 1000;
            byte[] messageBody = "性能测试消息".getBytes();

            long startTime = System.currentTimeMillis();
            CountDownLatch latch = new CountDownLatch(messageCount);
            AtomicInteger successCount = new AtomicInteger(0);

            logger.info("Starting performance test, sending {} messages", messageCount);
            
            // 异步发送消息
            for (int i = 0; i < messageCount; i++) {
                Message message = new Message("PerformanceTopic", "PerfTag", messageBody);
                message.setKey("perf-key-" + i);
                
                producer.sendAsync(message, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        successCount.incrementAndGet();
                        latch.countDown();
                    }
                    
                    @Override
                    public void onException(Throwable exception) {
                        latch.countDown();
                    }
                });
            }
            
            // 等待发送完成
            boolean finished = latch.await(60, TimeUnit.SECONDS);
            long endTime = System.currentTimeMillis();
            long totalTime = endTime - startTime;
            
            if (finished) {
                int success = successCount.get();
                double tps = (double) success * 1000 / totalTime;
                logger.info("Performance test result: success={}, totalTime={}ms, TPS={:.2f}",
                           success, totalTime, tps);
            } else {
                logger.warn("Performance test timeout");
            }

        } finally {
            producer.shutdown();
            logger.info("Performance test example resource cleanup completed");
        }
    }
    
    /**
     * 错误处理演示
     */
    private void errorHandlingExample() throws Exception {
        logger.info("--- Error handling demonstration ---");

        // 测试连接错误
        logger.info("Testing connection error handling...");
        ProducerConfig errorConfig = new ProducerConfig();
        errorConfig.setProducerGroup("error_producer_group");
        errorConfig.setNameServerAddr("invalid:9876"); // 无效地址
        errorConfig.setSendMsgTimeout(3000);
        
        Producer errorProducer = new ProducerImpl(errorConfig);
        
        try {
            errorProducer.start();
            
            Message message = new Message("ErrorTopic", "ErrorTag", "Error test message".getBytes());

            try {
                SendResult result = errorProducer.send(message);
                logger.info("Unexpected success: messageId={}", result.getMessageId());
            } catch (Exception e) {
                logger.info("Expected send error: {}", e.getMessage());
            }

        } catch (Exception e) {
            logger.info("Expected startup error: {}", e.getMessage());
        } finally {
            try {
                errorProducer.shutdown();
            } catch (Exception e) {
                logger.info("Error during shutdown: {}", e.getMessage());
            }
        }

        // 测试消费错误处理
        logger.info("Testing consume error handling...");
        ProducerConfig normalConfig = new ProducerConfig();
        normalConfig.setProducerGroup("normal_producer_group");
        normalConfig.setNameServerAddr("localhost:9876");
        
        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setConsumerGroup("error_consumer_group");
        consumerConfig.setNameServerAddr("localhost:9876");
        consumerConfig.setMaxRetryTimes(2);
        
        Producer normalProducer = new ProducerImpl(normalConfig);
        Consumer errorConsumer = new ConsumerImpl(consumerConfig);
        
        try {
            normalProducer.start();
            errorConsumer.start();
            
            AtomicInteger retryCount = new AtomicInteger(0);
            CountDownLatch errorLatch = new CountDownLatch(1);
            
            errorConsumer.subscribe("ErrorHandlingTopic", "*", new MessageListener() {
                @Override
                public ConsumeStatus consumeMessage(Message message) {
                    int count = retryCount.incrementAndGet();
                    logger.info("Consume attempt[{}]: messageId={}", count, message.getMessageId());

                    if (count < 3) {
                        logger.info("Simulating consume failure, will retry");
                        return ConsumeStatus.RECONSUME_LATER;
                    } else {
                        logger.info("Consume successful");
                        errorLatch.countDown();
                        return ConsumeStatus.CONSUME_SUCCESS;
                    }
                }
            });

            // 发送会触发重试的消息
            Message retryMessage = new Message("ErrorHandlingTopic", "RetryTag", "Retry test message".getBytes());
            SendResult result = normalProducer.send(retryMessage);
            logger.info("Sent retry test message: messageId={}", result.getMessageId());

            // 等待重试完成
            boolean success = errorLatch.await(30, TimeUnit.SECONDS);
            if (success) {
                logger.info("Error handling demonstration completed, retry mechanism works normally");
            } else {
                logger.warn("Error handling demonstration timeout");
            }

        } finally {
            normalProducer.shutdown();
            errorConsumer.shutdown();
            logger.info("Error handling example resource cleanup completed");
        }

        logger.info("--- Error handling demonstration completed ---");
    }
}
