package com.flare.mq.example.advanced;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Advanced Features Example
 *
 * Demonstrates FlareMQ advanced features:
 * - Ordered messages
 * - Transaction messages
 * - Delay messages
 * - Message retry and dead letter queue
 * - Message filtering
 *
 * @author FlareMQ Team
 */
public class AdvancedFeaturesExample {

    private static final Logger logger = LoggerFactory.getLogger(AdvancedFeaturesExample.class);

    public static void main(String[] args) {
        logger.info("=== FlareMQ Advanced Features Example ===");

        try {
            // Ordered message example
            orderedMessageExample();

            // Transaction message example
            transactionMessageExample();

            // Delay message example
            delayMessageExample();

            // Message retry and dead letter queue example
            retryAndDeadLetterExample();

            // Message filter example
            messageFilterExample();

            logger.info("=== Advanced features example completed ===");

        } catch (Exception e) {
            logger.error("Advanced features example execution failed", e);
        }
    }

    /**
     * Ordered message example
     */
    private static void orderedMessageExample() throws Exception {
        logger.info("--- Ordered message example ---");

        // Producer configuration
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setProducerGroup("ordered_producer_group");
        producerConfig.setNameServerAddr("localhost:9876");

        // Consumer configuration
        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setConsumerGroup("ordered_consumer_group");
        consumerConfig.setNameServerAddr("localhost:9876");
        consumerConfig.setOrderedConsume(true); // Enable ordered consumption

        Producer producer = new ProducerImpl(producerConfig);
        Consumer consumer = new ConsumerImpl(consumerConfig);

        try {
            // Start
            producer.start();
            consumer.start();

            // Set ordered consumption listener
            CountDownLatch latch = new CountDownLatch(10);
            AtomicInteger orderCounter = new AtomicInteger(0);

            consumer.subscribe("OrderedTopic", "*", new MessageListener() {
                @Override
                public ConsumeStatus consumeMessage(Message message) {
                    int order = orderCounter.incrementAndGet();
                    logger.info("Ordered consume message: order={}, messageId={}, body={}",
                               order, message.getMessageId(), new String(message.getBody()));
                    latch.countDown();
                    return ConsumeStatus.CONSUME_SUCCESS;
                }
            });
            
            // 发送顺序消息（使用相同的Key确保进入同一队列）
            String orderKey = "order-12345";
            for (int i = 0; i < 10; i++) {
                Message message = new Message("OrderedTopic", "OrderTag", 
                                            ("Ordered message-" + i).getBytes());
                message.setKey(orderKey); // 相同Key保证顺序
                message.setOrderedMessage(true); // 标记为顺序消息
                
                SendResult result = producer.send(message);
                logger.info("Send ordered message: index={}, messageId={}, queueId={}", 
                           i, result.getMessageId(), result.getQueueId());
            }
            
            // 等待消费完成
            boolean success = latch.await(30, TimeUnit.SECONDS);
            if (success) {
                logger.info("Ordered message consumption completed");
            } else {
                logger.warn("Ordered message consumption timed out");
            }
            
        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
        
        logger.info("--- Ordered message example completed ---");
    }
    
    /**
     * 事务消息示例
     */
    private static void transactionMessageExample() throws Exception {
        logger.info("--- Transaction message example ---");
        
        ProducerConfig config = new ProducerConfig();
        config.setProducerGroup("transaction_producer_group");
        config.setNameServerAddr("localhost:9876");
        
        Producer producer = new ProducerImpl(config);
        
        try {
            producer.start();
            
            // 发送事务消息
            for (int i = 0; i < 3; i++) {
                final int index = i;
                String transactionId = "tx-" + System.currentTimeMillis() + "-" + i;
                
                Message message = new Message("TransactionTopic", "TxTag", 
                                            ("Transaction message-" + i).getBytes());
                message.setKey("tx-key-" + i);
                message.setTransactionMessage(true);
                message.setTransactionId(transactionId);
                
                // 模拟事务处理
                boolean transactionSuccess = (i % 2 == 0); // 偶数成功，奇数失败
                
                if (transactionSuccess) {
                    // 事务成功，发送消息
                    SendResult result = producer.send(message);
                    logger.info("Transaction message sent successfully: index={}, transactionId={}, messageId={}", 
                               index, transactionId, result.getMessageId());
                } else {
                    // 事务失败，不发送消息
                    logger.info("Transaction failed, message not sent: index={}, transactionId={}", index, transactionId);
                }
            }
            
        } finally {
            producer.shutdown();
        }
        
        logger.info("--- Transaction message example completed ---");
    }
    
    /**
     * 延迟消息示例
     */
    private static void delayMessageExample() throws Exception {
        logger.info("--- Delay message example ---");
        
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setProducerGroup("delay_producer_group");
        producerConfig.setNameServerAddr("localhost:9876");
        
        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setConsumerGroup("delay_consumer_group");
        consumerConfig.setNameServerAddr("localhost:9876");
        
        Producer producer = new ProducerImpl(producerConfig);
        Consumer consumer = new ConsumerImpl(consumerConfig);
        
        try {
            producer.start();
            consumer.start();
            
            // 设置延迟消息监听器
            CountDownLatch latch = new CountDownLatch(3);
            
            consumer.subscribe("DelayTopic", "*", new MessageListener() {
                @Override
                public ConsumeStatus consumeMessage(Message message) {
                    long currentTime = System.currentTimeMillis();
                    logger.info("Received delay message: body={}, current time={}", 
                               new String(message.getBody()), currentTime);
                    latch.countDown();
                    return ConsumeStatus.CONSUME_SUCCESS;
                }
            });
            
            // 发送不同延迟时间的消息
            long currentTime = System.currentTimeMillis();
            
            // 延迟5秒
            Message delay5s = new Message("DelayTopic", "DelayTag", "Delay message for 5 seconds".getBytes());
            delay5s.setDelayTime(5000);
            SendResult result1 = producer.send(delay5s);
            logger.info("Send delay message for 5 seconds: messageId={}, send time={}", result1.getMessageId(), currentTime);
            
            // 延迟10秒
            Message delay10s = new Message("DelayTopic", "DelayTag", "Delay message for 10 seconds".getBytes());
            delay10s.setDelayTime(10000);
            SendResult result2 = producer.send(delay10s);
            logger.info("Send delay message for 10 seconds: messageId={}, send time={}", result2.getMessageId(), currentTime);
            
            // 延迟15秒
            Message delay15s = new Message("DelayTopic", "DelayTag", "Delay message for 15 seconds".getBytes());
            delay15s.setDelayTime(15000);
            SendResult result3 = producer.send(delay15s);
            logger.info("Send delay message for 15 seconds: messageId={}, send time={}", result3.getMessageId(), currentTime);
            
            // Wait for delay message consumption
            boolean success = latch.await(30, TimeUnit.SECONDS);
            if (success) {
                logger.info("Delay message consumption completed");
            } else {
                logger.warn("Delay message consumption timed out");
            }
            
        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
        
        logger.info("--- Delay message example completed ---");
    }
    
    /**
     * Message retry and dead letter queue example
     */
    private static void retryAndDeadLetterExample() throws Exception {
        logger.info("--- Message retry and dead letter queue example ---");
        
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setProducerGroup("retry_producer_group");
        producerConfig.setNameServerAddr("localhost:9876");
        
        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setConsumerGroup("retry_consumer_group");
        consumerConfig.setNameServerAddr("localhost:9876");
        consumerConfig.setMaxRetryTimes(3); // Maximum retries 3 times
        
        Producer producer = new ProducerImpl(producerConfig);
        Consumer consumer = new ConsumerImpl(consumerConfig);
        
        try {
            producer.start();
            consumer.start();
            
            // 设置会失败的消息监听器
            AtomicInteger consumeCount = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(1);
            
            consumer.subscribe("RetryTopic", "*", new MessageListener() {
                @Override
                public ConsumeStatus consumeMessage(Message message) {
                    int count = consumeCount.incrementAndGet();
                    logger.info("Consume attempt: count={}, messageId={}, body={}", 
                               count, message.getMessageId(), new String(message.getBody()));
                    
                    // Fail the first 3 times, succeed on the 4th
                    if (count < 4) {
                        logger.warn("Simulate consume failure, will retry: count={}", count);
                        return ConsumeStatus.RECONSUME_LATER;
                    } else {
                        logger.info("Consume successful: count={}", count);
                        latch.countDown();
                        return ConsumeStatus.CONSUME_SUCCESS;
                    }
                }
            });
            
            // Send a message that will trigger retries
            Message retryMessage = new Message("RetryTopic", "RetryTag", "Retry test message".getBytes());
            retryMessage.setKey("retry-key-1");
            
            SendResult result = producer.send(retryMessage);
            logger.info("Sent retry test message: messageId={}", result.getMessageId());
            
            // Wait for retries to complete
            boolean success = latch.await(60, TimeUnit.SECONDS);
            if (success) {
                logger.info("Retry mechanism test completed");
            } else {
                logger.warn("Retry test timed out, message may enter dead letter queue");
            }
            
        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
        
        logger.info("--- Message retry and dead letter queue example completed ---");
    }
    
    /**
     * 消息过滤示例
     */
    private static void messageFilterExample() throws Exception {
        logger.info("--- Message filtering example ---");
        
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setProducerGroup("filter_producer_group");
        producerConfig.setNameServerAddr("localhost:9876");
        
        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setConsumerGroup("filter_consumer_group");
        consumerConfig.setNameServerAddr("localhost:9876");
        
        Producer producer = new ProducerImpl(producerConfig);
        Consumer consumer = new ConsumerImpl(consumerConfig);
        
        try {
            producer.start();
            consumer.start();
            
            // 只消费VIP用户的消息
            CountDownLatch latch = new CountDownLatch(2);
            
            consumer.subscribe("FilterTopic", "VIP", new MessageListener() {
                @Override
                public ConsumeStatus consumeMessage(Message message) {
                    logger.info("Consume VIP message: messageId={}, userType={}, body={}", 
                               message.getMessageId(), 
                               message.getProperty("userType"), 
                               new String(message.getBody()));
                    latch.countDown();
                    return ConsumeStatus.CONSUME_SUCCESS;
                }
            });
            
            // Send different types of messages
            // VIP user message
            Message vipMessage1 = new Message("FilterTopic", "VIP", "VIP user message 1".getBytes());
            vipMessage1.putProperty("userType", "VIP");
            vipMessage1.putProperty("userId", "vip001");
            producer.send(vipMessage1);
            
            // Normal user message (will not be consumed)
            Message normalMessage = new Message("FilterTopic", "NORMAL", "Normal user message".getBytes());
            normalMessage.putProperty("userType", "NORMAL");
            normalMessage.putProperty("userId", "normal001");
            producer.send(normalMessage);
            
            // Another VIP user message
            Message vipMessage2 = new Message("FilterTopic", "VIP", "VIP user message 2".getBytes());
            vipMessage2.putProperty("userType", "VIP");
            vipMessage2.putProperty("userId", "vip002");
            producer.send(vipMessage2);
            
            logger.info("Sent 3 messages: 2 VIP, 1 NORMAL, only VIP messages will be consumed");
            
            // Wait for filtering consumption to complete
            boolean success = latch.await(30, TimeUnit.SECONDS);
            if (success) {
                logger.info("Message filtering example completed, only VIP messages consumed");
            } else {
                logger.warn("Message filtering example timed out");
            }
            
        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
        
        logger.info("--- Message filtering example completed ---");
    }
}
