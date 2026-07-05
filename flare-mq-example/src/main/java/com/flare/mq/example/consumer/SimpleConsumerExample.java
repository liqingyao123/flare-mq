package com.flare.mq.example.consumer;

import com.flare.mq.client.consumer.Consumer;
import com.flare.mq.client.consumer.ConsumerImpl;
import com.flare.mq.client.consumer.ConsumerConfig;
import com.flare.mq.client.consumer.MessageListener;
import com.flare.mq.client.consumer.ConsumeStatus;
import com.flare.mq.client.consumer.ConsumeMode;
import com.flare.mq.client.consumer.ConsumeType;
import com.flare.mq.client.consumer.ConsumeFromWhere;
import com.flare.mq.client.consumer.PullResult;
import com.flare.mq.client.consumer.PullStatus;
import com.flare.mq.client.producer.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Simple Consumer Example
 *
 * Demonstrates various message consumption methods
 *
 * @author FlareMQ Team
 */
public class SimpleConsumerExample {

    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumerExample.class);

    public static void main(String[] args) {
        logger.info("=== Simple Consumer Example ===");

        try {
            // Push mode consumption example
            pushConsumeExample();

            // Pull mode consumption example
            pullConsumeExample();

            // Tag filter consumption example
            tagFilterConsumeExample();

            // Batch consumption example
            batchConsumeExample();

            logger.info("=== Consumer example completed ===");

        } catch (Exception e) {
            logger.error("Consumer example execution failed", e);
        }
    }

    /**
     * Push mode consumption example
     */
    private static void pushConsumeExample() throws Exception {
        logger.info("--- Push mode consumption example ---");

        // Create Consumer configuration
        ConsumerConfig config = new ConsumerConfig();
        config.setConsumerGroup("push_consumer_group");
        config.setNameServerAddr("localhost:9876");
        config.setConsumeMode(ConsumeMode.CLUSTERING); // Cluster consumption
        config.setConsumeType(ConsumeType.CONSUME_PASSIVELY); // Push mode
        config.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        config.setConsumeTimeout(15000); // Consumption timeout 15 seconds
        config.setMaxRetryTimes(3); // Maximum retry 3 times
        
        Consumer consumer = new ConsumerImpl(config);

        try {
            // Subscribe to Topic and set message listener
            CountDownLatch latch = new CountDownLatch(10);

            consumer.subscribe("PushTopic", "*", new MessageListener() {
                @Override
                public ConsumeStatus consumeMessage(Message message) {
                    try {
                        logger.info("Push consume message: Topic={}, Tags={}, Key={}, Body={}, Properties={}",
                                   message.getTopic(),
                                   message.getTags(),
                                   message.getKey(),
                                   new String(message.getBody()),
                                   message.getProperties());

                        // Simulate message processing
                        Thread.sleep(100);

                        latch.countDown();
                        return ConsumeStatus.CONSUME_SUCCESS;

                    } catch (Exception e) {
                        logger.error("Message processing failed", e);
                        return ConsumeStatus.RECONSUME_LATER;
                    }
                }
            });

            // Start Consumer
            consumer.start();
            logger.info("Push Consumer started successfully");

            // Wait for message consumption (in real scenarios Consumer runs continuously)
            boolean success = latch.await(60, TimeUnit.SECONDS);
            if (success) {
                logger.info("Push mode message consumption completed");
            } else {
                logger.info("Push mode wait timeout, Consumer continues running");
            }

        } finally {
            consumer.shutdown();
            logger.info("Push Consumer closed successfully");
        }

        logger.info("--- Push mode consumption example completed ---");
    }
    
    /**
     * Pull mode consumption example
     */
    private static void pullConsumeExample() throws Exception {
        logger.info("--- Pull mode consumption example ---");

        ConsumerConfig config = new ConsumerConfig();
        config.setConsumerGroup("pull_consumer_group");
        config.setNameServerAddr("localhost:9876");
        config.setConsumeMode(ConsumeMode.CLUSTERING);
        config.setConsumeType(ConsumeType.CONSUME_ACTIVELY); // Pull mode
        config.setPullBatchSize(10); // Pull 10 messages each time
        config.setPullMsgTimeout(3000); // Pull timeout 3 seconds

        Consumer consumer = new ConsumerImpl(config);

        try {
            consumer.start();
            logger.info("Pull Consumer started successfully");

            // Actively pull messages
            String topic = "PullTopic";
            int queueId = 0;
            long offset = 0;
            int maxNums = 5;

            for (int i = 0; i < 5; i++) {
                PullResult result = consumer.pullMessage(topic, queueId, offset, maxNums, 5000);

                logger.info("Pull result: status={}, nextOffset={}, messageCount={}",
                           result.getPullStatus(), result.getNextBeginOffset(), result.getMessageCount());

                if (result.getPullStatus() == PullStatus.FOUND && result.getMessages() != null) {
                    for (Message message : result.getMessages()) {
                        logger.info("Pull consume message: Topic={}, Tags={}, Body={}",
                                   message.getTopic(), message.getTags(), new String(message.getBody()));

                        // Acknowledge message consumption
                        consumer.ackMessage(message.getMessageId());
                    }

                    // Update offset
                    offset = result.getNextBeginOffset();
                } else if (result.getPullStatus() == PullStatus.NO_NEW_MSG) {
                    logger.info("No new messages, wait and retry");
                    Thread.sleep(1000);
                } else {
                    logger.warn("Pull message failed: {}", result.getErrorMessage());
                    break;
                }
            }

        } finally {
            consumer.shutdown();
            logger.info("Pull Consumer closed successfully");
        }

        logger.info("--- Pull mode consumption example completed ---");
    }
    
    /**
     * Tag filter consumption example
     */
    private static void tagFilterConsumeExample() throws Exception {
        logger.info("--- Tag filter consumption example ---");

        ConsumerConfig config = new ConsumerConfig();
        config.setConsumerGroup("tag_filter_consumer_group");
        config.setNameServerAddr("localhost:9876");

        Consumer consumer = new ConsumerImpl(config);

        try {
            CountDownLatch latch = new CountDownLatch(5);

            // Only consume messages with TagA and TagB
            consumer.subscribe("FilterTopic", "TagA,TagB", new MessageListener() {
                @Override
                public ConsumeStatus consumeMessage(Message message) {
                    logger.info("Tag filter consumption: Topic={}, Tags={}, Body={}",
                               message.getTopic(), message.getTags(), new String(message.getBody()));
                    latch.countDown();
                    return ConsumeStatus.CONSUME_SUCCESS;
                }
            });

            consumer.start();
            logger.info("Tag filter Consumer started successfully, only consuming TagA and TagB");

            // Wait for message consumption
            boolean success = latch.await(30, TimeUnit.SECONDS);
            if (success) {
                logger.info("Tag filter message consumption completed");
            } else {
                logger.info("Tag filter consumption wait timeout");
            }

        } finally {
            consumer.shutdown();
            logger.info("Tag filter Consumer closed successfully");
        }

        logger.info("--- Tag filter consumption example completed ---");
    }

    /**
     * Batch consumption example
     */
    private static void batchConsumeExample() throws Exception {
        logger.info("--- Batch consumption example ---");

        ConsumerConfig config = new ConsumerConfig();
        config.setConsumerGroup("batch_consumer_group");
        config.setNameServerAddr("localhost:9876");
        config.setPullBatchSize(5); // Batch pull 5 messages
        config.setConsumeThreadNums(2); // 2 consumption threads

        Consumer consumer = new ConsumerImpl(config);

        try {
            CountDownLatch latch = new CountDownLatch(3);

            consumer.subscribe("BatchTopic", "*", new MessageListener() {
                @Override
                public ConsumeStatus consumeMessage(Message message) {
                    logger.info("Batch consume single message: messageId={}, body={}",
                               message.getMessageId(), new String(message.getBody()));
                    return ConsumeStatus.CONSUME_SUCCESS;
                }

                @Override
                public ConsumeStatus consumeMessages(List<Message> messages) {
                    logger.info("Batch consume multiple messages: count={}", messages.size());

                    for (Message message : messages) {
                        logger.info("  - messageId={}, body={}",
                                   message.getMessageId(), new String(message.getBody()));
                    }

                    latch.countDown();
                    return ConsumeStatus.CONSUME_SUCCESS;
                }
            });

            consumer.start();
            logger.info("Batch Consumer started successfully");

            // Wait for batch consumption
            boolean success = latch.await(30, TimeUnit.SECONDS);
            if (success) {
                logger.info("Batch consumption completed");
            } else {
                logger.info("Batch consumption wait timeout");
            }

        } finally {
            consumer.shutdown();
            logger.info("Batch Consumer closed successfully");
        }

        logger.info("--- Batch consumption example completed ---");
    }
}
