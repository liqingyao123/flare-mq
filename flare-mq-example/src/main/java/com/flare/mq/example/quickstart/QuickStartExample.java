package com.flare.mq.example.quickstart;

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
import com.flare.mq.client.consumer.ConsumeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * FlareMQ Quick Start Example
 *
 * Demonstrates basic message sending and receiving functionality
 *
 * @author FlareMQ Team
 */
public class QuickStartExample {

    private static final Logger logger = LoggerFactory.getLogger(QuickStartExample.class);

    public static void main(String[] args) {
        logger.info("=== FlareMQ Quick Start Example ===");

        try {
            // Run basic example
            runBasicExample();

            // Run async example
            //runAsyncExample();

            logger.info("=== Example execution completed ===");

        } catch (Exception e) {
            logger.error("Example execution failed", e);
        }
    }

    /**
     * Basic synchronous send and receive example
     */
    private static void runBasicExample() throws Exception {
        logger.info("--- Starting basic example ---");
        
        // 1. Create Producer
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setProducerGroup("example_producer_group");
        producerConfig.setNameServerAddr("localhost:9876");

        Producer producer = new ProducerImpl(producerConfig);

        // 2. Create Consumer
        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setConsumerGroup("example_consumer_group");
        consumerConfig.setNameServerAddr("localhost:9876");
        consumerConfig.setConsumeType(ConsumeType.CONSUME_ACTIVELY); // Set to active pull mode
        consumerConfig.setPullInterval(1000); // Set pull interval to 1 second
        consumerConfig.setPullBatchSize(10); // Set batch pull size

        Consumer consumer = new ConsumerImpl(consumerConfig);

        // 3. Set message listener
        CountDownLatch latch = new CountDownLatch(3);
        consumer.subscribe("QuickStartTopic", "*", new MessageListener() {
            @Override
            public ConsumeStatus consumeMessage(Message message) {
                logger.info("Received message: Topic={}, Tags={}, Body={}",
                           message.getTopic(),
                           message.getTags(),
                           new String(message.getBody()));
                latch.countDown();
                return ConsumeStatus.CONSUME_SUCCESS;
            }
        });
        
        try {
            // 4. Start Producer and Consumer
            producer.start();
            consumer.start();

            logger.info("Producer and Consumer started successfully");

            // 5. Send messages
            for (int i = 0; i < 3; i++) {
                Message message = new Message("QuickStartTopic", "TagA",
                                            ("Hello FlareMQ " + i).getBytes());
                message.setKey("key-" + i);

                SendResult result = producer.send(message);
                logger.info("Message sent successfully: messageId={}, queueId={}, offset={}",
                           result.getMessageId(), result.getQueueId(), result.getQueueOffset());
            }

            // 6. Wait for message consumption completion
            boolean success = latch.await(30, TimeUnit.SECONDS);
            if (success) {
                logger.info("All messages consumed successfully");
            } else {
                logger.warn("Message consumption timeout");
            }

        } finally {
            // 7. Close resources
            producer.shutdown();
            consumer.shutdown();
            logger.info("Resources closed successfully");
        }

        logger.info("--- Basic example completed ---");
    }
    
    /**
     * Asynchronous send example
     */
    private static void runAsyncExample() throws Exception {
        logger.info("--- Starting async example ---");

        // 1. Create Producer
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setProducerGroup("async_producer_group");
        producerConfig.setNameServerAddr("localhost:9876");

        Producer producer = new ProducerImpl(producerConfig);

        try {
            // 2. Start Producer
            producer.start();
            logger.info("Async Producer started successfully");

            // 3. Send messages asynchronously
            CountDownLatch sendLatch = new CountDownLatch(5);

            for (int i = 0; i < 5; i++) {
                final int index = i;
                Message message = new Message("AsyncTopic", "TagB",
                                            ("Async Message " + i).getBytes());
                message.setKey("async-key-" + i);

                producer.sendAsync(message, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        logger.info("Async send successful: index={}, messageId={}, queueId={}",
                                   index, sendResult.getMessageId(), sendResult.getQueueId());
                        sendLatch.countDown();
                    }

                    @Override
                    public void onException(Throwable exception) {
                        logger.error("Async send failed: index=" + index, exception);
                        sendLatch.countDown();
                    }
                });
            }

            // 4. Wait for async send completion
            boolean success = sendLatch.await(30, TimeUnit.SECONDS);
            if (success) {
                logger.info("All async messages sent successfully");
            } else {
                logger.warn("Async message send timeout");
            }

        } finally {
            // 5. Close resources
            producer.shutdown();
            logger.info("Async Producer closed successfully");
        }

        logger.info("--- Async example completed ---");
    }
}
