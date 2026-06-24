package com.ruyuan.mq.example.producer;

import com.ruyuan.mq.client.producer.Producer;
import com.ruyuan.mq.client.producer.ProducerImpl;
import com.ruyuan.mq.client.producer.ProducerConfig;
import com.ruyuan.mq.client.producer.Message;
import com.ruyuan.mq.client.producer.SendResult;
import com.ruyuan.mq.client.producer.SendCallback;
import com.ruyuan.mq.client.producer.SendStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Simple Producer Example
 *
 * Demonstrates various message sending methods
 *
 * @author RuYuan MQ Team
 */
public class SimpleProducerExample {

    private static final Logger logger = LoggerFactory.getLogger(SimpleProducerExample.class);

    public static void main(String[] args) {
        logger.info("=== Simple Producer Example ===");

        try {
            // Synchronous send example
            syncSendExample();

            // Asynchronous send example
            asyncSendExample();

            // Oneway send example
            onewaySendExample();

            // Batch send example
            batchSendExample();

            logger.info("=== Producer example completed ===");

        } catch (Exception e) {
            logger.error("Producer example execution failed", e);
        }
    }

    /**
     * Synchronous send example
     */
    private static void syncSendExample() throws Exception {
        logger.info("--- Synchronous send example ---");

        // Create Producer configuration
        ProducerConfig config = new ProducerConfig();
        config.setProducerGroup("sync_producer_group");
        config.setNameServerAddr("localhost:9876");
        config.setSendMsgTimeout(5000); // 5 seconds timeout
        config.setRetryTimesWhenSendFailed(3); // Retry 3 times on failure
        
        Producer producer = new ProducerImpl(config);

        try {
            // Start Producer
            producer.start();
            logger.info("Sync Producer started successfully");

            // Send normal message
            Message message1 = new Message("SyncTopic", "TagA", "Sync message content".getBytes());
            message1.setKey("sync-key-1");
            message1.setPriority(5);

            SendResult result1 = producer.send(message1);
            logger.info("Sync send result: status={}, messageId={}, queueId={}, offset={}, costTime={}ms",
                       result1.getSendStatus(), result1.getMessageId(),
                       result1.getQueueId(), result1.getQueueOffset(), result1.getCostTime());

            // Send message with properties
            Message message2 = new Message("SyncTopic", "TagB", "Message with properties".getBytes());
            message2.setKey("sync-key-2");
            message2.putProperty("userId", "12345");
            message2.putProperty("orderType", "NORMAL");
            message2.setPriority(8); // High priority

            SendResult result2 = producer.send(message2, 3000); // 3 seconds timeout
            logger.info("Message with properties send result: status={}, messageId={}",
                       result2.getSendStatus(), result2.getMessageId());

            // Send delay message
            Message delayMessage = new Message("SyncTopic", "TagC", "Delay message".getBytes());
            delayMessage.setKey("delay-key-1");
            delayMessage.setDelayTime(5000); // Delay 5 seconds

            SendResult delayResult = producer.send(delayMessage);
            logger.info("Delay message send result: status={}, messageId={}",
                       delayResult.getSendStatus(), delayResult.getMessageId());

        } finally {
            producer.shutdown();
            logger.info("Sync Producer closed successfully");
        }

        logger.info("--- Synchronous send example completed ---");
    }
    
    /**
     * Asynchronous send example
     */
    private static void asyncSendExample() throws Exception {
        logger.info("--- Asynchronous send example ---");

        ProducerConfig config = new ProducerConfig();
        config.setProducerGroup("async_producer_group");
        config.setNameServerAddr("localhost:9876");
        config.setRetryTimesWhenSendAsyncFailed(2);

        Producer producer = new ProducerImpl(config);

        try {
            producer.start();
            logger.info("Async Producer started successfully");

            CountDownLatch latch = new CountDownLatch(10);

            // Send multiple messages asynchronously
            for (int i = 0; i < 10; i++) {
                final int index = i;
                Message message = new Message("AsyncTopic", "TagAsync",
                                            ("Async message " + i).getBytes());
                message.setKey("async-key-" + i);

                producer.sendAsync(message, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        logger.info("Async send successful: index={}, messageId={}, queueId={}, costTime={}ms",
                                   index, sendResult.getMessageId(),
                                   sendResult.getQueueId(), sendResult.getCostTime());
                        latch.countDown();
                    }

                    @Override
                    public void onException(Throwable exception) {
                        logger.error("Async send failed: index=" + index, exception);
                        latch.countDown();
                    }
                }, 5000); // 5 seconds timeout
            }

            // Wait for all async sends to complete
            boolean success = latch.await(30, TimeUnit.SECONDS);
            if (success) {
                logger.info("All async messages sent successfully");
            } else {
                logger.warn("Async send timeout, some messages may not be completed");
            }

        } finally {
            producer.shutdown();
            logger.info("Async Producer closed successfully");
        }

        logger.info("--- Asynchronous send example completed ---");
    }
    
    /**
     * Oneway send example
     */
    private static void onewaySendExample() throws Exception {
        logger.info("--- Oneway send example ---");

        ProducerConfig config = new ProducerConfig();
        config.setProducerGroup("oneway_producer_group");
        config.setNameServerAddr("localhost:9876");

        Producer producer = new ProducerImpl(config);

        try {
            producer.start();
            logger.info("Oneway Producer started successfully");

            // Oneway send (don't care about result)
            for (int i = 0; i < 5; i++) {
                Message message = new Message("OnewayTopic", "TagOneway",
                                            ("Oneway message " + i).getBytes());
                message.setKey("oneway-key-" + i);

                producer.sendOneway(message);
                logger.info("Oneway message sent: index={}", i);
            }

            // Wait a bit after oneway send to ensure messages are sent
            Thread.sleep(1000);

        } finally {
            producer.shutdown();
            logger.info("Oneway Producer closed successfully");
        }

        logger.info("--- Oneway send example completed ---");
    }

    /**
     * Batch send example
     */
    private static void batchSendExample() throws Exception {
        logger.info("--- Batch send example ---");

        ProducerConfig config = new ProducerConfig();
        config.setProducerGroup("batch_producer_group");
        config.setNameServerAddr("localhost:9876");
        config.setMaxBatchSize(10); // Batch size
        config.setBatchMaxWaitTime(100); // Batch wait time 100ms

        Producer producer = new ProducerImpl(config);

        try {
            producer.start();
            logger.info("Batch Producer started successfully");

            // Send multiple messages quickly to trigger batch sending
            for (int i = 0; i < 20; i++) {
                Message message = new Message("BatchTopic", "TagBatch",
                                            ("Batch message " + i).getBytes());
                message.setKey("batch-key-" + i);

                SendResult result = producer.send(message);
                logger.info("Batch message sent: index={}, messageId={}", i, result.getMessageId());

                // Short interval
                Thread.sleep(10);
            }

        } finally {
            producer.shutdown();
            logger.info("Batch Producer closed successfully");
        }

        logger.info("--- Batch send example completed ---");
    }
}
