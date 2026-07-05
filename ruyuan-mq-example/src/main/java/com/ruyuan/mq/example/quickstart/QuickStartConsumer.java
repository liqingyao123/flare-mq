package com.ruyuan.mq.example.quickstart;

import com.ruyuan.mq.client.consumer.*;
import com.ruyuan.mq.client.producer.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuickStartConsumer {

    private static final Logger logger = LoggerFactory.getLogger(QuickStartConsumer.class);

    public static void main(String[] args) throws Exception {
        logger.info("=== RuYuan MQ Quick Consumer Start Example ===");

        Consumer consumer = runBasicConsumerExample();

        // 注册 shutdown hook 优雅关闭
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down consumer...");
            consumer.shutdown();
            logger.info("Consumer shutdown complete");
        }));

        // 阻塞主线程，保持进程持续监听
        logger.info("Consumer started, listening for messages...");
        Thread.currentThread().join();
    }

    private static Consumer runBasicConsumerExample() throws Exception {
        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setConsumerGroup("example_consumer_group");
        consumerConfig.setNameServerAddr("localhost:9876");
        consumerConfig.setConsumeType(ConsumeType.CONSUME_ACTIVELY); // Set to active pull mode
        consumerConfig.setPullInterval(1000); // Set pull interval to 1 second
        consumerConfig.setPullBatchSize(10); // Set batch pull size
        Consumer consumer = new ConsumerImpl(consumerConfig);
        consumer.subscribe("QuickStartTopic", "*", new MessageListener() {
            @Override
            public ConsumeStatus consumeMessage(Message message) {
                logger.info("Received message: QueueId={}, Topic={}, Tags={}, Body={}",
                        message.getQueueId(),
                        message.getTopic(),
                        message.getTags(),
                        new String(message.getBody()));
                return ConsumeStatus.CONSUME_SUCCESS;
            }
        });
        try{
            consumer.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return consumer;
    }
}
