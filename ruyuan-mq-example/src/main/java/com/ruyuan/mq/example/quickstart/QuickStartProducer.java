package com.ruyuan.mq.example.quickstart;

import com.ruyuan.mq.client.producer.Producer;
import com.ruyuan.mq.client.producer.ProducerImpl;
import com.ruyuan.mq.client.producer.ProducerConfig;
import com.ruyuan.mq.client.producer.Message;
import com.ruyuan.mq.client.producer.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Quick Start Producer — 持续发送消息用于测试
 */
public class QuickStartProducer {

    private static final Logger logger = LoggerFactory.getLogger(QuickStartProducer.class);

    public static void main(String[] args) throws Exception {
        logger.info("=== RuYuan MQ Quick Start Producer ===");

        ProducerConfig config = new ProducerConfig();
        config.setProducerGroup("quickstart_producer_group");
        config.setNameServerAddr("localhost:9876");

        Producer producer = new ProducerImpl(config);
        producer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down producer...");
            producer.shutdown();
            logger.info("Producer shutdown complete");
        }));

        logger.info("Producer started, sending messages every 2 seconds...");

        AtomicInteger counter = new AtomicInteger(0);
        while (!Thread.currentThread().isInterrupted()) {
            try {
                int idx = counter.incrementAndGet();
                String body = "你好，MQ中间件 - " + idx;
                Message message = new Message("QuickStartTopic", "TagA", body.getBytes());
                message.setKey("key-" + idx);

                SendResult result = producer.send(message);
                logger.info("Sent #{}: messageId={}, queueId={}, offset={}",
                        idx, result.getMessageId(), result.getQueueId(), result.getQueueOffset());

                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Send failed", e);
                Thread.sleep(1000);
            }
        }

        logger.info("Producer finished");
    }
}
