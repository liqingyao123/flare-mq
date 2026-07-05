package com.flare.mq.example.performance;

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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Performance Test Example
 *
 * Tests FlareMQ performance metrics:
 * - Throughput test
 * - Latency test
 * - Concurrency test
 * - Large message test
 *
 * @author FlareMQ Team
 */
public class PerformanceTestExample {

    private static final Logger logger = LoggerFactory.getLogger(PerformanceTestExample.class);

    public static void main(String[] args) {
        logger.info("=== FlareMQ Performance Test Example ===");

        try {
            // Throughput test
            throughputTest();

            // Latency test
            latencyTest();

            // Concurrency test
            concurrencyTest();

            // Large message test
            largeMessageTest();

            logger.info("=== Performance test example completed ===");

        } catch (Exception e) {
            logger.error("Performance test example execution failed", e);
        }
    }

    /**
     * Throughput test
     */
    private static void throughputTest() throws Exception {
        logger.info("--- Throughput test ---");

        ProducerConfig config = new ProducerConfig();
        config.setProducerGroup("throughput_producer_group");
        config.setNameServerAddr("localhost:9876");
        config.setSendMsgTimeout(5000);

        Producer producer = new ProducerImpl(config);

        try {
            producer.start();

            int messageCount = 10000; // Send 10,000 messages
            byte[] messageBody = "Performance test message content".getBytes();

            long startTime = System.currentTimeMillis();
            AtomicLong successCount = new AtomicLong(0);
            AtomicLong failureCount = new AtomicLong(0);
            CountDownLatch latch = new CountDownLatch(messageCount);

            logger.info("Starting throughput test, sending {} messages", messageCount);
            
            // Send messages asynchronously
            for (int i = 0; i < messageCount; i++) {
                Message message = new Message("ThroughputTopic", "PerfTag", messageBody);
                message.setKey("perf-key-" + i);

                producer.sendAsync(message, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        successCount.incrementAndGet();
                        latch.countDown();
                    }

                    @Override
                    public void onException(Throwable exception) {
                        failureCount.incrementAndGet();
                        latch.countDown();
                    }
                });
            }

            // Wait for all messages to be sent
            boolean finished = latch.await(60, TimeUnit.SECONDS);
            long endTime = System.currentTimeMillis();
            long totalTime = endTime - startTime;

            if (finished) {
                long success = successCount.get();
                long failure = failureCount.get();
                double tps = (double) success * 1000 / totalTime;

                logger.info("Throughput test results:");
                logger.info("  Total messages: {}", messageCount);
                logger.info("  Successful sends: {}", success);
                logger.info("  Failed sends: {}", failure);
                logger.info("  Total time: {}ms", totalTime);
                logger.info("  TPS: {:.2f}", tps);
                logger.info("  Average latency: {:.2f}ms", (double) totalTime / success);
            } else {
                logger.warn("Throughput test timeout");
            }

        } finally {
            producer.shutdown();
        }

        logger.info("--- Throughput test completed ---");
    }
    
    /**
     * Latency test
     */
    private static void latencyTest() throws Exception {
        logger.info("--- Latency test ---");

        ProducerConfig config = new ProducerConfig();
        config.setProducerGroup("latency_producer_group");
        config.setNameServerAddr("localhost:9876");

        Producer producer = new ProducerImpl(config);

        try {
            producer.start();

            int testCount = 1000;
            long[] latencies = new long[testCount];
            byte[] messageBody = "Latency test message".getBytes();

            logger.info("Starting latency test, sending {} messages", testCount);

            // Send messages synchronously and measure latency
            for (int i = 0; i < testCount; i++) {
                Message message = new Message("LatencyTopic", "LatencyTag", messageBody);
                message.setKey("latency-key-" + i);

                long startTime = System.nanoTime();
                SendResult result = producer.send(message);
                long endTime = System.nanoTime();

                latencies[i] = (endTime - startTime) / 1_000_000; // Convert to milliseconds

                if (i % 100 == 0) {
                    logger.info("Sent {} messages", i);
                }
            }

            // Calculate latency statistics
            java.util.Arrays.sort(latencies);
            long min = latencies[0];
            long max = latencies[testCount - 1];
            long p50 = latencies[testCount / 2];
            long p95 = latencies[(int) (testCount * 0.95)];
            long p99 = latencies[(int) (testCount * 0.99)];
            double avg = java.util.Arrays.stream(latencies).average().orElse(0.0);

            logger.info("Latency test results:");
            logger.info("  Test message count: {}", testCount);
            logger.info("  Min latency: {}ms", min);
            logger.info("  Max latency: {}ms", max);
            logger.info("  Average latency: {:.2f}ms", avg);
            logger.info("  P50 latency: {}ms", p50);
            logger.info("  P95 latency: {}ms", p95);
            logger.info("  P99 latency: {}ms", p99);

        } finally {
            producer.shutdown();
        }

        logger.info("--- Latency test completed ---");
    }
    
    /**
     * 并发测试
     */
    private static void concurrencyTest() throws Exception {
        logger.info("--- concurrency test ---");
        
        int threadCount = 10;
        int messagesPerThread = 1000;
        int totalMessages = threadCount * messagesPerThread;
        
        ProducerConfig config = new ProducerConfig();
        config.setProducerGroup("concurrent_producer_group");
        config.setNameServerAddr("localhost:9876");
        
        Producer producer = new ProducerImpl(config);
        
        try {
            producer.start();
            
            AtomicLong successCount = new AtomicLong(0);
            AtomicLong failureCount = new AtomicLong(0);
            CountDownLatch latch = new CountDownLatch(totalMessages);
            
            logger.info("Start concurrency test, {} threads, {} messages per thread", threadCount, messagesPerThread);
            
            long startTime = System.currentTimeMillis();
            
            // 启动多个线程并发发送
            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                new Thread(() -> {
                    try {
                        for (int i = 0; i < messagesPerThread; i++) {
                            Message message = new Message("ConcurrentTopic", "ConcurrentTag", 
                                                        ("concurrent message-" + threadId + "-" + i).getBytes());
                            message.setKey("concurrent-" + threadId + "-" + i);
                            
                            producer.sendAsync(message, new SendCallback() {
                                @Override
                                public void onSuccess(SendResult sendResult) {
                                    successCount.incrementAndGet();
                                    latch.countDown();
                                }
                                
                                @Override
                                public void onException(Throwable exception) {
                                    failureCount.incrementAndGet();
                                    latch.countDown();
                                }
                            });
                        }
                    } catch (Exception e) {
                        logger.error("Thread {} send message failed", threadId, e);
                    }
                }).start();
            }
            
            // 等待所有消息发送完成
            boolean finished = latch.await(120, TimeUnit.SECONDS);
            long endTime = System.currentTimeMillis();
            long totalTime = endTime - startTime;
            
            if (finished) {
                long success = successCount.get();
                long failure = failureCount.get();
                double tps = (double) success * 1000 / totalTime;
                
                logger.info("Concurrency test results:");
                logger.info("  Concurrent thread count: {}", threadCount);
                logger.info("  Total message count: {}", totalMessages);
                logger.info("  Successful sends: {}", success);
                logger.info("  Failed sends: {}", failure);
                logger.info("  Total time: {}ms", totalTime);
                logger.info("  Concurrency TPS: {:.2f}", tps);
            } else {
                logger.warn("Concurrency test timeout");
            }
            
        } finally {
            producer.shutdown();
        }
        
        logger.info("--- Concurrency test completed ---");
    }
    
    /**
     * 大消息测试
     */
    private static void largeMessageTest() throws Exception {
        logger.info("--- Large message test ---");

        // 测试不同大小的消息
        int[] messageSizes = {1024, 4096, 16384, 65536, 262144, 1048576}; // 1KB到1MB

        for (int size : messageSizes) {
            // 为每个消息大小创建新的Producer实例，避免连接问题
            ProducerConfig config = new ProducerConfig();
            config.setProducerGroup("large_message_producer_group_" + size);
            config.setNameServerAddr("localhost:9876");
            config.setMaxMessageSize(16 * 1024 * 1024); // 16MB
            config.setSendMsgTimeout(30000); // 30秒超时
            config.setRetryTimesWhenSendFailed(3); // 失败重试3次

            Producer producer = new ProducerImpl(config);

            try {
                producer.start();

                byte[] largeBody = new byte[size];
                java.util.Arrays.fill(largeBody, (byte) 'A');

                Message message = new Message("LargeMessageTopic", "LargeTag", largeBody);
                message.setKey("large-key-" + size);

                long startTime = System.currentTimeMillis();

                try {
                    SendResult result = producer.send(message);
                    long endTime = System.currentTimeMillis();

                    logger.info("Large message test: size={}KB, time={}ms, messageId={}",
                               size / 1024, endTime - startTime, result.getMessageId());
                } catch (Exception e) {
                    long endTime = System.currentTimeMillis();
                    logger.warn("Large message test failed: size={}KB, time={}ms, error={}",
                               size / 1024, endTime - startTime, e.getMessage());

                    // 对于大消息失败，继续测试下一个大小
                    continue;
                }

                // 在发送大消息后稍作等待，避免连接问题
                if (size >= 4096) {
                    Thread.sleep(1000);
                }

            } finally {
                producer.shutdown();
            }
        }

        logger.info("--- Large message test completed ---");
    }
}
