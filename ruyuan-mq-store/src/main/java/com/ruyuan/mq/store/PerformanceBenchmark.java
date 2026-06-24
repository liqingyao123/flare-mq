package com.ruyuan.mq.store;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 性能基准测试
 * 
 * @author RuYuan MQ Team
 */
public class PerformanceBenchmark {
    
    public static void main(String[] args) {
        System.out.println("开始RuYuan MQ存储引擎性能基准测试");
        
        try {
            // 基础存储性能测试
            testBasicStoragePerformance();
            
            // 智能存储性能测试
            testIntelligentStoragePerformance();
            
            // 并发性能测试
            testConcurrentPerformance();
            
            System.out.println("\n所有性能测试完成！");
            
        } catch (Exception e) {
            System.err.println("性能测试失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void testBasicStoragePerformance() {
        System.out.println("\n=== 基础存储性能测试 ===");
        
        String testStorePath = System.getProperty("java.io.tmpdir") + File.separator + "ruyuan-mq-perf-test-" + System.currentTimeMillis();
        DefaultMessageStore messageStore = new DefaultMessageStore(testStorePath);
        
        try {
            messageStore.start();
            
            int messageCount = 10000;
            int[] messageSizes = {100, 1024, 4096}; // 不同大小的消息
            
            for (int size : messageSizes) {
                System.out.println("\n测试消息大小: " + size + " bytes");
                
                byte[] messageBody = new byte[size];
                for (int i = 0; i < messageBody.length; i++) {
                    messageBody[i] = (byte) (i % 256);
                }
                
                long startTime = System.currentTimeMillis();
                long totalLatency = 0;
                
                for (int i = 0; i < messageCount; i++) {
                    Message message = new Message("perf-topic", "perf-tag", "key-" + i, messageBody);
                    message.setQueueId(i % 4);
                    
                    long msgStartTime = System.nanoTime();
                    PutMessageResult result = messageStore.putMessage(message);
                    long msgEndTime = System.nanoTime();
                    
                    if (!result.isOk()) {
                        System.err.println("消息存储失败: " + result);
                        continue;
                    }
                    
                    totalLatency += (msgEndTime - msgStartTime);
                }
                
                long endTime = System.currentTimeMillis();
                long totalTime = endTime - startTime;
                
                double tps = (double) messageCount / totalTime * 1000;
                double avgLatency = (double) totalLatency / messageCount / 1_000_000; // 转换为毫秒
                
                System.out.println("消息数量: " + messageCount);
                System.out.println("总耗时: " + totalTime + " ms");
                System.out.println("TPS: " + String.format("%.2f", tps));
                System.out.println("平均延迟: " + String.format("%.2f", avgLatency) + " ms");
                
                // 验证数据完整性
                for (int queueId = 0; queueId < 4; queueId++) {
                    long maxOffset = messageStore.getMaxOffset("perf-topic", queueId);
                    System.out.println("队列" + queueId + "消息数: " + maxOffset);
                }
            }
            
        } finally {
            messageStore.shutdown();
        }
        
        System.out.println("基础存储性能测试完成");
    }
    
    private static void testIntelligentStoragePerformance() {
        System.out.println("\n=== 智能存储性能测试 ===");
        
        IntelligentStorageManager intelligentManager = new IntelligentStorageManager();
        intelligentManager.start();
        
        try {
            // 测试不同类型消息的存储决策性能
            String[] messageTypes = {"JSON", "TEXT", "BINARY"};
            byte[][] messageBodies = {
                "{\"name\":\"test\",\"value\":123,\"data\":[1,2,3,4,5]}".getBytes(),
                "Hello World! This is a test message with some repeated content.".getBytes(),
                new byte[2048]
            };
            
            for (int i = 0; i < messageBodies.length; i++) {
                String type = messageTypes[i];
                byte[] body = messageBodies[i];
                
                System.out.println("\n测试" + type + "消息存储决策性能:");
                
                int decisionCount = 1000;
                long startTime = System.nanoTime();
                
                for (int j = 0; j < decisionCount; j++) {
                    Message message = new Message(type.toLowerCase() + "-topic", type.toLowerCase() + "-tag", 
                                                "key-" + j, body);
                    message.setQueueId(0);
                    
                    StorageDecision decision = intelligentManager.analyzeMessage(message);
                    
                    // 模拟一些访问
                    if (j % 10 == 0) {
                        intelligentManager.recordMessageAccess(message.getTopic(), message.getQueueId());
                    }
                }
                
                long endTime = System.nanoTime();
                long totalTime = endTime - startTime;
                
                double avgDecisionTime = (double) totalTime / decisionCount / 1_000_000; // 转换为毫秒
                double decisionsPerSecond = (double) decisionCount / totalTime * 1_000_000_000; // 每秒决策数
                
                System.out.println("决策数量: " + decisionCount);
                System.out.println("总耗时: " + String.format("%.2f", totalTime / 1_000_000.0) + " ms");
                System.out.println("平均决策时间: " + String.format("%.4f", avgDecisionTime) + " ms");
                System.out.println("决策TPS: " + String.format("%.0f", decisionsPerSecond));
            }
            
            // 获取智能存储统计
            IntelligentStorageStats stats = intelligentManager.getStats();
            System.out.println("\n智能存储统计: " + stats);
            
        } finally {
            intelligentManager.shutdown();
        }
        
        System.out.println("智能存储性能测试完成");
    }
    
    private static void testConcurrentPerformance() throws InterruptedException {
        System.out.println("\n=== 并发性能测试 ===");
        
        String testStorePath = System.getProperty("java.io.tmpdir") + File.separator + "ruyuan-mq-concurrent-test-" + System.currentTimeMillis();
        DefaultMessageStore messageStore = new DefaultMessageStore(testStorePath);
        
        try {
            messageStore.start();
            
            int threadCount = 8;
            int messagesPerThread = 1000;
            int totalMessages = threadCount * messagesPerThread;
            
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);
            AtomicLong totalLatency = new AtomicLong(0);
            
            System.out.println("并发配置: " + threadCount + "个线程，每线程" + messagesPerThread + "条消息");
            
            long startTime = System.currentTimeMillis();
            
            // 启动多线程写入
            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < messagesPerThread; i++) {
                            long msgStartTime = System.nanoTime();
                            
                            Message message = new Message("concurrent-topic", "concurrent-tag", 
                                                        "key-" + threadId + "-" + i,
                                                        ("Concurrent test message " + threadId + "-" + i).getBytes());
                            message.setQueueId(threadId % 4);
                            
                            PutMessageResult result = messageStore.putMessage(message);
                            
                            long msgEndTime = System.nanoTime();
                            totalLatency.addAndGet(msgEndTime - msgStartTime);
                            
                            if (result.isOk()) {
                                successCount.incrementAndGet();
                            } else {
                                errorCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("线程 " + threadId + " 执行异常: " + e.getMessage());
                        errorCount.addAndGet(messagesPerThread);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            
            // 等待所有线程完成
            latch.await();
            executor.shutdown();
            
            long endTime = System.currentTimeMillis();
            long totalTime = endTime - startTime;
            
            // 计算性能指标
            double tps = (double) totalMessages / totalTime * 1000;
            double avgLatency = (double) totalLatency.get() / successCount.get() / 1_000_000; // 转换为毫秒
            
            System.out.println("\n并发性能测试结果:");
            System.out.println("总消息数: " + totalMessages);
            System.out.println("成功数: " + successCount.get());
            System.out.println("失败数: " + errorCount.get());
            System.out.println("总耗时: " + totalTime + " ms");
            System.out.println("并发TPS: " + String.format("%.2f", tps));
            System.out.println("平均延迟: " + String.format("%.2f", avgLatency) + " ms");
            
            // 验证数据完整性
            System.out.println("\n数据完整性验证:");
            for (int queueId = 0; queueId < 4; queueId++) {
                long maxOffset = messageStore.getMaxOffset("concurrent-topic", queueId);
                System.out.println("队列" + queueId + "消息数: " + maxOffset);
            }
            
            // 性能评估
            System.out.println("\n性能评估:");
            if (tps > 5000) {
                System.out.println("✓ 并发TPS优秀 (> 5000)");
            } else if (tps > 1000) {
                System.out.println("✓ 并发TPS良好 (> 1000)");
            } else {
                System.out.println("⚠ 并发TPS需要优化 (< 1000)");
            }
            
            if (avgLatency < 5.0) {
                System.out.println("✓ 平均延迟优秀 (< 5ms)");
            } else if (avgLatency < 10.0) {
                System.out.println("✓ 平均延迟良好 (< 10ms)");
            } else {
                System.out.println("⚠ 平均延迟需要优化 (> 10ms)");
            }
            
            if (errorCount.get() == 0) {
                System.out.println("✓ 数据可靠性优秀 (无错误)");
            } else {
                System.out.println("⚠ 数据可靠性需要关注 (有" + errorCount.get() + "个错误)");
            }
            
        } finally {
            messageStore.shutdown();
        }
        
        System.out.println("并发性能测试完成");
    }
}
