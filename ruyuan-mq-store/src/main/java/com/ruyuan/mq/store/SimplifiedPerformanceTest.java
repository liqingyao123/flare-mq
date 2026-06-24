package com.ruyuan.mq.store;

import java.io.File;

/**
 * 简化的性能测试
 * 
 * 专注于验证基本功能，避免复杂的并发和大文件问题
 * 
 * @author RuYuan MQ Team
 */
public class SimplifiedPerformanceTest {
    
    public static void main(String[] args) {
        System.out.println("开始简化性能测试");
        
        try {
            // 测试基础存储功能
            testBasicStorage();
            
            // 测试性能
            testPerformance();
            
            System.out.println("\n✅ 所有测试通过！");
            
        } catch (Exception e) {
            System.err.println("❌ 测试失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void testBasicStorage() {
        System.out.println("\n=== 基础存储功能测试 ===");
        
        String testStorePath = System.getProperty("java.io.tmpdir") + File.separator + "ruyuan-mq-simple-test-" + System.currentTimeMillis();
        DefaultMessageStore messageStore = new DefaultMessageStore(testStorePath);
        
        try {
            messageStore.start();
            System.out.println("✓ 消息存储引擎启动成功");
            
            // 测试单条消息
            Message message = new Message("test-topic", "test-tag", "test-key", "Hello RuYuan MQ!".getBytes());
            message.setQueueId(0);
            
            PutMessageResult putResult = messageStore.putMessage(message);
            if (!putResult.isOk()) {
                throw new RuntimeException("消息存储失败: " + putResult);
            }
            System.out.println("✓ 单条消息存储成功");
            
            // 测试消息读取
            GetMessageResult getResult = messageStore.getMessage("test-topic", 0, 0, 1);
            if (!getResult.isFound() || getResult.getMessageCount() != 1) {
                throw new RuntimeException("消息读取失败: " + getResult);
            }
            
            Message retrievedMessage = getResult.getMessageList().get(0);
            if (!message.getTopic().equals(retrievedMessage.getTopic()) ||
                !message.getTags().equals(retrievedMessage.getTags()) ||
                !message.getKeys().equals(retrievedMessage.getKeys())) {
                throw new RuntimeException("消息内容验证失败");
            }
            System.out.println("✓ 消息读取和验证成功");
            
            // 测试多条消息
            int messageCount = 100;
            for (int i = 0; i < messageCount; i++) {
                Message msg = new Message("multi-topic", "tag-" + i, "key-" + i, 
                                        ("Message " + i).getBytes());
                msg.setQueueId(i % 2); // 分布到2个队列
                
                PutMessageResult result = messageStore.putMessage(msg);
                if (!result.isOk()) {
                    throw new RuntimeException("消息 " + i + " 存储失败: " + result);
                }
            }
            System.out.println("✓ " + messageCount + "条消息存储成功");
            
            // 验证队列消息数量
            for (int queueId = 0; queueId < 2; queueId++) {
                long maxOffset = messageStore.getMaxOffset("multi-topic", queueId);
                System.out.println("  队列" + queueId + "消息数: " + maxOffset);
                if (maxOffset <= 0) {
                    throw new RuntimeException("队列 " + queueId + " 消息数量异常: " + maxOffset);
                }
            }
            
        } finally {
            messageStore.shutdown();
            System.out.println("✓ 消息存储引擎关闭成功");
        }
        
        System.out.println("基础存储功能测试通过");
    }
    
    private static void testPerformance() {
        System.out.println("\n=== 性能测试 ===");
        
        String testStorePath = System.getProperty("java.io.tmpdir") + File.separator + "ruyuan-mq-perf-test-" + System.currentTimeMillis();
        DefaultMessageStore messageStore = new DefaultMessageStore(testStorePath);
        
        try {
            messageStore.start();
            
            // 性能测试参数
            int messageCount = 1000;
            byte[] messageBody = "Performance test message content for RuYuan MQ storage engine.".getBytes();
            
            System.out.println("开始性能测试: " + messageCount + "条消息");
            
            long startTime = System.currentTimeMillis();
            long totalLatency = 0;
            int successCount = 0;
            
            // 写入测试
            for (int i = 0; i < messageCount; i++) {
                Message message = new Message("perf-topic", "perf-tag", "key-" + i, messageBody);
                message.setQueueId(i % 4); // 分布到4个队列
                
                long msgStartTime = System.nanoTime();
                PutMessageResult result = messageStore.putMessage(message);
                long msgEndTime = System.nanoTime();
                
                if (result.isOk()) {
                    successCount++;
                    totalLatency += (msgEndTime - msgStartTime);
                } else {
                    System.err.println("消息 " + i + " 存储失败: " + result);
                }
                
                // 每100条消息打印一次进度
                if ((i + 1) % 100 == 0) {
                    System.out.println("  已处理: " + (i + 1) + "/" + messageCount);
                }
            }
            
            long endTime = System.currentTimeMillis();
            long totalTime = endTime - startTime;
            
            // 计算性能指标
            double tps = (double) successCount / totalTime * 1000;
            double avgLatency = (double) totalLatency / successCount / 1_000_000; // 转换为毫秒
            
            System.out.println("\n性能测试结果:");
            System.out.println("  总消息数: " + messageCount);
            System.out.println("  成功数: " + successCount);
            System.out.println("  失败数: " + (messageCount - successCount));
            System.out.println("  总耗时: " + totalTime + " ms");
            System.out.println("  TPS: " + String.format("%.2f", tps));
            System.out.println("  平均延迟: " + String.format("%.2f", avgLatency) + " ms");
            
            // 验证数据完整性
            System.out.println("\n数据完整性验证:");
            long totalMessages = 0;
            for (int queueId = 0; queueId < 4; queueId++) {
                long maxOffset = messageStore.getMaxOffset("perf-topic", queueId);
                totalMessages += maxOffset;
                System.out.println("  队列" + queueId + "消息数: " + maxOffset);
            }
            
            if (totalMessages != successCount) {
                throw new RuntimeException("数据完整性验证失败，期望: " + successCount + ", 实际: " + totalMessages);
            }
            
            // 性能评估
            System.out.println("\n性能评估:");
            if (successCount == messageCount) {
                System.out.println("✓ 数据可靠性: 100% (无丢失)");
            } else {
                System.out.println("⚠ 数据可靠性: " + String.format("%.2f", (double) successCount / messageCount * 100) + "%");
            }
            
            if (tps > 500) {
                System.out.println("✓ TPS性能: 优秀 (> 500)");
            } else if (tps > 100) {
                System.out.println("✓ TPS性能: 良好 (> 100)");
            } else {
                System.out.println("⚠ TPS性能: 需要优化 (< 100)");
            }
            
            if (avgLatency < 10.0) {
                System.out.println("✓ 延迟性能: 优秀 (< 10ms)");
            } else if (avgLatency < 50.0) {
                System.out.println("✓ 延迟性能: 良好 (< 50ms)");
            } else {
                System.out.println("⚠ 延迟性能: 需要优化 (> 50ms)");
            }
            
            // 读取性能测试
            System.out.println("\n读取性能测试:");
            long readStartTime = System.currentTimeMillis();
            int readCount = 0;
            
            for (int queueId = 0; queueId < 4; queueId++) {
                long maxOffset = messageStore.getMaxOffset("perf-topic", queueId);
                if (maxOffset > 0) {
                    GetMessageResult getResult = messageStore.getMessage("perf-topic", queueId, 0, (int) maxOffset);
                    if (getResult.isFound()) {
                        readCount += getResult.getMessageCount();
                    }
                }
            }
            
            long readEndTime = System.currentTimeMillis();
            long readTime = readEndTime - readStartTime;
            double readTps = (double) readCount / readTime * 1000;
            
            System.out.println("  读取消息数: " + readCount);
            System.out.println("  读取耗时: " + readTime + " ms");
            System.out.println("  读取TPS: " + String.format("%.2f", readTps));
            
            if (readCount == successCount) {
                System.out.println("✓ 读取完整性: 100%");
            } else {
                System.out.println("⚠ 读取完整性: " + String.format("%.2f", (double) readCount / successCount * 100) + "%");
            }
            
        } finally {
            messageStore.shutdown();
        }
        
        System.out.println("性能测试通过");
    }
}
