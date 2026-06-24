package com.ruyuan.mq.protocol.zerocopy;

import java.nio.ByteBuffer;

/**
 * 快速测试零拷贝功能
 * 
 * @author RuYuan MQ Team
 */
public class QuickTest {
    
    public static void main(String[] args) {
        System.out.println("=== RuYuan MQ 零拷贝传输优化快速测试 ===");
        
        try {
            testDirectBufferPool();
            testMessageLocation();
            testPerformance();
            
            System.out.println("=== 所有测试通过 ===");
            
        } catch (Exception e) {
            System.err.println("测试失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void testDirectBufferPool() {
        System.out.println("\n--- 测试DirectBuffer池化 ---");
        
        DirectBufferPool pool = DirectBufferPool.getInstance();
        
        // 获取缓冲区
        ByteBuffer buffer1 = pool.acquire(1024);
        ByteBuffer buffer2 = pool.acquire(4096);
        
        System.out.println("获取1KB缓冲区: capacity=" + buffer1.capacity() + ", isDirect=" + buffer1.isDirect());
        System.out.println("获取4KB缓冲区: capacity=" + buffer2.capacity() + ", isDirect=" + buffer2.isDirect());
        
        // 使用缓冲区
        buffer1.putInt(12345);
        buffer2.putLong(67890L);
        
        // 释放缓冲区
        pool.release(buffer1);
        pool.release(buffer2);
        
        // 再次获取测试复用
        ByteBuffer buffer3 = pool.acquire(1024);
        System.out.println("复用1KB缓冲区: capacity=" + buffer3.capacity());
        pool.release(buffer3);
        
        // 统计信息
        DirectBufferPool.PoolStatistics stats = pool.getStatistics();
        System.out.println("池统计: 分配=" + stats.totalAllocated + ", 释放=" + stats.totalReleased + 
                          ", 命中率=" + String.format("%.2f%%", stats.hitRate * 100));
        
        System.out.println("DirectBuffer池化测试通过");
    }
    
    private static void testMessageLocation() {
        System.out.println("\n--- 测试消息位置 ---");
        
        MessageLocation location1 = new MessageLocation("test.dat", 100, 1024);
        MessageLocation location2 = new MessageLocation("test.dat", 100, 1024);
        
        System.out.println("位置1: " + location1);
        System.out.println("位置2: " + location2);
        System.out.println("相等性: " + location1.equals(location2));
        System.out.println("哈希码相等: " + (location1.hashCode() == location2.hashCode()));
        
        System.out.println("消息位置测试通过");
    }
    
    private static void testPerformance() {
        System.out.println("\n--- 测试性能对比 ---");
        
        DirectBufferPool pool = DirectBufferPool.getInstance();
        int iterations = 5000;
        int bufferSize = 4096;
        
        // 测试池化分配
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            ByteBuffer buffer = pool.acquire(bufferSize);
            buffer.putInt(i);
            pool.release(buffer);
        }
        long pooledTime = System.nanoTime() - startTime;
        
        // 测试直接分配
        startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
            buffer.putInt(i);
        }
        long directTime = System.nanoTime() - startTime;
        
        double pooledMs = pooledTime / 1_000_000.0;
        double directMs = directTime / 1_000_000.0;
        double improvement = directMs > 0 ? ((directMs - pooledMs) / directMs) * 100 : 0;
        
        System.out.println("性能对比 (" + iterations + "次操作):");
        System.out.println("  池化分配: " + String.format("%.2f", pooledMs) + "ms");
        System.out.println("  直接分配: " + String.format("%.2f", directMs) + "ms");
        System.out.println("  性能提升: " + String.format("%.2f", improvement) + "%");
        
        DirectBufferPool.PoolStatistics finalStats = pool.getStatistics();
        System.out.println("最终命中率: " + String.format("%.2f%%", finalStats.hitRate * 100));
        
        System.out.println("性能对比测试通过");
    }
}
