package com.ruyuan.mq.protocol.zerocopy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * 零拷贝功能测试主类
 * 
 * @author RuYuan MQ Team
 */
public class ZeroCopyTestMain {
    
    private static final Logger logger = LoggerFactory.getLogger(ZeroCopyTestMain.class);
    
    public static void main(String[] args) {
        logger.info("=== RuYuan MQ 零拷贝传输优化测试 ===");
        
        try {
            // 测试DirectBuffer池化
            testDirectBufferPool();
            
            // 测试消息位置
            testMessageLocation();
            
            // 测试性能对比
            testPerformanceComparison();
            
            // 测试内存效率
            testMemoryEfficiency();
            
            logger.info("=== 所有测试完成 ===");
            
        } catch (Exception e) {
            logger.error("测试过程中发生错误", e);
        }
    }
    
    /**
     * 测试DirectBuffer池化
     */
    private static void testDirectBufferPool() {
        logger.info("\n--- 测试DirectBuffer池化 ---");
        
        DirectBufferPool pool = DirectBufferPool.getInstance();
        
        // 测试不同大小的缓冲区
        ByteBuffer buffer1 = pool.acquire(1024);
        ByteBuffer buffer2 = pool.acquire(4096);
        ByteBuffer buffer3 = pool.acquire(16384);
        
        logger.info("获取缓冲区:");
        logger.info("  1KB: capacity={}, isDirect={}", buffer1.capacity(), buffer1.isDirect());
        logger.info("  4KB: capacity={}, isDirect={}", buffer2.capacity(), buffer2.isDirect());
        logger.info("  16KB: capacity={}, isDirect={}", buffer3.capacity(), buffer3.isDirect());
        
        // 使用缓冲区
        buffer1.putInt(0, 12345);
        buffer2.putLong(0, 67890L);
        buffer3.put(0, (byte) 255);
        
        // 释放缓冲区
        pool.release(buffer1);
        pool.release(buffer2);
        pool.release(buffer3);
        
        // 再次获取，测试复用
        ByteBuffer reusedBuffer = pool.acquire(1024);
        logger.info("复用缓冲区: capacity={}", reusedBuffer.capacity());
        pool.release(reusedBuffer);
        
        // 打印统计信息
        DirectBufferPool.PoolStatistics stats = pool.getStatistics();
        logger.info("池统计信息:");
        logger.info("  总分配: {}", stats.totalAllocated);
        logger.info("  总释放: {}", stats.totalReleased);
        logger.info("  池命中: {}", stats.poolHits);
        logger.info("  池未命中: {}", stats.poolMisses);
        logger.info("  命中率: {:.2f}%", stats.hitRate * 100);
    }
    
    /**
     * 测试消息位置
     */
    private static void testMessageLocation() {
        logger.info("\n--- 测试消息位置 ---");
        
        MessageLocation location1 = new MessageLocation("commitlog_001", 1024, 2048);
        MessageLocation location2 = new MessageLocation("commitlog_001", 1024, 2048);
        MessageLocation location3 = new MessageLocation("commitlog_002", 2048, 1024);
        
        logger.info("消息位置1: {}", location1);
        logger.info("消息位置2: {}", location2);
        logger.info("消息位置3: {}", location3);
        
        logger.info("位置1和位置2相等: {}", location1.equals(location2));
        logger.info("位置1和位置3相等: {}", location1.equals(location3));
        logger.info("位置1哈希码: {}", location1.hashCode());
        logger.info("位置2哈希码: {}", location2.hashCode());
    }
    
    /**
     * 测试性能对比
     */
    private static void testPerformanceComparison() {
        logger.info("\n--- 测试性能对比 ---");
        
        DirectBufferPool pool = DirectBufferPool.getInstance();
        int iterations = 10000;
        int bufferSize = 8192;
        
        // 测试池化分配性能
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            ByteBuffer buffer = pool.acquire(bufferSize);
            buffer.putInt(0, i); // 模拟使用
            pool.release(buffer);
        }
        long pooledTime = System.nanoTime() - startTime;
        
        // 测试直接分配性能
        startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
            buffer.putInt(0, i); // 模拟使用
            // 直接分配的buffer会被GC回收
        }
        long directTime = System.nanoTime() - startTime;
        
        double pooledMs = pooledTime / 1_000_000.0;
        double directMs = directTime / 1_000_000.0;
        double improvement = directMs > 0 ? ((directMs - pooledMs) / directMs) * 100 : 0;
        
        logger.info("性能对比结果 ({}次操作):", iterations);
        logger.info("  池化分配耗时: {:.2f}ms", pooledMs);
        logger.info("  直接分配耗时: {:.2f}ms", directMs);
        logger.info("  性能提升: {:.2f}%", improvement);
        logger.info("  平均每次操作 - 池化: {:.2f}μs", pooledMs * 1000 / iterations);
        logger.info("  平均每次操作 - 直接: {:.2f}μs", directMs * 1000 / iterations);
        
        // 更新后的统计信息
        DirectBufferPool.PoolStatistics finalStats = pool.getStatistics();
        logger.info("最终池统计:");
        logger.info("  命中率: {:.2f}%", finalStats.hitRate * 100);
        logger.info("  总操作: {}", finalStats.poolHits + finalStats.poolMisses);
    }
    
    /**
     * 测试内存效率
     */
    private static void testMemoryEfficiency() {
        logger.info("\n--- 测试内存效率 ---");
        
        DirectBufferPool pool = DirectBufferPool.getInstance();
        Runtime runtime = Runtime.getRuntime();
        
        // 强制GC并记录初始内存
        System.gc();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        long initialMemory = runtime.totalMemory() - runtime.freeMemory();
        
        // 分配大量缓冲区
        int bufferCount = 100;
        int bufferSize = 4096;
        ByteBuffer[] buffers = new ByteBuffer[bufferCount];
        
        for (int i = 0; i < bufferCount; i++) {
            buffers[i] = pool.acquire(bufferSize);
            buffers[i].putInt(0, i); // 模拟使用
        }
        
        long afterAllocMemory = runtime.totalMemory() - runtime.freeMemory();
        
        // 释放所有缓冲区
        for (ByteBuffer buffer : buffers) {
            pool.release(buffer);
        }
        
        // 强制GC
        System.gc();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        long afterReleaseMemory = runtime.totalMemory() - runtime.freeMemory();
        
        logger.info("内存效率测试 ({}个{}KB缓冲区):", bufferCount, bufferSize / 1024);
        logger.info("  初始内存: {}KB", initialMemory / 1024);
        logger.info("  分配后内存: {}KB", afterAllocMemory / 1024);
        logger.info("  释放后内存: {}KB", afterReleaseMemory / 1024);
        logger.info("  分配时内存增长: {}KB", (afterAllocMemory - initialMemory) / 1024);
        logger.info("  最终内存增长: {}KB", (afterReleaseMemory - initialMemory) / 1024);
        logger.info("  平均每个缓冲区内存开销: {}B", 
                   (afterReleaseMemory - initialMemory) / bufferCount);
    }
    
    /**
     * 测试零拷贝组件
     */
    private static void testZeroCopyComponents() {
        logger.info("\n--- 测试零拷贝组件 ---");
        
        // 测试文件映射管理器
        MappedFileManager fileManager = MappedFileManager.getInstance();
        MappedFileManager.MappingStatistics mappingStats = fileManager.getStatistics();
        
        logger.info("文件映射管理器统计:");
        logger.info("  映射文件数: {}", mappingStats.totalMappedFiles);
        logger.info("  映射大小: {}MB", mappingStats.totalMappedSize / 1024 / 1024);
        logger.info("  映射命中: {}", mappingStats.mappingHits);
        logger.info("  映射未命中: {}", mappingStats.mappingMisses);
        
        // 测试零拷贝传输器
        ZeroCopyMessageTransfer transfer = ZeroCopyMessageTransfer.getInstance();
        ZeroCopyMessageTransfer.TransferStatistics transferStats = transfer.getStatistics();
        
        logger.info("零拷贝传输器统计:");
        logger.info("  缓存文件通道数: {}", transferStats.cachedFileChannels);
        
        logger.info("零拷贝组件测试完成");
    }
}
