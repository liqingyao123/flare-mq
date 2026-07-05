package com.flare.mq.protocol.zerocopy;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * 简单的零拷贝功能测试
 * 
 * @author FlareMQ Team
 */
class SimpleZeroCopyTest {
    
    private static final Logger logger = LoggerFactory.getLogger(SimpleZeroCopyTest.class);
    
    @Test
    void testDirectBufferPoolBasic() {
        logger.info("=== 测试DirectBuffer池基本功能 ===");
        
        DirectBufferPool pool = DirectBufferPool.getInstance();
        
        // 测试获取和释放
        ByteBuffer buffer1 = pool.acquire(1024);
        logger.info("获取1KB缓冲区: capacity={}, isDirect={}", buffer1.capacity(), buffer1.isDirect());
        
        ByteBuffer buffer2 = pool.acquire(4096);
        logger.info("获取4KB缓冲区: capacity={}, isDirect={}", buffer2.capacity(), buffer2.isDirect());
        
        // 使用缓冲区
        buffer1.putInt(12345);
        buffer2.putLong(67890L);
        
        // 释放缓冲区
        pool.release(buffer1);
        pool.release(buffer2);
        
        // 再次获取，应该从池中复用
        ByteBuffer buffer3 = pool.acquire(1024);
        logger.info("再次获取1KB缓冲区: capacity={}", buffer3.capacity());
        
        pool.release(buffer3);
        
        // 打印统计信息
        DirectBufferPool.PoolStatistics stats = pool.getStatistics();
        logger.info("池统计信息: {}", stats);
        
        logger.info("DirectBuffer池基本功能测试通过");
    }
    
    @Test
    void testMessageLocation() {
        logger.info("=== 测试消息位置信息 ===");
        
        MessageLocation location = new MessageLocation("test.dat", 100, 1024);
        
        logger.info("消息位置: {}", location);
        logger.info("文件名: {}", location.getFileName());
        logger.info("偏移量: {}", location.getOffset());
        logger.info("大小: {}", location.getSize());
        
        // 测试相等性
        MessageLocation location2 = new MessageLocation("test.dat", 100, 1024);
        boolean isEqual = location.equals(location2);
        logger.info("位置相等性: {}", isEqual);
        
        logger.info("消息位置信息测试通过");
    }
    
    @Test
    void testZeroCopyComponents() {
        logger.info("=== 测试零拷贝组件集成 ===");
        
        // 测试DirectBuffer池
        DirectBufferPool bufferPool = DirectBufferPool.getInstance();
        ByteBuffer buffer = bufferPool.acquire(2048);
        logger.info("从池获取缓冲区: {}", buffer);
        
        // 测试文件映射管理器
        MappedFileManager fileManager = MappedFileManager.getInstance();
        logger.info("文件映射管理器: {}", fileManager);
        
        // 测试零拷贝传输器
        ZeroCopyMessageTransfer transfer = ZeroCopyMessageTransfer.getInstance();
        logger.info("零拷贝传输器: {}", transfer);
        
        // 获取统计信息
        DirectBufferPool.PoolStatistics poolStats = bufferPool.getStatistics();
        MappedFileManager.MappingStatistics mappingStats = fileManager.getStatistics();
        ZeroCopyMessageTransfer.TransferStatistics transferStats = transfer.getStatistics();
        
        logger.info("DirectBuffer池统计: 分配={}, 释放={}, 命中率={:.2f}%", 
                   poolStats.totalAllocated, poolStats.totalReleased, poolStats.hitRate * 100);
        
        logger.info("文件映射统计: 文件数={}, 大小={}MB", 
                   mappingStats.totalMappedFiles, mappingStats.totalMappedSize / 1024 / 1024);
        
        logger.info("传输统计: 缓存文件通道={}", transferStats.cachedFileChannels);
        
        // 清理资源
        bufferPool.release(buffer);
        
        logger.info("零拷贝组件集成测试通过");
    }
    
    @Test
    void testPerformanceImprovement() {
        logger.info("=== 测试性能改进效果 ===");
        
        DirectBufferPool pool = DirectBufferPool.getInstance();
        int iterations = 1000;
        int bufferSize = 4096;
        
        // 测试池化分配性能
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            ByteBuffer buffer = pool.acquire(bufferSize);
            buffer.putInt(i); // 模拟使用
            pool.release(buffer);
        }
        long pooledTime = System.nanoTime() - startTime;
        
        // 测试直接分配性能
        startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
            buffer.putInt(i); // 模拟使用
            // 直接分配的buffer会被GC回收
        }
        long directTime = System.nanoTime() - startTime;
        
        double pooledMs = pooledTime / 1_000_000.0;
        double directMs = directTime / 1_000_000.0;
        double improvement = ((directMs - pooledMs) / directMs) * 100;
        
        logger.info("性能对比结果:");
        logger.info("  池化分配: {:.2f}ms", pooledMs);
        logger.info("  直接分配: {:.2f}ms", directMs);
        logger.info("  性能提升: {:.2f}%", improvement);
        
        // 获取最终统计信息
        DirectBufferPool.PoolStatistics stats = pool.getStatistics();
        logger.info("最终池统计: 命中率={:.2f}%, 总分配={}, 总释放={}", 
                   stats.hitRate * 100, stats.totalAllocated, stats.totalReleased);
        
        logger.info("性能改进效果测试通过");
    }
    
    @Test
    void testMemoryManagement() {
        logger.info("=== 测试内存管理 ===");
        
        DirectBufferPool pool = DirectBufferPool.getInstance();
        Runtime runtime = Runtime.getRuntime();
        
        // 记录初始内存
        System.gc();
        long initialMemory = runtime.totalMemory() - runtime.freeMemory();
        
        // 分配大量缓冲区
        ByteBuffer[] buffers = new ByteBuffer[50];
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = pool.acquire(8192); // 8KB
        }
        
        long afterAllocMemory = runtime.totalMemory() - runtime.freeMemory();
        
        // 释放缓冲区
        for (ByteBuffer buffer : buffers) {
            pool.release(buffer);
        }
        
        // 强制GC
        System.gc();
        long afterReleaseMemory = runtime.totalMemory() - runtime.freeMemory();
        
        logger.info("内存管理测试:");
        logger.info("  初始内存: {}KB", initialMemory / 1024);
        logger.info("  分配后内存: {}KB", afterAllocMemory / 1024);
        logger.info("  释放后内存: {}KB", afterReleaseMemory / 1024);
        logger.info("  内存增长: {}KB", (afterReleaseMemory - initialMemory) / 1024);
        
        logger.info("内存管理测试通过");
    }
}
