package com.flare.mq.protocol.zerocopy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 文件映射管理器
 * 
 * 管理文件的内存映射，提供高效的文件访问和零拷贝传输支持
 * 
 * @author FlareMQ Team
 */
public class MappedFileManager {
    
    private static final Logger logger = LoggerFactory.getLogger(MappedFileManager.class);
    
    /**
     * 最大映射文件大小：1GB
     */
    private final long maxMappedSize = 1024 * 1024 * 1024L;
    
    /**
     * 映射文件缓存
     */
    private final ConcurrentHashMap<String, MappedFileEntry> mappedFiles;
    
    /**
     * 统计信息
     */
    private final AtomicLong totalMappedFiles = new AtomicLong(0);
    private final AtomicLong totalMappedSize = new AtomicLong(0);
    private final AtomicLong mappingHits = new AtomicLong(0);
    private final AtomicLong mappingMisses = new AtomicLong(0);
    
    /**
     * 单例实例
     */
    private static volatile MappedFileManager instance;
    
    /**
     * 私有构造函数
     */
    private MappedFileManager() {
        this.mappedFiles = new ConcurrentHashMap<>();
    }
    
    /**
     * 获取单例实例
     */
    public static MappedFileManager getInstance() {
        if (instance == null) {
            synchronized (MappedFileManager.class) {
                if (instance == null) {
                    instance = new MappedFileManager();
                }
            }
        }
        return instance;
    }
    
    /**
     * 映射文件的指定区域
     */
    public MappedByteBuffer map(MessageLocation location) {
        if (location == null) {
            return null;
        }
        
        String fileName = location.getFileName();
        MappedFileEntry entry = mappedFiles.get(fileName);
        
        if (entry != null) {
            mappingHits.incrementAndGet();
            return entry.slice(location.getOffset(), location.getSize());
        }
        
        // 缓存中没有，创建新的映射
        mappingMisses.incrementAndGet();
        entry = createMappedFile(fileName);
        
        if (entry != null) {
            mappedFiles.put(fileName, entry);
            return entry.slice(location.getOffset(), location.getSize());
        }
        
        return null;
    }
    
    /**
     * 创建文件映射
     */
    private MappedFileEntry createMappedFile(String fileName) {
        try {
            RandomAccessFile file = new RandomAccessFile(fileName, "r");
            FileChannel channel = file.getChannel();
            long fileSize = channel.size();
            
            // 检查文件大小限制
            if (fileSize > maxMappedSize) {
                logger.warn("文件太大，无法完全映射: fileName={}, size={}", fileName, fileSize);
                // 只映射前面的部分
                fileSize = maxMappedSize;
            }
            
            MappedByteBuffer mappedBuffer = channel.map(
                FileChannel.MapMode.READ_ONLY, 
                0, 
                fileSize
            );
            
            totalMappedFiles.incrementAndGet();
            totalMappedSize.addAndGet(fileSize);
            
            logger.debug("创建文件映射成功: fileName={}, size={}", fileName, fileSize);
            
            return new MappedFileEntry(file, channel, mappedBuffer, fileSize);
            
        } catch (Exception e) {
            logger.error("创建文件映射失败: " + fileName, e);
            return null;
        }
    }
    
    /**
     * 预加载文件映射
     */
    public void preloadFile(String fileName) {
        if (!mappedFiles.containsKey(fileName)) {
            MappedFileEntry entry = createMappedFile(fileName);
            if (entry != null) {
                mappedFiles.put(fileName, entry);
                logger.info("预加载文件映射成功: {}", fileName);
            }
        }
    }
    
    /**
     * 移除文件映射
     */
    public void removeMapping(String fileName) {
        MappedFileEntry entry = mappedFiles.remove(fileName);
        if (entry != null) {
            entry.cleanup();
            totalMappedFiles.decrementAndGet();
            totalMappedSize.addAndGet(-entry.getSize());
            logger.info("移除文件映射: {}", fileName);
        }
    }
    
    /**
     * 清理所有映射
     */
    public void cleanup() {
        logger.info("开始清理所有文件映射...");
        
        mappedFiles.values().forEach(MappedFileEntry::cleanup);
        mappedFiles.clear();
        
        totalMappedFiles.set(0);
        totalMappedSize.set(0);
        mappingHits.set(0);
        mappingMisses.set(0);
        
        logger.info("文件映射清理完成");
    }
    
    /**
     * 获取统计信息
     */
    public MappingStatistics getStatistics() {
        MappingStatistics stats = new MappingStatistics();
        stats.totalMappedFiles = totalMappedFiles.get();
        stats.totalMappedSize = totalMappedSize.get();
        stats.mappingHits = mappingHits.get();
        stats.mappingMisses = mappingMisses.get();
        stats.hitRate = stats.mappingHits + stats.mappingMisses > 0 ? 
                       (double) stats.mappingHits / (stats.mappingHits + stats.mappingMisses) : 0.0;
        return stats;
    }
    
    /**
     * 映射文件条目
     */
    private static class MappedFileEntry {
        private final RandomAccessFile file;
        private final FileChannel channel;
        private final MappedByteBuffer mappedBuffer;
        private final long size;
        
        public MappedFileEntry(RandomAccessFile file, FileChannel channel, 
                              MappedByteBuffer mappedBuffer, long size) {
            this.file = file;
            this.channel = channel;
            this.mappedBuffer = mappedBuffer;
            this.size = size;
        }
        
        /**
         * 切片指定区域
         */
        public MappedByteBuffer slice(long offset, int length) {
            if (offset < 0 || offset + length > size) {
                logger.warn("切片参数超出范围: offset={}, length={}, fileSize={}", 
                           offset, length, size);
                return null;
            }
            
            try {
                // 创建切片
                mappedBuffer.position((int) offset);
                mappedBuffer.limit((int) (offset + length));
                MappedByteBuffer slice = (MappedByteBuffer) mappedBuffer.slice();
                
                // 恢复原始position和limit
                mappedBuffer.clear();
                
                return slice;
            } catch (Exception e) {
                logger.error("创建映射切片失败: offset={}, length={}", offset, length, e);
                return null;
            }
        }
        
        public long getSize() {
            return size;
        }
        
        /**
         * 清理资源
         */
        public void cleanup() {
            try {
                if (channel != null) {
                    channel.close();
                }
                if (file != null) {
                    file.close();
                }
            } catch (IOException e) {
                logger.warn("清理映射文件资源失败", e);
            }
        }
    }
    
    /**
     * 映射统计信息
     */
    public static class MappingStatistics {
        public long totalMappedFiles;
        public long totalMappedSize;
        public long mappingHits;
        public long mappingMisses;
        public double hitRate;
        
        @Override
        public String toString() {
            return "MappedFile Statistics:\n" +
                   "  Total Mapped Files: " + totalMappedFiles + "\n" +
                   "  Total Mapped Size: " + (totalMappedSize / 1024 / 1024) + "MB\n" +
                   "  Mapping Hits: " + mappingHits + "\n" +
                   "  Mapping Misses: " + mappingMisses + "\n" +
                   "  Hit Rate: " + String.format("%.2f%%", hitRate * 100);
        }
    }
}
