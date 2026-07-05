package com.flare.mq.protocol.zerocopy;

import io.netty.channel.Channel;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.FileRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 零拷贝消息传输器
 * 
 * 实现端到端零拷贝传输，直接从文件映射区域传输到网络通道
 * 
 * @author FlareMQ Team
 */
public class ZeroCopyMessageTransfer {
    
    private static final Logger logger = LoggerFactory.getLogger(ZeroCopyMessageTransfer.class);
    
    /**
     * 文件映射管理器
     */
    private final MappedFileManager fileManager;
    
    /**
     * DirectBuffer池
     */
    private final DirectBufferPool bufferPool;
    
    /**
     * 文件通道缓存
     */
    private final ConcurrentHashMap<String, FileChannel> fileChannelCache;
    
    /**
     * 单例实例
     */
    private static volatile ZeroCopyMessageTransfer instance;
    
    /**
     * 私有构造函数
     */
    private ZeroCopyMessageTransfer() {
        this.fileManager = MappedFileManager.getInstance();
        this.bufferPool = DirectBufferPool.getInstance();
        this.fileChannelCache = new ConcurrentHashMap<>();
    }
    
    /**
     * 获取单例实例
     */
    public static ZeroCopyMessageTransfer getInstance() {
        if (instance == null) {
            synchronized (ZeroCopyMessageTransfer.class) {
                if (instance == null) {
                    instance = new ZeroCopyMessageTransfer();
                }
            }
        }
        return instance;
    }
    
    /**
     * 零拷贝传输消息
     */
    public void transferMessage(Channel channel, MessageLocation location) {
        if (channel == null || location == null) {
            logger.warn("无效的传输参数: channel={}, location={}", channel, location);
            return;
        }
        
        try {
            // 尝试使用文件映射零拷贝传输
            if (transferFromMappedFile(channel, location)) {
                logger.debug("使用文件映射零拷贝传输成功: {}", location);
                return;
            }
            
            // 降级到FileRegion零拷贝传输
            if (transferFromFileRegion(channel, location)) {
                logger.debug("使用FileRegion零拷贝传输成功: {}", location);
                return;
            }
            
            // 最后降级到普通Buffer传输
            transferFromBuffer(channel, location);
            logger.debug("使用Buffer传输成功: {}", location);
            
        } catch (Exception e) {
            logger.error("零拷贝传输失败: " + location, e);
            throw new RuntimeException("Zero copy transfer failed", e);
        }
    }
    
    /**
     * 使用文件映射进行零拷贝传输
     */
    private boolean transferFromMappedFile(Channel channel, MessageLocation location) {
        try {
            MappedByteBuffer mappedBuffer = fileManager.map(location);
            if (mappedBuffer != null) {
                // 直接从映射缓冲区传输到网络通道
                channel.write(mappedBuffer);
                return true;
            }
        } catch (Exception e) {
            logger.debug("文件映射传输失败: " + location, e);
        }
        return false;
    }
    
    /**
     * 使用FileRegion进行零拷贝传输
     */
    private boolean transferFromFileRegion(Channel channel, MessageLocation location) {
        try {
            FileChannel fileChannel = getFileChannel(location.getFileName());
            if (fileChannel != null) {
                FileRegion fileRegion = new DefaultFileRegion(
                    fileChannel, 
                    location.getOffset(), 
                    location.getSize()
                );
                channel.writeAndFlush(fileRegion);
                return true;
            }
        } catch (Exception e) {
            logger.debug("FileRegion传输失败: " + location, e);
        }
        return false;
    }
    
    /**
     * 使用Buffer进行传输（降级方案）
     */
    private void transferFromBuffer(Channel channel, MessageLocation location) throws IOException {
        ByteBuffer buffer = bufferPool.acquire(location.getSize());
        try {
            // 从文件读取数据到buffer
            FileChannel fileChannel = getFileChannel(location.getFileName());
            if (fileChannel != null) {
                buffer.clear();
                int bytesRead = fileChannel.read(buffer, location.getOffset());
                if (bytesRead > 0) {
                    buffer.flip();
                    channel.writeAndFlush(buffer);
                }
            }
        } finally {
            bufferPool.release(buffer);
        }
    }
    
    /**
     * 获取文件通道（带缓存）
     */
    private FileChannel getFileChannel(String fileName) {
        return fileChannelCache.computeIfAbsent(fileName, this::createFileChannel);
    }
    
    /**
     * 创建文件通道
     */
    private FileChannel createFileChannel(String fileName) {
        try {
            RandomAccessFile file = new RandomAccessFile(fileName, "r");
            return file.getChannel();
        } catch (Exception e) {
            logger.error("创建文件通道失败: " + fileName, e);
            return null;
        }
    }
    
    /**
     * 批量传输消息
     */
    public void transferMessages(Channel channel, MessageLocation[] locations) {
        if (channel == null || locations == null || locations.length == 0) {
            return;
        }
        
        // 计算总大小
        int totalSize = 0;
        for (MessageLocation location : locations) {
            totalSize += location.getSize();
        }
        
        // 如果总大小较小，使用单个Buffer批量传输
        if (totalSize <= 65536) { // 64KB以下使用批量传输
            transferBatch(channel, locations, totalSize);
        } else {
            // 大批量数据，逐个传输
            for (MessageLocation location : locations) {
                transferMessage(channel, location);
            }
        }
    }
    
    /**
     * 批量传输优化
     */
    private void transferBatch(Channel channel, MessageLocation[] locations, int totalSize) {
        ByteBuffer batchBuffer = bufferPool.acquire(totalSize);
        try {
            batchBuffer.clear();
            
            // 将所有消息读取到一个buffer中
            for (MessageLocation location : locations) {
                FileChannel fileChannel = getFileChannel(location.getFileName());
                if (fileChannel != null) {
                    int remaining = batchBuffer.remaining();
                    if (remaining >= location.getSize()) {
                        fileChannel.read(batchBuffer, location.getOffset());
                    }
                }
            }
            
            // 一次性发送
            batchBuffer.flip();
            channel.writeAndFlush(batchBuffer);
            
            logger.debug("批量传输完成: messages={}, totalSize={}", locations.length, totalSize);
            
        } catch (Exception e) {
            logger.error("批量传输失败", e);
        } finally {
            bufferPool.release(batchBuffer);
        }
    }
    
    /**
     * 清理资源
     */
    public void cleanup() {
        logger.info("开始清理ZeroCopyMessageTransfer资源...");
        
        // 关闭所有文件通道
        fileChannelCache.values().forEach(channel -> {
            try {
                channel.close();
            } catch (IOException e) {
                logger.warn("关闭文件通道失败", e);
            }
        });
        fileChannelCache.clear();
        
        // 清理文件映射管理器
        fileManager.cleanup();
        
        logger.info("ZeroCopyMessageTransfer资源清理完成");
    }
    
    /**
     * 获取传输统计信息
     */
    public TransferStatistics getStatistics() {
        TransferStatistics stats = new TransferStatistics();
        stats.bufferPoolStats = bufferPool.getStatistics();
        stats.fileMappingStats = fileManager.getStatistics();
        stats.cachedFileChannels = fileChannelCache.size();
        return stats;
    }
    
    /**
     * 传输统计信息
     */
    public static class TransferStatistics {
        public DirectBufferPool.PoolStatistics bufferPoolStats;
        public MappedFileManager.MappingStatistics fileMappingStats;
        public int cachedFileChannels;
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("ZeroCopy Transfer Statistics:\n");
            sb.append("  Cached File Channels: ").append(cachedFileChannels).append("\n");
            if (bufferPoolStats != null) {
                sb.append("  ").append(bufferPoolStats.toString().replace("\n", "\n  "));
            }
            if (fileMappingStats != null) {
                sb.append("  ").append(fileMappingStats.toString().replace("\n", "\n  "));
            }
            return sb.toString();
        }
    }
}
