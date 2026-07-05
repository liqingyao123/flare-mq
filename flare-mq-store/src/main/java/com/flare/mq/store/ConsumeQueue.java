package com.flare.mq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * ConsumeQueue
 * 
 * 为特定Topic和QueueId维护的消息索引队列
 * 
 * @author FlareMQ Team
 */
public class ConsumeQueue {
    
    private static final Logger logger = LoggerFactory.getLogger(ConsumeQueue.class);
    
    /**
     * Topic名称
     */
    private final String topic;
    
    /**
     * 队列ID
     */
    private final int queueId;
    
    /**
     * 存储路径
     */
    private final String storePath;
    
    /**
     * ConsumeQueue目录路径
     */
    private final String consumeQueuePath;
    
    /**
     * MappedFile映射表，key为文件起始偏移量
     */
    private final ConcurrentSkipListMap<Long, MappedFile> mappedFiles = new ConcurrentSkipListMap<>();
    
    /**
     * 读写锁
     */
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    
    /**
     * 当前最大偏移量（逻辑偏移量，即消息数量）
     */
    private volatile long maxOffset = 0;
    
    /**
     * 构造函数
     */
    public ConsumeQueue(String topic, int queueId, String storePath) {
        this.topic = topic;
        this.queueId = queueId;
        this.storePath = storePath;
        this.consumeQueuePath = storePath + File.separator + StoreConstants.CONSUME_QUEUE_DIR + 
                               File.separator + topic + File.separator + queueId;
        
        // 创建目录
        File dir = new File(consumeQueuePath);
        if (!dir.exists()) {
            boolean created = dir.mkdirs();
            if (!created) {
                throw new RuntimeException("Failed to create consumequeue directory: " + consumeQueuePath);
            }
        }
        
        // 加载已存在的文件
        loadExistingFiles();
    }
    
    /**
     * 加载已存在的ConsumeQueue文件
     */
    private void loadExistingFiles() {
        File dir = new File(consumeQueuePath);
        File[] files = dir.listFiles();
        
        if (files != null) {
            for (File file : files) {
                if (file.isFile() && file.getName().length() == StoreConstants.FILE_NAME_LENGTH) {
                    try {
                        long fileFromOffset = Long.parseLong(file.getName());
                        MappedFile mappedFile = new MappedFile(file.getAbsolutePath(), 
                                                              StoreConstants.CONSUME_QUEUE_FILE_SIZE);
                        
                        this.mappedFiles.put(fileFromOffset, mappedFile);
                        
                        // 更新最大偏移量
                        long fileMaxOffset = fileFromOffset + mappedFile.getWrotePosition() / StoreConstants.CONSUME_QUEUE_UNIT_SIZE;
                        if (fileMaxOffset > maxOffset) {
                            maxOffset = fileMaxOffset;
                        }
                        
                        logger.info("加载ConsumeQueue文件: topic={}, queueId={}, file={}, wrotePosition={}", 
                                   topic, queueId, file.getName(), mappedFile.getWrotePosition());
                        
                    } catch (Exception e) {
                        logger.error("加载ConsumeQueue文件失败: " + file.getName(), e);
                    }
                }
            }
        }
        
        logger.info("ConsumeQueue加载完成: topic={}, queueId={}, maxOffset={}", topic, queueId, maxOffset);
    }
    
    /**
     * 添加消息索引
     */
    public boolean putMessageIndex(long commitLogOffset, int size, long tagsHashCode) {
        readWriteLock.writeLock().lock();
        try {
            // 创建ConsumeQueue单元
            ConsumeQueueUnit unit = new ConsumeQueueUnit(commitLogOffset, size, tagsHashCode);
            byte[] unitBytes = unit.serialize();
            
            // 获取当前可写的MappedFile
            MappedFile mappedFile = getLastMappedFile();
            if (mappedFile == null || mappedFile.getRemainSpace() < StoreConstants.CONSUME_QUEUE_UNIT_SIZE) {
                mappedFile = createNewMappedFile();
                if (mappedFile == null) {
                    logger.error("创建ConsumeQueue文件失败: topic={}, queueId={}", topic, queueId);
                    return false;
                }
            }
            
            // 写入索引单元
            boolean success = mappedFile.appendMessage(unitBytes);
            if (success) {
                maxOffset++;
                logger.debug("添加ConsumeQueue索引成功: topic={}, queueId={}, offset={}, commitLogOffset={}", 
                           topic, queueId, maxOffset - 1, commitLogOffset);
            } else {
                logger.error("写入ConsumeQueue索引失败: topic={}, queueId={}", topic, queueId);
            }
            
            return success;
            
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }
    
    /**
     * 根据逻辑偏移量获取ConsumeQueue单元
     */
    public ConsumeQueueUnit getConsumeQueueUnit(long offset) {
        if (offset < 0 || offset >= maxOffset) {
            return null;
        }
        
        readWriteLock.readLock().lock();
        try {
            // 计算在哪个文件中
            long fileIndex = offset / StoreConstants.CONSUME_QUEUE_MAX_RECORDS;
            long fileFromOffset = fileIndex * StoreConstants.CONSUME_QUEUE_MAX_RECORDS;
            
            MappedFile mappedFile = mappedFiles.get(fileFromOffset);
            if (mappedFile == null) {
                logger.warn("未找到ConsumeQueue文件: topic={}, queueId={}, offset={}, fileFromOffset={}", 
                           topic, queueId, offset, fileFromOffset);
                return null;
            }
            
            // 计算在文件中的位置
            int relativeOffset = (int) ((offset % StoreConstants.CONSUME_QUEUE_MAX_RECORDS) * StoreConstants.CONSUME_QUEUE_UNIT_SIZE);
            
            // 读取数据
            byte[] unitBytes = mappedFile.readBytes(relativeOffset, StoreConstants.CONSUME_QUEUE_UNIT_SIZE);
            if (unitBytes == null) {
                logger.warn("读取ConsumeQueue单元失败: topic={}, queueId={}, offset={}", topic, queueId, offset);
                return null;
            }
            
            return ConsumeQueueUnit.deserialize(unitBytes);
            
        } finally {
            readWriteLock.readLock().unlock();
        }
    }
    
    /**
     * 获取指定范围的ConsumeQueue单元
     */
    public List<ConsumeQueueUnit> getConsumeQueueUnits(long startOffset, int maxCount) {
        List<ConsumeQueueUnit> units = new ArrayList<>();
        
        if (startOffset < 0 || startOffset >= maxOffset || maxCount <= 0) {
            return units;
        }
        
        readWriteLock.readLock().lock();
        try {
            long currentOffset = startOffset;
            int count = 0;
            
            while (currentOffset < maxOffset && count < maxCount) {
                ConsumeQueueUnit unit = getConsumeQueueUnit(currentOffset);
                if (unit != null && unit.isValid()) {
                    units.add(unit);
                    count++;
                }
                currentOffset++;
            }
            
            return units;
            
        } finally {
            readWriteLock.readLock().unlock();
        }
    }
    
    /**
     * 根据Tags过滤获取ConsumeQueue单元
     */
    public List<ConsumeQueueUnit> getConsumeQueueUnitsByTags(long startOffset, int maxCount, String tags) {
        List<ConsumeQueueUnit> units = new ArrayList<>();
        
        if (startOffset < 0 || startOffset >= maxOffset || maxCount <= 0) {
            return units;
        }
        
        readWriteLock.readLock().lock();
        try {
            long currentOffset = startOffset;
            int count = 0;
            
            while (currentOffset < maxOffset && count < maxCount) {
                ConsumeQueueUnit unit = getConsumeQueueUnit(currentOffset);
                if (unit != null && unit.isValid() && unit.matchTags(tags)) {
                    units.add(unit);
                    count++;
                }
                currentOffset++;
            }
            
            return units;
            
        } finally {
            readWriteLock.readLock().unlock();
        }
    }
    
    /**
     * 获取最后一个MappedFile
     */
    private MappedFile getLastMappedFile() {
        if (mappedFiles.isEmpty()) {
            return null;
        }
        return mappedFiles.lastEntry().getValue();
    }
    
    /**
     * 创建新的MappedFile
     */
    private MappedFile createNewMappedFile() {
        // 计算新文件的起始偏移量
        long newFileFromOffset = 0;
        if (!mappedFiles.isEmpty()) {
            MappedFile lastFile = mappedFiles.lastEntry().getValue();
            newFileFromOffset = lastFile.getFileFromOffset() + StoreConstants.CONSUME_QUEUE_MAX_RECORDS;
        }
        
        // 生成文件名
        String fileName = String.format("%020d", newFileFromOffset);
        String filePath = consumeQueuePath + File.separator + fileName;
        
        try {
            MappedFile mappedFile = new MappedFile(filePath, StoreConstants.CONSUME_QUEUE_FILE_SIZE);
            mappedFiles.put(newFileFromOffset, mappedFile);
            
            logger.info("创建新的ConsumeQueue文件: topic={}, queueId={}, file={}", topic, queueId, fileName);
            return mappedFile;
            
        } catch (IOException e) {
            logger.error("创建ConsumeQueue文件失败: " + filePath, e);
            return null;
        }
    }
    
    /**
     * 刷盘
     */
    public void flush() {
        for (MappedFile mappedFile : mappedFiles.values()) {
            mappedFile.flush(0);
        }
    }
    
    /**
     * 清理资源
     */
    public void shutdown() {
        readWriteLock.writeLock().lock();
        try {
            for (MappedFile mappedFile : mappedFiles.values()) {
                mappedFile.cleanup();
            }
            mappedFiles.clear();
            
            logger.info("ConsumeQueue关闭完成: topic={}, queueId={}", topic, queueId);
            
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }
    
    // ========== Getter方法 ==========
    
    public String getTopic() {
        return topic;
    }
    
    public int getQueueId() {
        return queueId;
    }
    
    public String getStorePath() {
        return storePath;
    }
    
    public long getMaxOffset() {
        return maxOffset;
    }
    
    public int getMappedFileCount() {
        return mappedFiles.size();
    }
}
