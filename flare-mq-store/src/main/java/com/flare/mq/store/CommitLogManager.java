package com.flare.mq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * CommitLog管理器
 * 
 * 负责管理CommitLog文件的创建、写入、读取等操作
 * 
 * @author FlareMQ Team
 */
public class CommitLogManager {
    
    private static final Logger logger = LoggerFactory.getLogger(CommitLogManager.class);
    
    /**
     * 存储路径
     */
    private final String storePath;
    
    /**
     * CommitLog目录
     */
    private final String commitLogPath;
    
    /**
     * MappedFile映射表，key为文件起始偏移量
     */
    private final ConcurrentSkipListMap<Long, MappedFileInterface> mappedFiles = new ConcurrentSkipListMap<>();
    
    /**
     * 读写锁
     */
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    
    /**
     * 当前写入的全局偏移量
     */
    private volatile long currentWriteOffset = 0;
    
    /**
     * 构造函数
     */
    public CommitLogManager(String storePath) {
        this.storePath = storePath;
        this.commitLogPath = storePath + File.separator + StoreConstants.COMMIT_LOG_DIR;
        
        // 创建目录
        File dir = new File(commitLogPath);
        if (!dir.exists()) {
            boolean created = dir.mkdirs();
            if (!created) {
                throw new RuntimeException("Failed to create commitlog directory: " + commitLogPath);
            }
        }
        
        // 加载已存在的文件
        loadExistingFiles();
    }
    
    /**
     * 加载已存在的CommitLog文件
     */
    private void loadExistingFiles() {
        File dir = new File(commitLogPath);
        File[] files = dir.listFiles();
        
        if (files != null) {
            for (File file : files) {
                if (file.isFile() && file.getName().length() == StoreConstants.FILE_NAME_LENGTH) {
                    try {
                        long fileFromOffset = Long.parseLong(file.getName());
                        MappedFile mappedFile = new MappedFile(file.getAbsolutePath(), 
                                                              StoreConstants.COMMIT_LOG_FILE_SIZE);
                        
                        this.mappedFiles.put(fileFromOffset, mappedFile);
                        
                        // 更新当前写入偏移量
                        long fileEndOffset = fileFromOffset + mappedFile.getWrotePosition();
                        if (fileEndOffset > currentWriteOffset) {
                            currentWriteOffset = fileEndOffset;
                        }
                        
                        logger.info("加载CommitLog文件: {}, wrotePosition={}", 
                                   file.getName(), mappedFile.getWrotePosition());
                        
                    } catch (Exception e) {
                        logger.error("加载CommitLog文件失败: " + file.getName(), e);
                    }
                }
            }
        }
        
        logger.info("CommitLog加载完成，当前写入偏移量: {}", currentWriteOffset);
    }
    
    /**
     * 追加消息到CommitLog
     */
    public AppendMessageResult appendMessage(Message message) {
        System.out.println("[DEBUG] CommitLogManager.appendMessage开始: topic=" + message.getTopic() + ", queueId=" + message.getQueueId());

        // 序列化消息（不需要锁）
        System.out.println("[DEBUG] 开始序列化消息...");
        byte[] messageBytes = MessageSerializer.serialize(message);
        if (messageBytes == null) {
            System.out.println("[DEBUG] 消息序列化失败");
            return new AppendMessageResult(AppendMessageStatus.SERIALIZE_ERROR, 0, 0);
        }
        System.out.println("[DEBUG] 消息序列化成功，大小: " + messageBytes.length + " bytes");

        // 获取或创建MappedFile
        MappedFileInterface mappedFile = getOrCreateMappedFile(messageBytes.length);
        if (mappedFile == null) {
            System.out.println("[DEBUG] 获取或创建MappedFile失败");
            return new AppendMessageResult(AppendMessageStatus.CREATE_FILE_ERROR, 0, 0);
        }

        // 最多尝试两次：先写当前文件，若因并发导致空间不足则新建文件重试一次
        for (int attempt = 0; attempt < 2; attempt++) {
            long relativeOffset;
            readWriteLock.readLock().lock();
            try {
                // appendMessage 在 synchronized 内部计算实际写入位置并返回，
                // 避免在锁外读 wrotePosition 计算 offset 造成并发错位
                relativeOffset = mappedFile.appendMessage(messageBytes);
            } finally {
                readWriteLock.readLock().unlock();
            }

            if (relativeOffset >= 0) {
                long msgOffset = mappedFile.getFileFromOffset() + relativeOffset;

                // 更新消息的存储信息
                message.setCommitLogOffset(msgOffset);
                message.setStoreSize(messageBytes.length);

                // 更新全局写入偏移量
                currentWriteOffset = msgOffset + messageBytes.length;

                logger.debug("消息追加成功: topic={}, queueId={}, offset={}, size={}",
                            message.getTopic(), message.getQueueId(), msgOffset, messageBytes.length);

                return new AppendMessageResult(AppendMessageStatus.SUCCESS, msgOffset, messageBytes.length);
            }

            // 写入失败：通常是文件空间被其他线程并发占满。
            // 重新获取一个可写文件（复用其他线程刚创建的，或新建），避免各自新建文件
            System.out.println("[DEBUG] 当前文件写入失败，重新获取可写文件重试...");
            mappedFile = getOrCreateMappedFile(messageBytes.length);
            if (mappedFile == null) {
                System.out.println("[DEBUG] 获取可写文件失败");
                return new AppendMessageResult(AppendMessageStatus.CREATE_FILE_ERROR, 0, 0);
            }
        }

        return new AppendMessageResult(AppendMessageStatus.APPEND_ERROR, 0, 0);
    }
    
    /**
     * 根据偏移量读取消息
     */
    public Message getMessage(long offset, int size) {
        readWriteLock.readLock().lock();
        try {
            // 找到对应的MappedFile
            MappedFileInterface mappedFile = findMappedFileByOffset(offset);
            if (mappedFile == null) {
                logger.warn("未找到对应的MappedFile: offset={}", offset);
                return null;
            }
            
            // 计算在文件中的相对位置
            int relativePos = (int) (offset - mappedFile.getFileFromOffset());
            
            // 读取消息数据
            byte[] messageBytes = mappedFile.readBytes(relativePos, size);
            if (messageBytes == null) {
                logger.warn("读取消息数据失败: offset={}, size={}", offset, size);
                return null;
            }
            
            // 反序列化消息
            Message message = MessageSerializer.deserialize(messageBytes);
            if (message != null) {
                message.setCommitLogOffset(offset);
            }
            
            return message;
            
        } finally {
            readWriteLock.readLock().unlock();
        }
    }
    
    /**
     * 获取最后一个MappedFile
     */
    private MappedFileInterface getLastMappedFile() {
        if (mappedFiles.isEmpty()) {
            return null;
        }
        return mappedFiles.lastEntry().getValue();
    }

    /**
     * 获取或创建MappedFile
     *
     * 冷启动/文件滚动时会有多个线程同时发现"没有可写文件"。
     * 快速路径用读锁直接复用现有文件；未命中时升级到写锁【复查】一次，
     * 若其他线程已建好可写文件则直接复用，避免每个线程各自新建文件（文件风暴）。
     */
    private MappedFileInterface getOrCreateMappedFile(int requiredSize) {
        System.out.println("[DEBUG] getOrCreateMappedFile开始，需要大小: " + requiredSize);

        // 快速路径：读锁下直接复用现有可写文件
        readWriteLock.readLock().lock();
        try {
            MappedFileInterface mappedFile = getWritableMappedFile(requiredSize);
            if (mappedFile != null) {
                System.out.println("[DEBUG] 使用现有MappedFile: " + mappedFile.getFileName());
                return mappedFile;
            }
        } finally {
            readWriteLock.readLock().unlock();
        }

        // 慢速路径：写锁下复查，确实没有可写文件才创建。
        // 写锁互斥，多个并发线程只有一个能真正走到创建，其余在复查时复用新文件
        readWriteLock.writeLock().lock();
        try {
            MappedFileInterface mappedFile = getWritableMappedFile(requiredSize);
            if (mappedFile != null) {
                System.out.println("[DEBUG] 其他线程已创建可写MappedFile，直接复用: " + mappedFile.getFileName());
                return mappedFile;
            }
            return createNewMappedFileInternal();
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * 返回最后一个可写文件（未满且剩余空间足够容纳 requiredSize）；否则返回 null。
     * 调用方必须已持有读锁或写锁。
     */
    private MappedFileInterface getWritableMappedFile(int requiredSize) {
        MappedFileInterface mappedFile = getLastMappedFile();
        if (mappedFile != null && !mappedFile.isFull() && mappedFile.getRemainSpace() >= requiredSize) {
            return mappedFile;
        }
        return null;
    }

    /**
     * 创建新的CommitLog文件（调用方必须已持有写锁）
     *
     * 优先创建基于 mmap 的 MappedFile；mmap 失败时降级为 SimpleMappedFile，
     * 保证服务不中断。
     */
    private MappedFileInterface createNewMappedFileInternal() {
        System.out.println("[DEBUG] createNewMappedFileInternal开始...");

        // 计算新文件的起始偏移量
        long newFileFromOffset = 0;
        if (!mappedFiles.isEmpty()) {
            MappedFileInterface lastFile = mappedFiles.lastEntry().getValue();
            newFileFromOffset = lastFile.getFileFromOffset() + StoreConstants.COMMIT_LOG_FILE_SIZE;
        }
        System.out.println("[DEBUG] 新文件偏移量: " + newFileFromOffset);

        // 生成文件名（20位数字，不足前面补0）
        String fileName = String.format("%020d", newFileFromOffset);
        String filePath = commitLogPath + File.separator + fileName;
        System.out.println("[DEBUG] 新文件路径: " + filePath);
        System.out.println("[DEBUG] 文件大小: " + StoreConstants.COMMIT_LOG_FILE_SIZE);

        try {
            System.out.println("[DEBUG] 开始创建MappedFile...");
            MappedFile mappedFile = new MappedFile(filePath, StoreConstants.COMMIT_LOG_FILE_SIZE);
            System.out.println("[DEBUG] MappedFile创建成功，添加到映射表...");
            mappedFiles.put(newFileFromOffset, mappedFile);

            System.out.println("[DEBUG] 创建新的CommitLog文件成功: " + fileName);
            logger.info("创建新的CommitLog文件: {}", fileName);
            return mappedFile;

        } catch (IOException e) {
            System.out.println("[DEBUG] 创建MappedFile异常，降级为SimpleMappedFile: " + e.getMessage());
            logger.error("创建MappedFile失败，降级为SimpleMappedFile: " + filePath, e);
            return createSimpleMappedFile(filePath, newFileFromOffset);
        }
    }

    /**
     * 降级创建基于 RandomAccessFile 的 SimpleMappedFile
     */
    private MappedFileInterface createSimpleMappedFile(String filePath, long newFileFromOffset) {
        try {
            int fallbackSize = 64 * 1024; // 64KB
            System.out.println("[DEBUG] 降级策略：创建SimpleMappedFile，大小: " + fallbackSize);

            SimpleMappedFile simpleMappedFile = new SimpleMappedFile(filePath, fallbackSize, newFileFromOffset);
            mappedFiles.put(newFileFromOffset, simpleMappedFile);
            return simpleMappedFile;

        } catch (Exception e) {
            System.out.println("[DEBUG] 降级创建SimpleMappedFile失败: " + e.getMessage());
            e.printStackTrace();
            logger.error("降级创建SimpleMappedFile失败: " + filePath, e);
            return null;
        }
    }
    
    /**
     * 根据偏移量查找MappedFile
     */
    private MappedFileInterface findMappedFileByOffset(long offset) {
        // 找到小于等于offset的最大key
        Long fileFromOffset = mappedFiles.floorKey(offset);
        if (fileFromOffset != null) {
            MappedFileInterface mappedFile = mappedFiles.get(fileFromOffset);
            if (mappedFile != null &&
                offset >= mappedFile.getFileFromOffset() &&
                offset < mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) {
                return mappedFile;
            }
        }
        return null;
    }
    
    /**
     * 刷盘
     */
    public void flush() {
        for (MappedFileInterface mappedFile : mappedFiles.values()) {
            mappedFile.flush(0);
        }
    }
    
    /**
     * 获取所有MappedFile
     */
    public List<MappedFileInterface> getAllMappedFiles() {
        readWriteLock.readLock().lock();
        try {
            return new ArrayList<>(mappedFiles.values());
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    /**
     * 清理资源
     */
    public void shutdown() {
        readWriteLock.writeLock().lock();
        try {
            for (MappedFileInterface mappedFile : mappedFiles.values()) {
                mappedFile.cleanup();
            }
            mappedFiles.clear();
            
            logger.info("CommitLogManager关闭完成");
            
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }
    
    // ========== Getter方法 ==========
    
    public String getStorePath() {
        return storePath;
    }
    
    public String getCommitLogPath() {
        return commitLogPath;
    }
    
    public long getCurrentWriteOffset() {
        return currentWriteOffset;
    }
    
    public int getMappedFileCount() {
        return mappedFiles.size();
    }
}
