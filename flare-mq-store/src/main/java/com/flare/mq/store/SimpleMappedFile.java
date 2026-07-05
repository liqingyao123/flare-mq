package com.flare.mq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 简化的MappedFile实现
 *
 * 不使用内存映射，直接使用文件IO，避免内存映射可能导致的阻塞问题
 * 完全独立实现，不继承MappedFile
 *
 * @author FlareMQ Team
 */
public class SimpleMappedFile implements MappedFileInterface {
    
    private static final Logger logger = LoggerFactory.getLogger(SimpleMappedFile.class);

    /** 文件末尾保留8字节：4字节魔数 + 4字节实际写入位置 */
    private static final int FOOTER_SIZE = 8;
    private static final int FOOTER_MAGIC = 0x4D464D46;
    
    /**
     * 文件名
     */
    private final String fileName;
    
    /**
     * 文件大小
     */
    private final int fileSize;
    
    /**
     * 文件起始偏移量
     */
    private final long fileFromOffset;
    
    /**
     * 文件对象
     */
    private File file;
    
    /**
     * 随机访问文件
     */
    private RandomAccessFile randomAccessFile;
    
    /**
     * 当前写入位置
     */
    private final AtomicInteger wrotePosition = new AtomicInteger(0);
    
    /**
     * 当前提交位置
     */
    private final AtomicInteger committedPosition = new AtomicInteger(0);
    
    /**
     * 当前刷盘位置
     */
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    
    /**
     * 引用计数
     */
    private final AtomicLong refCount = new AtomicLong(1);
    
    /**
     * 是否可用
     */
    private volatile boolean available = true;
    
    /**
     * 最后修改时间
     */
    private volatile long lastModifiedTimestamp = System.currentTimeMillis();
    
    /**
     * 构造函数
     */
    public SimpleMappedFile(String fileName, int fileSize, long fileFromOffset) throws IOException {
        System.out.println("[DEBUG] SimpleMappedFile构造函数: fileName=" + fileName + ", fileSize=" + fileSize);
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.fileFromOffset = fileFromOffset;

        initSimple();
    }
    
    /**
     * 简化的初始化方法
     */
    private void initSimple() throws IOException {
        System.out.println("[DEBUG] SimpleMappedFile.initSimple开始");
        try {
            this.file = new File(fileName);
            
            // 确保父目录存在
            File parentDir = this.file.getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                boolean created = parentDir.mkdirs();
                if (!created) {
                    throw new IOException("Failed to create directory: " + parentDir.getAbsolutePath());
                }
            }
            
            // 创建随机访问文件
            this.randomAccessFile = new RandomAccessFile(this.file, "rw");
            
            // 检查文件是否已存在且有内容
            boolean fileExistsWithContent = this.file.exists() && this.file.length() > 0;
            long existingContentLength = fileExistsWithContent ? this.file.length() : 0;

            // 预分配文件空间
            if (!this.file.exists() || this.file.length() < fileSize) {
                this.randomAccessFile.setLength(fileSize);
            }

            // 从文件末尾恢复真實的写入位置（避免预分配空间被误判为有效数据）
            if (fileExistsWithContent) {
                int restoredPos = readFooterWrotePosition();
                if (restoredPos > 0) {
                    this.wrotePosition.set(restoredPos);
                    this.committedPosition.set(restoredPos);
                    this.flushedPosition.set(restoredPos);
                    System.out.println("[DEBUG] 从文件末尾恢复写入位置: " + restoredPos);
                } else {
                    // 无有效尾部标记（旧格式文件），尝试兼容：取文件长度但不超过有效数据区
                    long actualContentLength = Math.min(existingContentLength, fileSize - FOOTER_SIZE);
                    this.wrotePosition.set((int) actualContentLength);
                    this.committedPosition.set((int) actualContentLength);
                    this.flushedPosition.set((int) actualContentLength);
                    System.out.println("[DEBUG] 旧格式文件，设置写入位置为: " + actualContentLength);
                }
            } else {
                // 新文件，从0开始
                this.wrotePosition.set(0);
                this.committedPosition.set(0);
                this.flushedPosition.set(0);
                System.out.println("[DEBUG] 新文件，写入位置从0开始");
            }
            
            System.out.println("[DEBUG] SimpleMappedFile初始化成功: " + fileName);
            
        } catch (Exception e) {
            System.out.println("[DEBUG] SimpleMappedFile初始化失败: " + e.getMessage());
            throw e;
        }
    }
    
    /**
     * 追加数据
     */
    public synchronized boolean appendMessage(byte[] data) {
        return appendMessage(data, 0, data.length);
    }

    /**
     * 追加数据
     */
    public synchronized boolean appendMessage(byte[] data, int offset, int length) {
        System.out.println("[DEBUG] SimpleMappedFile.appendMessage: length=" + length);
        
        if (!available) {
            System.out.println("[DEBUG] SimpleMappedFile不可用");
            return false;
        }
        
        if (data == null || length <= 0) {
            System.out.println("[DEBUG] 无效的数据参数");
            return false;
        }
        
        int currentPos = this.wrotePosition.get();

        // 检查空间是否足够（预留尾部元数据区）
        if (currentPos + length > fileSize - FOOTER_SIZE) {
            System.out.println("[DEBUG] SimpleMappedFile空间不足: currentPos=" + currentPos + ", length=" + length + ", fileSize=" + fileSize);
            return false;
        }

        try {
            // 使用普通文件IO写入
            this.randomAccessFile.seek(currentPos);
            this.randomAccessFile.write(data, offset, length);

            // 更新写入位置
            this.wrotePosition.addAndGet(length);
            // 将实际写入位置持久化到文件末尾
            writeFooterWrotePosition(this.wrotePosition.get());
            this.lastModifiedTimestamp = System.currentTimeMillis();
            
            System.out.println("[DEBUG] SimpleMappedFile写入成功: position=" + currentPos + ", length=" + length);
            return true;
            
        } catch (Exception e) {
            System.out.println("[DEBUG] SimpleMappedFile写入失败: " + e.getMessage());
            logger.error("SimpleMappedFile写入数据失败", e);
            return false;
        }
    }
    
    /**
     * 读取数据
     */
    public byte[] readBytes(int position, int length) {
        if (!available) {
            return null;
        }

        if (position < 0 || position + length > wrotePosition.get()) {
            return null;
        }

        try {
            byte[] data = new byte[length];
            this.randomAccessFile.seek(position);
            this.randomAccessFile.readFully(data);
            return data;

        } catch (Exception e) {
            logger.error("SimpleMappedFile读取数据失败", e);
            return null;
        }
    }

    /**
     * 刷盘
     */
    public int flush(int flushLeastPages) {
        if (!available) {
            return this.flushedPosition.get();
        }

        try {
            if (this.randomAccessFile != null) {
                this.randomAccessFile.getFD().sync();
            }

            int wrotePos = this.wrotePosition.get();
            this.flushedPosition.set(wrotePos);
            this.committedPosition.set(wrotePos);

            return wrotePos;

        } catch (Exception e) {
            logger.error("SimpleMappedFile刷盘失败", e);
            return this.flushedPosition.get();
        }
    }

    /**
     * 清理资源
     */
    public void cleanup() {
        if (available) {
            available = false;

            try {
                if (this.randomAccessFile != null) {
                    this.randomAccessFile.close();
                }

                System.out.println("[DEBUG] SimpleMappedFile清理完成: " + fileName);

            } catch (IOException e) {
                logger.error("SimpleMappedFile清理失败", e);
            }
        }
    }

    /**
     * 从文件末尾读取持久化的 wrotePosition
     */
    private int readFooterWrotePosition() {
        try {
            int footerOffset = fileSize - FOOTER_SIZE;
            randomAccessFile.seek(footerOffset);
            int magic = randomAccessFile.readInt();
            if (magic == FOOTER_MAGIC) {
                return randomAccessFile.readInt();
            }
        } catch (Exception e) {
            logger.warn("读取文件尾部标记失败: {}", fileName, e);
        }
        return -1;
    }

    /**
     * 将 wrotePosition 写入文件末尾
     */
    private void writeFooterWrotePosition(int position) {
        try {
            int footerOffset = fileSize - FOOTER_SIZE;
            randomAccessFile.seek(footerOffset);
            randomAccessFile.writeInt(FOOTER_MAGIC);
            randomAccessFile.writeInt(position);
        } catch (Exception e) {
            logger.warn("写入文件尾部标记失败: {}", fileName, e);
        }
    }

    // ========== Getter方法 ==========

    public String getFileName() {
        return fileName;
    }

    public int getFileSize() {
        return fileSize;
    }

    public long getFileFromOffset() {
        return fileFromOffset;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public int getCommittedPosition() {
        return committedPosition.get();
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public boolean isAvailable() {
        return available;
    }

    public long getLastModifiedTimestamp() {
        return lastModifiedTimestamp;
    }

    public boolean isFull() {
        return this.wrotePosition.get() >= this.fileSize - FOOTER_SIZE;
    }

    public int getRemainSpace() {
        return this.fileSize - FOOTER_SIZE - this.wrotePosition.get();
    }
}
