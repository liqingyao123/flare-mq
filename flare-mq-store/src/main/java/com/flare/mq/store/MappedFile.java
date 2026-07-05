package com.flare.mq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 内存映射文件
 *
 * 封装了文件的内存映射操作，提供高性能的文件读写
 *
 * @author FlareMQ Team
 */
public class MappedFile implements MappedFileInterface {
    
    private static final Logger logger = LoggerFactory.getLogger(MappedFile.class);

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
     * 文件通道
     */
    private FileChannel fileChannel;
    
    /**
     * 内存映射缓冲区
     */
    private MappedByteBuffer mappedByteBuffer;

    /**
     * 是否使用内存映射
     */
    private boolean useMmap = true;
    
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
     * 第一次创建时间
     */
    private long firstCreateTimestamp = System.currentTimeMillis();
    
    /**
     * 最后修改时间
     */
    private volatile long lastModifiedTimestamp = System.currentTimeMillis();
    
    /**
     * 构造函数
     */
    public MappedFile(String fileName, int fileSize) throws IOException {
        System.out.println("[DEBUG] MappedFile构造函数开始: fileName=" + fileName + ", fileSize=" + fileSize);
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.fileFromOffset = Long.parseLong(new File(fileName).getName());

        System.out.println("[DEBUG] MappedFile开始初始化: fileFromOffset=" + fileFromOffset);
        init();
        System.out.println("[DEBUG] MappedFile构造函数完成: " + fileName);
    }
    
    /**
     * 初始化文件和内存映射
     */
    private void init() throws IOException {
        System.out.println("[DEBUG] MappedFile.init()开始: " + fileName);
        try {
            this.file = new File(fileName);
            System.out.println("[DEBUG] 创建File对象: " + file.getAbsolutePath());

            // 确保父目录存在
            File parentDir = this.file.getParentFile();
            System.out.println("[DEBUG] 父目录: " + (parentDir != null ? parentDir.getAbsolutePath() : "null"));
            if (parentDir != null && !parentDir.exists()) {
                System.out.println("[DEBUG] 创建父目录...");
                boolean created = parentDir.mkdirs();
                System.out.println("[DEBUG] 父目录创建结果: " + created);
                if (!created) {
                    throw new IOException("Failed to create directory: " + parentDir.getAbsolutePath());
                }
            }

            // 创建随机访问文件
            System.out.println("[DEBUG] 创建RandomAccessFile...");
            this.randomAccessFile = new RandomAccessFile(this.file, "rw");
            System.out.println("[DEBUG] RandomAccessFile创建成功");
            this.fileChannel = this.randomAccessFile.getChannel();
            System.out.println("[DEBUG] FileChannel获取成功");

            // 检查文件是否已存在且有实际内容
            boolean fileExistsWithContent = this.file.exists() && this.file.length() > 0;
            long existingContentLength = fileExistsWithContent ? this.file.length() : 0;

            System.out.println("[DEBUG] 文件状态: exists=" + this.file.exists() +
                             ", length=" + this.file.length() +
                             ", hasContent=" + fileExistsWithContent +
                             ", contentLength=" + existingContentLength);

            // 预分配文件空间
            if (!this.file.exists() || this.file.length() < fileSize) {
                System.out.println("[DEBUG] 预分配文件空间...");
                this.randomAccessFile.setLength(fileSize);
                System.out.println("[DEBUG] 文件空间预分配完成: " + fileSize + " bytes");
            }

            // 创建内存映射，添加重试机制
            System.out.println("[DEBUG] 开始创建内存映射...");
            this.mappedByteBuffer = createMappedBuffer();
            System.out.println("[DEBUG] 内存映射创建成功");

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
                // 新文件或空文件，从0开始
                this.wrotePosition.set(0);
                this.committedPosition.set(0);
                this.flushedPosition.set(0);
                System.out.println("[DEBUG] 新文件或空文件，写入位置从0开始");
            }

            logger.info("初始化MappedFile成功: fileName={}, fileSize={}, wrotePosition={}",
                       fileName, fileSize, wrotePosition.get());

        } catch (FileNotFoundException e) {
            logger.error("文件未找到: " + fileName, e);
            throw e;
        } catch (IOException e) {
            logger.error("初始化MappedFile失败: " + fileName, e);
            throw e;
        }
    }

    /**
     * 创建内存映射缓冲区，带重试机制
     */
    private MappedByteBuffer createMappedBuffer() throws IOException {
        System.out.println("[DEBUG] createMappedBuffer开始: fileSize=" + fileSize);
        int retryCount = 3;
        IOException lastException = null;

        for (int i = 0; i < retryCount; i++) {
            try {
                System.out.println("[DEBUG] 尝试创建内存映射，第" + (i + 1) + "次...");
                // 尝试创建内存映射
                MappedByteBuffer buffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
                System.out.println("[DEBUG] 内存映射创建成功，buffer=" + buffer);

                // 预热内存映射（可选）
                if (fileSize <= StoreConstants.MMAP_WARMUP_SIZE) {
                    System.out.println("[DEBUG] 开始预热内存映射...");
                    warmupMappedBuffer(buffer);
                    System.out.println("[DEBUG] 内存映射预热完成");
                }

                System.out.println("[DEBUG] createMappedBuffer成功: fileName=" + fileName + ", attempt=" + (i + 1));
                return buffer;

            } catch (IOException e) {
                lastException = e;
                logger.warn("创建内存映射失败，尝试次数: {}, 错误: {}", i + 1, e.getMessage());

                // 等待一段时间后重试
                if (i < retryCount - 1) {
                    try {
                        Thread.sleep(100 * (i + 1)); // 递增等待时间
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("创建内存映射被中断", ie);
                    }
                }
            }
        }

        // 如果内存映射失败，尝试使用更小的文件大小
        System.out.println("[DEBUG] 内存映射失败，尝试使用更小的文件大小...");

        // 尝试使用更小的文件大小重新映射
        int smallerSize = Math.min(fileSize, 64 * 1024); // 64KB
        try {
            System.out.println("[DEBUG] 尝试映射更小的文件大小: " + smallerSize);
            // 先设置文件大小为较小值
            this.randomAccessFile.setLength(smallerSize);
            MappedByteBuffer buffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, smallerSize);
            System.out.println("[DEBUG] 小文件映射成功");
            useMmap = true;
            return buffer;
        } catch (Exception e2) {
            System.out.println("[DEBUG] 小文件映射也失败: " + e2.getMessage());
            useMmap = false;
            throw new IOException("创建内存映射失败，已尝试所有方案", lastException);
        }
    }

    /**
     * 预热内存映射缓冲区
     */
    private void warmupMappedBuffer(MappedByteBuffer buffer) {
        try {
            int pageSize = 4096; // 4KB页面大小
            for (int i = 0; i < fileSize; i += pageSize) {
                buffer.get(i); // 触发页面加载
            }
            logger.debug("内存映射预热完成: fileName={}", fileName);
        } catch (Exception e) {
            logger.warn("内存映射预热失败: fileName={}, 错误: {}", fileName, e.getMessage());
        }
    }
    
    /**
     * 追加数据
     */
    public boolean appendMessage(byte[] data) {
        return appendMessage(data, 0, data.length);
    }
    
    /**
     * 追加数据（线程安全）
     */
    public synchronized boolean appendMessage(byte[] data, int offset, int length) {
        if (!available) {
            logger.warn("MappedFile不可用: {}", fileName);
            return false;
        }

        if (data == null || length <= 0) {
            logger.warn("无效的数据参数: data={}, length={}", data, length);
            return false;
        }

        int currentPos = this.wrotePosition.get();

        // 检查空间是否足够（预留尾部元数据区）
        if (currentPos + length > fileSize - FOOTER_SIZE) {
            logger.warn("MappedFile空间不足: fileName={}, currentPos={}, length={}, fileSize={}, maxDataSize={}",
                       fileName, currentPos, length, fileSize, fileSize - FOOTER_SIZE);
            return false;
        }

        try {
            // 确保mappedByteBuffer可用
            if (this.mappedByteBuffer == null) {
                logger.error("MappedByteBuffer为空: {}", fileName);
                return false;
            }

            if (useMmap) {
                // 使用内存映射写入
                synchronized (this.mappedByteBuffer) {
                    this.mappedByteBuffer.position(currentPos);
                    this.mappedByteBuffer.put(data, offset, length);
                }
            } else {
                // 使用普通文件IO写入
                System.out.println("[DEBUG] 使用普通文件IO写入数据...");
                synchronized (this.randomAccessFile) {
                    this.randomAccessFile.seek(currentPos);
                    this.randomAccessFile.write(data, offset, length);
                }
            }

            // 更新写入位置
            this.wrotePosition.addAndGet(length);
            // 将实际写入位置持久化到文件末尾，避免重启后误读预分配空间
            writeFooterWrotePosition(this.wrotePosition.get());
            this.lastModifiedTimestamp = System.currentTimeMillis();

            logger.debug("写入数据成功: fileName={}, position={}, length={}, useMmap={}",
                        fileName, currentPos, length, useMmap);

            return true;

        } catch (Exception e) {
            logger.error("写入数据失败: fileName={}, position={}, length={}, useMmap={}",
                        fileName, currentPos, length, useMmap, e);
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
            logger.warn("读取位置超出范围: fileName={}, position={}, length={}, wrotePosition={}", 
                       fileName, position, length, wrotePosition.get());
            return null;
        }
        
        try {
            byte[] data = new byte[length];

            if (useMmap && this.mappedByteBuffer != null) {
                // 使用内存映射读取
                synchronized (this.mappedByteBuffer) {
                    this.mappedByteBuffer.position(position);
                    this.mappedByteBuffer.get(data);
                }
            } else {
                // 使用普通文件IO读取
                synchronized (this.randomAccessFile) {
                    this.randomAccessFile.seek(position);
                    this.randomAccessFile.readFully(data);
                }
            }

            return data;

        } catch (Exception e) {
            logger.error("读取数据失败: fileName={}, useMmap={}", fileName, useMmap, e);
            return null;
        }
    }
    
    /**
     * 提交数据到文件系统缓存
     */
    public int commit(int commitLeastPages) {
        if (!available) {
            return this.wrotePosition.get();
        }
        
        if (commitLeastPages > 0) {
            int writeSize = this.wrotePosition.get() - this.committedPosition.get();
            if (writeSize / (4 * 1024) < commitLeastPages) {
                return this.committedPosition.get();
            }
        }
        
        int writePos = this.wrotePosition.get();
        this.committedPosition.set(writePos);
        
        return writePos;
    }
    
    /**
     * 刷盘
     */
    public int flush(int flushLeastPages) {
        if (!available) {
            return this.flushedPosition.get();
        }
        
        if (flushLeastPages > 0) {
            int flushSize = this.committedPosition.get() - this.flushedPosition.get();
            if (flushSize / (4 * 1024) < flushLeastPages) {
                return this.flushedPosition.get();
            }
        }
        
        try {
            if (this.mappedByteBuffer != null) {
                this.mappedByteBuffer.force();
            }
            
            int committedPos = this.committedPosition.get();
            this.flushedPosition.set(committedPos);
            
            return committedPos;
            
        } catch (Exception e) {
            logger.error("刷盘失败: " + fileName, e);
            return this.flushedPosition.get();
        }
    }
    
    /**
     * 是否已满（需预留尾部8字节存储实际写入位置）
     */
    public boolean isFull() {
        return this.wrotePosition.get() >= this.fileSize - FOOTER_SIZE;
    }

    /**
     * 获取剩余空间（扣除尾部8字节元数据区）
     */
    public int getRemainSpace() {
        return this.fileSize - FOOTER_SIZE - this.wrotePosition.get();
    }
    
    /**
     * 增加引用计数
     */
    public boolean hold() {
        if (available) {
            refCount.incrementAndGet();
            return true;
        }
        return false;
    }
    
    /**
     * 减少引用计数
     */
    public void release() {
        long value = refCount.decrementAndGet();
        if (value <= 0) {
            cleanup();
        }
    }
    
    /**
     * 清理资源
     */
    public void cleanup() {
        if (available) {
            available = false;
            
            try {
                if (this.fileChannel != null) {
                    this.fileChannel.close();
                }
                
                if (this.randomAccessFile != null) {
                    this.randomAccessFile.close();
                }
                
                logger.info("清理MappedFile成功: {}", fileName);
                
            } catch (IOException e) {
                logger.error("清理MappedFile失败: " + fileName, e);
            }
        }
    }
    
    /**
     * 从文件末尾读取持久化的 wrotePosition
     * @return 有效的 wrotePosition，如果未找到标记则返回 -1
     */
    private int readFooterWrotePosition() {
        try {
            int footerOffset = fileSize - FOOTER_SIZE;
            if (useMmap && mappedByteBuffer != null) {
                int magic = mappedByteBuffer.getInt(footerOffset);
                if (magic == FOOTER_MAGIC) {
                    return mappedByteBuffer.getInt(footerOffset + 4);
                }
            } else if (randomAccessFile != null) {
                randomAccessFile.seek(footerOffset);
                int magic = randomAccessFile.readInt();
                if (magic == FOOTER_MAGIC) {
                    return randomAccessFile.readInt();
                }
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
            if (useMmap && mappedByteBuffer != null) {
                mappedByteBuffer.putInt(footerOffset, FOOTER_MAGIC);
                mappedByteBuffer.putInt(footerOffset + 4, position);
            } else if (randomAccessFile != null) {
                randomAccessFile.seek(footerOffset);
                randomAccessFile.writeInt(FOOTER_MAGIC);
                randomAccessFile.writeInt(position);
            }
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
    
    public long getFirstCreateTimestamp() {
        return firstCreateTimestamp;
    }
    
    public long getLastModifiedTimestamp() {
        return lastModifiedTimestamp;
    }
}
