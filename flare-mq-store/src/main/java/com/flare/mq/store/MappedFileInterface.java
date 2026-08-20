package com.flare.mq.store;

/**
 * MappedFile接口
 * 
 * 定义MappedFile的基本操作，让MappedFile和SimpleMappedFile都能实现
 * 
 * @author FlareMQ Team
 */
public interface MappedFileInterface {
    
    /**
     * 追加数据
     *
     * @return 数据在文件内的写入起始位置（成功）；-1（失败）
     */
    long appendMessage(byte[] data);

    /**
     * 追加数据
     *
     * @return 数据在文件内的写入起始位置（成功）；-1（失败）
     */
    long appendMessage(byte[] data, int offset, int length);
    
    /**
     * 读取数据
     */
    byte[] readBytes(int position, int length);
    
    /**
     * 刷盘
     */
    int flush(int flushLeastPages);
    
    /**
     * 清理资源
     */
    void cleanup();
    
    /**
     * 获取文件名
     */
    String getFileName();
    
    /**
     * 获取文件大小
     */
    int getFileSize();
    
    /**
     * 获取文件起始偏移量
     */
    long getFileFromOffset();
    
    /**
     * 获取当前写入位置
     */
    int getWrotePosition();
    
    /**
     * 获取当前提交位置
     */
    int getCommittedPosition();
    
    /**
     * 获取当前刷盘位置
     */
    int getFlushedPosition();
    
    /**
     * 是否可用
     */
    boolean isAvailable();
    
    /**
     * 获取最后修改时间
     */
    long getLastModifiedTimestamp();
    
    /**
     * 是否已满
     */
    boolean isFull();
    
    /**
     * 获取剩余空间
     */
    int getRemainSpace();
}
