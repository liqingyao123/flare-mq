package com.ruyuan.mq.protocol.zerocopy;

/**
 * 消息位置信息
 * 
 * 用于描述消息在文件中的位置，支持零拷贝传输
 * 
 * @author RuYuan MQ Team
 */
public class MessageLocation {
    
    /**
     * 文件名
     */
    private final String fileName;
    
    /**
     * 文件偏移量
     */
    private final long offset;
    
    /**
     * 消息大小
     */
    private final int size;
    
    /**
     * 构造函数
     */
    public MessageLocation(String fileName, long offset, int size) {
        this.fileName = fileName;
        this.offset = offset;
        this.size = size;
    }
    
    /**
     * 获取文件名
     */
    public String getFileName() {
        return fileName;
    }
    
    /**
     * 获取偏移量
     */
    public long getOffset() {
        return offset;
    }
    
    /**
     * 获取大小
     */
    public int getSize() {
        return size;
    }
    
    @Override
    public String toString() {
        return "MessageLocation{" +
                "fileName='" + fileName + '\'' +
                ", offset=" + offset +
                ", size=" + size +
                '}';
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        MessageLocation that = (MessageLocation) o;
        
        if (offset != that.offset) return false;
        if (size != that.size) return false;
        return fileName != null ? fileName.equals(that.fileName) : that.fileName == null;
    }
    
    @Override
    public int hashCode() {
        int result = fileName != null ? fileName.hashCode() : 0;
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        result = 31 * result + size;
        return result;
    }
}
