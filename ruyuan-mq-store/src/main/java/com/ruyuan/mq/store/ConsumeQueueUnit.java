package com.ruyuan.mq.store;

import java.nio.ByteBuffer;

/**
 * ConsumeQueue单元
 * 
 * 每个单元固定20字节，包含：
 * - CommitLogOffset: 8字节，消息在CommitLog中的偏移量
 * - Size: 4字节，消息大小
 * - TagsHashCode: 8字节，Tags的哈希码
 * 
 * @author RuYuan MQ Team
 */
public class ConsumeQueueUnit {
    
    /**
     * 消息在CommitLog中的偏移量
     */
    private long commitLogOffset;
    
    /**
     * 消息大小
     */
    private int size;
    
    /**
     * Tags的哈希码
     */
    private long tagsHashCode;
    
    /**
     * 默认构造函数
     */
    public ConsumeQueueUnit() {
    }
    
    /**
     * 构造函数
     */
    public ConsumeQueueUnit(long commitLogOffset, int size, long tagsHashCode) {
        this.commitLogOffset = commitLogOffset;
        this.size = size;
        this.tagsHashCode = tagsHashCode;
    }
    
    /**
     * 序列化为字节数组
     */
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(StoreConstants.CONSUME_QUEUE_UNIT_SIZE);
        buffer.putLong(commitLogOffset);
        buffer.putInt(size);
        buffer.putLong(tagsHashCode);
        return buffer.array();
    }
    
    /**
     * 从字节数组反序列化
     */
    public static ConsumeQueueUnit deserialize(byte[] data) {
        if (data == null || data.length != StoreConstants.CONSUME_QUEUE_UNIT_SIZE) {
            return null;
        }
        
        ByteBuffer buffer = ByteBuffer.wrap(data);
        long commitLogOffset = buffer.getLong();
        int size = buffer.getInt();
        long tagsHashCode = buffer.getLong();
        
        return new ConsumeQueueUnit(commitLogOffset, size, tagsHashCode);
    }
    
    /**
     * 从ByteBuffer反序列化
     */
    public static ConsumeQueueUnit deserialize(ByteBuffer buffer) {
        if (buffer.remaining() < StoreConstants.CONSUME_QUEUE_UNIT_SIZE) {
            return null;
        }
        
        long commitLogOffset = buffer.getLong();
        int size = buffer.getInt();
        long tagsHashCode = buffer.getLong();
        
        return new ConsumeQueueUnit(commitLogOffset, size, tagsHashCode);
    }
    
    /**
     * 检查是否为有效单元
     */
    public boolean isValid() {
        return commitLogOffset >= 0 && size > 0;
    }
    
    /**
     * 检查Tags是否匹配
     */
    public boolean matchTags(String tags) {
        if (tags == null || tags.isEmpty()) {
            return true; // 空tags匹配所有消息
        }
        
        long targetHashCode = tags.hashCode() & 0xFFFFFFFFL;
        return this.tagsHashCode == targetHashCode;
    }
    
    // ========== Getter和Setter方法 ==========
    
    public long getCommitLogOffset() {
        return commitLogOffset;
    }
    
    public void setCommitLogOffset(long commitLogOffset) {
        this.commitLogOffset = commitLogOffset;
    }
    
    public int getSize() {
        return size;
    }
    
    public void setSize(int size) {
        this.size = size;
    }
    
    public long getTagsHashCode() {
        return tagsHashCode;
    }
    
    public void setTagsHashCode(long tagsHashCode) {
        this.tagsHashCode = tagsHashCode;
    }
    
    @Override
    public String toString() {
        return "ConsumeQueueUnit{" +
                "commitLogOffset=" + commitLogOffset +
                ", size=" + size +
                ", tagsHashCode=" + tagsHashCode +
                '}';
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        ConsumeQueueUnit that = (ConsumeQueueUnit) obj;
        return commitLogOffset == that.commitLogOffset &&
               size == that.size &&
               tagsHashCode == that.tagsHashCode;
    }
    
    @Override
    public int hashCode() {
        int result = Long.hashCode(commitLogOffset);
        result = 31 * result + size;
        result = 31 * result + Long.hashCode(tagsHashCode);
        return result;
    }
}
