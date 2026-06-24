package com.ruyuan.mq.store;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * 存储层消息对象
 * 
 * @author RuYuan MQ Team
 */
public class Message {
    
    /**
     * Topic名称
     */
    private String topic;
    
    /**
     * 标签
     */
    private String tags;
    
    /**
     * 消息Key
     */
    private String keys;
    
    /**
     * 队列ID
     */
    private int queueId;
    
    /**
     * 消息标志位
     */
    private int flag;
    
    /**
     * 消息体
     */
    private byte[] body;
    
    /**
     * 扩展属性
     */
    private Map<String, String> properties;
    
    /**
     * 生产时间戳
     */
    private long bornTimestamp;
    
    /**
     * 存储时间戳
     */
    private long storeTimestamp;
    
    /**
     * 消息在CommitLog中的物理偏移量
     */
    private long commitLogOffset;
    
    /**
     * 消息在CommitLog中的大小
     */
    private int storeSize;
    
    /**
     * 默认构造函数
     */
    public Message() {
        this.properties = new HashMap<>();
        this.bornTimestamp = System.currentTimeMillis();
    }
    
    /**
     * 构造函数
     */
    public Message(String topic, String tags, String keys, byte[] body) {
        this();
        this.topic = topic;
        this.tags = tags;
        this.keys = keys;
        this.body = body;
    }
    
    /**
     * 计算消息的存储大小
     */
    public int calculateStoreSize() {
        int size = StoreConstants.MESSAGE_MIN_SIZE; // 基础头部大小
        
        // Topic长度
        if (topic != null) {
            size += 2 + topic.getBytes(StandardCharsets.UTF_8).length; // 长度(2字节) + 内容
        } else {
            size += 2; // 空Topic的长度标识
        }
        
        // Tags长度
        if (tags != null) {
            size += 2 + tags.getBytes(StandardCharsets.UTF_8).length;
        } else {
            size += 2;
        }
        
        // Keys长度
        if (keys != null) {
            size += 2 + keys.getBytes(StandardCharsets.UTF_8).length;
        } else {
            size += 2;
        }
        
        // Body长度
        if (body != null) {
            size += body.length;
        }
        
        // Properties长度
        if (properties != null && !properties.isEmpty()) {
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (key != null) {
                    size += 2 + key.getBytes(StandardCharsets.UTF_8).length;
                }
                if (value != null) {
                    size += 2 + value.getBytes(StandardCharsets.UTF_8).length;
                }
            }
        }
        
        return size;
    }
    
    /**
     * 计算Tags的哈希码
     */
    public long getTagsHashCode() {
        if (tags == null || tags.isEmpty()) {
            return 0;
        }
        return tags.hashCode() & 0xFFFFFFFFL; // 转换为无符号32位整数
    }
    
    /**
     * 添加属性
     */
    public void putProperty(String key, String value) {
        if (properties == null) {
            properties = new HashMap<>();
        }
        properties.put(key, value);
    }
    
    /**
     * 获取属性
     */
    public String getProperty(String key) {
        return properties != null ? properties.get(key) : null;
    }
    
    // ========== Getter和Setter方法 ==========
    
    public String getTopic() {
        return topic;
    }
    
    public void setTopic(String topic) {
        this.topic = topic;
    }
    
    public String getTags() {
        return tags;
    }
    
    public void setTags(String tags) {
        this.tags = tags;
    }
    
    public String getKeys() {
        return keys;
    }
    
    public void setKeys(String keys) {
        this.keys = keys;
    }
    
    public int getQueueId() {
        return queueId;
    }
    
    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }
    
    public int getFlag() {
        return flag;
    }
    
    public void setFlag(int flag) {
        this.flag = flag;
    }
    
    public byte[] getBody() {
        return body;
    }
    
    public void setBody(byte[] body) {
        this.body = body;
    }
    
    public Map<String, String> getProperties() {
        return properties;
    }
    
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
    
    public long getBornTimestamp() {
        return bornTimestamp;
    }
    
    public void setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }
    
    public long getStoreTimestamp() {
        return storeTimestamp;
    }
    
    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }
    
    public long getCommitLogOffset() {
        return commitLogOffset;
    }
    
    public void setCommitLogOffset(long commitLogOffset) {
        this.commitLogOffset = commitLogOffset;
    }
    
    public int getStoreSize() {
        return storeSize;
    }
    
    public void setStoreSize(int storeSize) {
        this.storeSize = storeSize;
    }
    
    @Override
    public String toString() {
        return "Message{" +
                "topic='" + topic + '\'' +
                ", tags='" + tags + '\'' +
                ", keys='" + keys + '\'' +
                ", queueId=" + queueId +
                ", flag=" + flag +
                ", bodyLength=" + (body != null ? body.length : 0) +
                ", bornTimestamp=" + bornTimestamp +
                ", storeTimestamp=" + storeTimestamp +
                ", commitLogOffset=" + commitLogOffset +
                ", storeSize=" + storeSize +
                '}';
    }
}
