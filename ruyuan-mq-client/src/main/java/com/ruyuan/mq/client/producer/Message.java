package com.ruyuan.mq.client.producer;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 消息类
 * 
 * @author RuYuan MQ Team
 */
public class Message {
    
    /**
     * 消息ID（系统生成）
     */
    private String messageId;
    
    /**
     * Topic名称
     */
    private String topic;
    
    /**
     * 消息标签
     */
    private String tags;
    
    /**
     * 消息Key（用于路由和查询）
     */
    private String key;

    /**
     * 队列ID
     */
    private int queueId;
    
    /**
     * 消息体
     */
    private byte[] body;
    
    /**
     * 消息属性
     */
    private Map<String, String> properties;
    
    /**
     * 消息优先级（1-10，数字越大优先级越高）
     */
    private int priority;
    
    /**
     * 延迟时间（毫秒）
     */
    private long delayTime;
    
    /**
     * 消息创建时间
     */
    private long createTime;
    
    /**
     * 是否为顺序消息
     */
    private boolean orderedMessage;
    
    /**
     * 是否为事务消息
     */
    private boolean transactionMessage;
    
    /**
     * 事务ID
     */
    private String transactionId;
    
    /**
     * 默认构造函数
     */
    public Message() {
        this.properties = new HashMap<>();
        this.priority = 5; // 默认优先级
        this.createTime = System.currentTimeMillis();
    }
    
    /**
     * 构造函数
     */
    public Message(String topic, String tags, byte[] body) {
        this();
        this.topic = topic;
        this.tags = tags;
        this.body = body;
    }
    
    /**
     * 构造函数
     */
    public Message(String topic, String tags, String key, byte[] body) {
        this(topic, tags, body);
        this.key = key;
    }
    
    // Getter和Setter方法
    
    public String getMessageId() {
        return messageId;
    }
    
    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }
    
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
    
    public String getKey() {
        return key;
    }
    
    public void setKey(String key) {
        this.key = key;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
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
        this.properties = properties != null ? properties : new HashMap<>();
    }
    
    public int getPriority() {
        return priority;
    }
    
    public void setPriority(int priority) {
        this.priority = Math.max(1, Math.min(10, priority)); // 限制在1-10之间
    }
    
    public long getDelayTime() {
        return delayTime;
    }
    
    public void setDelayTime(long delayTime) {
        this.delayTime = Math.max(0, delayTime);
    }
    
    public long getCreateTime() {
        return createTime;
    }
    
    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }
    
    public boolean isOrderedMessage() {
        return orderedMessage;
    }
    
    public void setOrderedMessage(boolean orderedMessage) {
        this.orderedMessage = orderedMessage;
    }
    
    public boolean isTransactionMessage() {
        return transactionMessage;
    }
    
    public void setTransactionMessage(boolean transactionMessage) {
        this.transactionMessage = transactionMessage;
    }
    
    public String getTransactionId() {
        return transactionId;
    }
    
    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
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
    
    /**
     * 获取消息大小
     */
    public int getMessageSize() {
        int size = 0;
        if (body != null) {
            size += body.length;
        }
        if (topic != null) {
            size += topic.getBytes().length;
        }
        if (tags != null) {
            size += tags.getBytes().length;
        }
        if (key != null) {
            size += key.getBytes().length;
        }
        if (properties != null) {
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                if (entry.getKey() != null) {
                    size += entry.getKey().getBytes().length;
                }
                if (entry.getValue() != null) {
                    size += entry.getValue().getBytes().length;
                }
            }
        }
        return size;
    }
    
    /**
     * 检查消息是否有效
     */
    public boolean isValid() {
        return topic != null && !topic.trim().isEmpty() && body != null;
    }
    
    /**
     * 复制消息
     */
    public Message copy() {
        Message copy = new Message();
        copy.messageId = this.messageId;
        copy.topic = this.topic;
        copy.tags = this.tags;
        copy.key = this.key;
        copy.queueId = this.queueId;
        copy.body = this.body != null ? this.body.clone() : null;
        copy.properties = this.properties != null ? new HashMap<>(this.properties) : new HashMap<>();
        copy.priority = this.priority;
        copy.delayTime = this.delayTime;
        copy.createTime = this.createTime;
        copy.orderedMessage = this.orderedMessage;
        copy.transactionMessage = this.transactionMessage;
        copy.transactionId = this.transactionId;
        return copy;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return Objects.equals(messageId, message.messageId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(messageId);
    }
    
    @Override
    public String toString() {
        return "Message{" +
                "messageId='" + messageId + '\'' +
                ", topic='" + topic + '\'' +
                ", tags='" + tags + '\'' +
                ", key='" + key + '\'' +
                ", bodySize=" + (body != null ? body.length : 0) +
                ", priority=" + priority +
                ", delayTime=" + delayTime +
                ", orderedMessage=" + orderedMessage +
                ", transactionMessage=" + transactionMessage +
                ", createTime=" + createTime +
                '}';
    }
}
