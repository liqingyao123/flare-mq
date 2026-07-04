package com.ruyuan.mq.broker.topic;

import java.util.Objects;

/**
 * Topic配置信息
 * 
 * @author RuYuan MQ Team
 */
public class TopicConfig {
    
    /**
     * Topic名称
     */
    private String topicName;
    
    /**
     * 队列数量
     */
    private int queueCount;
    
    /**
     * 权限设置
     */
    private int permission;
    
    /**
     * 创建时间
     */
    private long createTime;
    
    /**
     * 最后更新时间
     */
    private long lastUpdateTime;
    
    /**
     * Topic描述
     */
    private String description;
    
    /**
     * 是否启用
     */
    private boolean enabled;
    
    /**
     * 消息保留时间（毫秒）
     */
    private long messageRetentionTime;
    
    /**
     * 最大消息大小（字节）
     */
    private int maxMessageSize;

    /**
     * 是否同步刷盘（默认false，异步刷盘）
     */
    private boolean syncFlush = false;
    
    /**
     * 默认构造函数
     */
    public TopicConfig() {
        this.createTime = System.currentTimeMillis();
        this.lastUpdateTime = this.createTime;
        this.enabled = true;
        this.messageRetentionTime = 72 * 60 * 60 * 1000L; // 默认72小时
        this.maxMessageSize = 4 * 1024 * 1024; // 默认4MB
    }
    
    /**
     * 构造函数
     */
    public TopicConfig(String topicName, int queueCount, int permission) {
        this();
        this.topicName = topicName;
        this.queueCount = queueCount;
        this.permission = permission;
    }
    
    /**
     * 完整构造函数
     */
    public TopicConfig(String topicName, int queueCount, int permission, 
                      String description, long messageRetentionTime, int maxMessageSize) {
        this(topicName, queueCount, permission);
        this.description = description;
        this.messageRetentionTime = messageRetentionTime;
        this.maxMessageSize = maxMessageSize;
    }
    
    // Getter和Setter方法
    
    public String getTopicName() {
        return topicName;
    }
    
    public void setTopicName(String topicName) {
        this.topicName = topicName;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public int getQueueCount() {
        return queueCount;
    }
    
    public void setQueueCount(int queueCount) {
        this.queueCount = queueCount;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public int getPermission() {
        return permission;
    }
    
    public void setPermission(int permission) {
        this.permission = permission;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public long getCreateTime() {
        return createTime;
    }
    
    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }
    
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }
    
    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }
    
    public String getDescription() {
        return description;
    }
    
    public void setDescription(String description) {
        this.description = description;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public long getMessageRetentionTime() {
        return messageRetentionTime;
    }
    
    public void setMessageRetentionTime(long messageRetentionTime) {
        this.messageRetentionTime = messageRetentionTime;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public int getMaxMessageSize() {
        return maxMessageSize;
    }
    
    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
        this.lastUpdateTime = System.currentTimeMillis();
    }

    public boolean isSyncFlush() {
        return syncFlush;
    }

    public void setSyncFlush(boolean syncFlush) {
        this.syncFlush = syncFlush;
        this.lastUpdateTime = System.currentTimeMillis();
    }

    /**
     * 检查是否可读
     */
    public boolean isReadable() {
        return (permission & TopicPermission.READ) != 0;
    }
    
    /**
     * 检查是否可写
     */
    public boolean isWritable() {
        return (permission & TopicPermission.WRITE) != 0;
    }
    
    /**
     * 获取权限描述
     */
    public String getPermissionDescription() {
        if (permission == TopicPermission.READ_WRITE) {
            return "READ_WRITE";
        } else if (permission == TopicPermission.READ_ONLY) {
            return "READ_ONLY";
        } else if (permission == TopicPermission.WRITE_ONLY) {
            return "WRITE_ONLY";
        } else {
            return "NONE";
        }
    }
    
    /**
     * 复制配置
     */
    public TopicConfig copy() {
        TopicConfig copy = new TopicConfig();
        copy.topicName = this.topicName;
        copy.queueCount = this.queueCount;
        copy.permission = this.permission;
        copy.createTime = this.createTime;
        copy.lastUpdateTime = this.lastUpdateTime;
        copy.description = this.description;
        copy.enabled = this.enabled;
        copy.messageRetentionTime = this.messageRetentionTime;
        copy.maxMessageSize = this.maxMessageSize;
        copy.syncFlush = this.syncFlush;
        return copy;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicConfig that = (TopicConfig) o;
        return Objects.equals(topicName, that.topicName);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(topicName);
    }
    
    @Override
    public String toString() {
        return "TopicConfig{" +
                "topicName='" + topicName + '\'' +
                ", queueCount=" + queueCount +
                ", permission=" + getPermissionDescription() +
                ", enabled=" + enabled +
                ", messageRetentionTime=" + messageRetentionTime +
                ", maxMessageSize=" + maxMessageSize +
                ", createTime=" + createTime +
                ", lastUpdateTime=" + lastUpdateTime +
                '}';
    }
}
