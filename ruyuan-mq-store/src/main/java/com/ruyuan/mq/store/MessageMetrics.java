package com.ruyuan.mq.store;

/**
 * 消息指标数据
 * 
 * @author RuYuan MQ Team
 */
public class MessageMetrics {
    
    /**
     * Topic名称
     */
    private String topic;
    
    /**
     * 队列ID
     */
    private int queueId;
    
    /**
     * 访问次数
     */
    private long accessCount;
    
    /**
     * 最后访问时间
     */
    private long lastAccessTime;
    
    /**
     * 创建时间
     */
    private long createTime;
    
    /**
     * 消息年龄（天）
     */
    private double ageDays;
    
    /**
     * 业务优先级 (1-10)
     */
    private int businessPriority;
    
    /**
     * 平均消息大小
     */
    private int avgMessageSize;
    
    /**
     * 总消息数量
     */
    private long totalMessages;
    
    /**
     * 构造函数
     */
    public MessageMetrics() {
        this.createTime = System.currentTimeMillis();
        this.lastAccessTime = this.createTime;
        this.businessPriority = 5; // 默认中等优先级
    }
    
    /**
     * 构造函数
     */
    public MessageMetrics(String topic, int queueId) {
        this();
        this.topic = topic;
        this.queueId = queueId;
    }
    
    /**
     * 更新访问统计
     */
    public void updateAccess() {
        this.accessCount++;
        this.lastAccessTime = System.currentTimeMillis();
        updateAgeDays();
    }
    
    /**
     * 更新消息统计
     */
    public void updateMessageStats(int messageSize) {
        this.totalMessages++;
        // 计算平均消息大小
        this.avgMessageSize = (int) ((this.avgMessageSize * (this.totalMessages - 1) + messageSize) / this.totalMessages);
    }
    
    /**
     * 更新年龄（天）
     */
    private void updateAgeDays() {
        long currentTime = System.currentTimeMillis();
        this.ageDays = (currentTime - this.createTime) / (24.0 * 60 * 60 * 1000);
    }
    
    /**
     * 获取访问频率（次/天）
     */
    public double getAccessFrequency() {
        updateAgeDays();
        if (ageDays <= 0) {
            return accessCount; // 当天创建的，返回访问次数
        }
        return accessCount / ageDays;
    }
    
    /**
     * 获取最近访问间隔（小时）
     */
    public double getLastAccessHours() {
        long currentTime = System.currentTimeMillis();
        return (currentTime - lastAccessTime) / (60.0 * 60 * 1000);
    }
    
    /**
     * 是否为热点Topic
     */
    public boolean isHotTopic() {
        // 简单判断：访问频率 > 100次/天 或 最近1小时内有访问
        return getAccessFrequency() > 100 || getLastAccessHours() < 1.0;
    }
    
    // ========== Getter和Setter方法 ==========
    
    public String getTopic() {
        return topic;
    }
    
    public void setTopic(String topic) {
        this.topic = topic;
    }
    
    public int getQueueId() {
        return queueId;
    }
    
    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }
    
    public long getAccessCount() {
        return accessCount;
    }
    
    public void setAccessCount(long accessCount) {
        this.accessCount = accessCount;
    }
    
    public long getLastAccessTime() {
        return lastAccessTime;
    }
    
    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }
    
    public long getCreateTime() {
        return createTime;
    }
    
    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }
    
    public double getAgeDays() {
        updateAgeDays();
        return ageDays;
    }
    
    public int getBusinessPriority() {
        return businessPriority;
    }
    
    public void setBusinessPriority(int businessPriority) {
        this.businessPriority = Math.max(1, Math.min(10, businessPriority)); // 限制在1-10范围内
    }
    
    public int getAvgMessageSize() {
        return avgMessageSize;
    }
    
    public void setAvgMessageSize(int avgMessageSize) {
        this.avgMessageSize = avgMessageSize;
    }
    
    public long getTotalMessages() {
        return totalMessages;
    }
    
    public void setTotalMessages(long totalMessages) {
        this.totalMessages = totalMessages;
    }
    
    @Override
    public String toString() {
        return "MessageMetrics{" +
                "topic='" + topic + '\'' +
                ", queueId=" + queueId +
                ", accessCount=" + accessCount +
                ", ageDays=" + String.format("%.2f", getAgeDays()) +
                ", businessPriority=" + businessPriority +
                ", avgMessageSize=" + avgMessageSize +
                ", totalMessages=" + totalMessages +
                ", accessFrequency=" + String.format("%.2f", getAccessFrequency()) +
                ", lastAccessHours=" + String.format("%.2f", getLastAccessHours()) +
                '}';
    }
}
