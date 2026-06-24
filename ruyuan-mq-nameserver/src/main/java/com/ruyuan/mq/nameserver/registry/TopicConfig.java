package com.ruyuan.mq.nameserver.registry;

/**
 * Topic配置模型
 * 
 * @author RuYuan MQ Team
 */
public class TopicConfig {
    
    public static final int SEPARATOR = 1;
    
    private String topicName;
    private int readQueueNums = 16;
    private int writeQueueNums = 16;
    private int perm = 6; // 读写权限
    private int topicFilterType = 0;
    private int topicSysFlag = 0;
    private boolean order = false;
    
    public TopicConfig() {
    }
    
    public TopicConfig(String topicName) {
        this.topicName = topicName;
    }
    
    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums, int perm) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
        this.perm = perm;
    }
    
    // Getters and Setters
    public String getTopicName() {
        return topicName;
    }
    
    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }
    
    public int getReadQueueNums() {
        return readQueueNums;
    }
    
    public void setReadQueueNums(int readQueueNums) {
        this.readQueueNums = readQueueNums;
    }
    
    public int getWriteQueueNums() {
        return writeQueueNums;
    }
    
    public void setWriteQueueNums(int writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }
    
    public int getPerm() {
        return perm;
    }
    
    public void setPerm(int perm) {
        this.perm = perm;
    }
    
    public int getTopicFilterType() {
        return topicFilterType;
    }
    
    public void setTopicFilterType(int topicFilterType) {
        this.topicFilterType = topicFilterType;
    }
    
    public int getTopicSysFlag() {
        return topicSysFlag;
    }
    
    public void setTopicSysFlag(int topicSysFlag) {
        this.topicSysFlag = topicSysFlag;
    }
    
    public boolean isOrder() {
        return order;
    }
    
    public void setOrder(boolean order) {
        this.order = order;
    }
    
    /**
     * 检查是否可读
     */
    public boolean isReadable() {
        return (perm & 0x4) != 0;
    }
    
    /**
     * 检查是否可写
     */
    public boolean isWritable() {
        return (perm & 0x2) != 0;
    }
    
    /**
     * 检查是否继承
     */
    public boolean isInherited() {
        return (perm & 0x1) != 0;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        TopicConfig that = (TopicConfig) obj;
        return topicName != null ? topicName.equals(that.topicName) : that.topicName == null;
    }
    
    @Override
    public int hashCode() {
        return topicName != null ? topicName.hashCode() : 0;
    }
    
    @Override
    public String toString() {
        return "TopicConfig{" +
                "topicName='" + topicName + '\'' +
                ", readQueueNums=" + readQueueNums +
                ", writeQueueNums=" + writeQueueNums +
                ", perm=" + perm +
                ", topicFilterType=" + topicFilterType +
                ", topicSysFlag=" + topicSysFlag +
                ", order=" + order +
                '}';
    }
}
