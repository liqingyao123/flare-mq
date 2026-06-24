package com.ruyuan.mq.nameserver.registry;

/**
 * 队列数据模型
 * 
 * @author RuYuan MQ Team
 */
public class QueueData {
    
    private String brokerName;
    private int readQueueNums;
    private int writeQueueNums;
    private int perm;
    private int topicSynFlag;
    
    public QueueData() {
    }
    
    public QueueData(String brokerName, int readQueueNums, int writeQueueNums, int perm) {
        this.brokerName = brokerName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
        this.perm = perm;
    }
    
    // Getters and Setters
    public String getBrokerName() {
        return brokerName;
    }
    
    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
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
    
    public int getTopicSynFlag() {
        return topicSynFlag;
    }
    
    public void setTopicSynFlag(int topicSynFlag) {
        this.topicSynFlag = topicSynFlag;
    }
    
    /**
     * 检查是否可读
     */
    public boolean isReadable() {
        return readQueueNums > 0 && (perm & 0x4) != 0;
    }
    
    /**
     * 检查是否可写
     */
    public boolean isWritable() {
        return writeQueueNums > 0 && (perm & 0x2) != 0;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        QueueData queueData = (QueueData) obj;
        return brokerName != null ? brokerName.equals(queueData.brokerName) : queueData.brokerName == null;
    }
    
    @Override
    public int hashCode() {
        return brokerName != null ? brokerName.hashCode() : 0;
    }
    
    @Override
    public String toString() {
        return "QueueData{" +
                "brokerName='" + brokerName + '\'' +
                ", readQueueNums=" + readQueueNums +
                ", writeQueueNums=" + writeQueueNums +
                ", perm=" + perm +
                ", topicSynFlag=" + topicSynFlag +
                '}';
    }
}
