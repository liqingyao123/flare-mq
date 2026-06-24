package com.ruyuan.mq.nameserver.registry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Topic配置序列化包装器
 * 
 * @author RuYuan MQ Team
 */
public class TopicConfigSerializeWrapper {
    
    private Map<String, TopicConfig> topicConfigTable;
    
    public TopicConfigSerializeWrapper() {
        this.topicConfigTable = new ConcurrentHashMap<>();
    }
    
    // Getters and Setters
    public Map<String, TopicConfig> getTopicConfigTable() {
        return topicConfigTable;
    }
    
    public void setTopicConfigTable(Map<String, TopicConfig> topicConfigTable) {
        this.topicConfigTable = topicConfigTable;
    }
    
    /**
     * 添加Topic配置
     */
    public void addTopicConfig(String topic, TopicConfig config) {
        topicConfigTable.put(topic, config);
    }
    
    /**
     * 获取Topic配置
     */
    public TopicConfig getTopicConfig(String topic) {
        return topicConfigTable.get(topic);
    }
    
    /**
     * 移除Topic配置
     */
    public TopicConfig removeTopicConfig(String topic) {
        return topicConfigTable.remove(topic);
    }
    
    /**
     * 检查是否包含Topic
     */
    public boolean containsTopic(String topic) {
        return topicConfigTable.containsKey(topic);
    }
    
    /**
     * 获取Topic数量
     */
    public int getTopicCount() {
        return topicConfigTable.size();
    }
    
    @Override
    public String toString() {
        return "TopicConfigSerializeWrapper{" +
                "topicConfigTable=" + topicConfigTable.size() + " topics" +
                '}';
    }
}
