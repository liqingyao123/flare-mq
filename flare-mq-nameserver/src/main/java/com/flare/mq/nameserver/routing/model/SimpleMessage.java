package com.flare.mq.nameserver.routing.model;

import java.util.HashMap;
import java.util.Map;

/**
 * 简单消息实现
 * 
 * 用于测试和演示的消息实现
 * 
 * @author FlareMQ Team
 */
public class SimpleMessage implements RoutableMessage {
    
    private String topic;
    private String tags;
    private String keys;
    private byte[] body;
    private Map<String, String> properties;
    
    public SimpleMessage() {
        this.properties = new HashMap<>();
    }
    
    public SimpleMessage(String topic, String tags, String keys, byte[] body) {
        this.topic = topic;
        this.tags = tags;
        this.keys = keys;
        this.body = body;
        this.properties = new HashMap<>();
    }
    
    @Override
    public String getTopic() {
        return topic;
    }
    
    @Override
    public String getTags() {
        return tags;
    }
    
    @Override
    public String getKeys() {
        return keys;
    }
    
    @Override
    public byte[] getBody() {
        return body;
    }
    
    @Override
    public Map<String, String> getProperties() {
        return properties;
    }
    
    // Setters
    public void setTopic(String topic) {
        this.topic = topic;
    }
    
    public void setTags(String tags) {
        this.tags = tags;
    }
    
    public void setKeys(String keys) {
        this.keys = keys;
    }
    
    public void setBody(byte[] body) {
        this.body = body;
    }
    
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
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
     * 设置为顺序消息
     */
    public void setOrdered(boolean ordered) {
        putProperty("ordered", String.valueOf(ordered));
    }
    
    /**
     * 设置为事务消息
     */
    public void setTransaction(boolean transaction) {
        putProperty("transaction", String.valueOf(transaction));
    }
    
    /**
     * 设置业务优先级
     */
    public void setBusinessPriority(int priority) {
        putProperty("priority", String.valueOf(priority));
    }
    
    /**
     * 设置消息类型
     */
    public void setMessageType(String messageType) {
        putProperty("messageType", messageType);
    }
    
    /**
     * 设置地理区域
     */
    public void setRegion(String region) {
        putProperty("region", region);
    }
    
    @Override
    public String toString() {
        return "SimpleMessage{" +
                "topic='" + topic + '\'' +
                ", tags='" + tags + '\'' +
                ", keys='" + keys + '\'' +
                ", bodySize=" + (body != null ? body.length : 0) +
                ", properties=" + properties +
                '}';
    }
}
