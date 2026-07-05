package com.flare.mq.client.consumer;

import java.util.Set;
import java.util.HashSet;
import java.util.Objects;

/**
 * 订阅数据
 * 
 * @author FlareMQ Team
 */
public class SubscriptionData {
    
    /**
     * Topic名称
     */
    private String topic;
    
    /**
     * 标签表达式
     */
    private String subString;
    
    /**
     * 标签集合
     */
    private Set<String> tagsSet;
    
    /**
     * 消息监听器
     */
    private MessageListener messageListener;
    
    /**
     * 订阅时间
     */
    private long subscribeTime;
    
    /**
     * 最后更新时间
     */
    private long lastUpdateTime;
    
    /**
     * 是否启用
     */
    private boolean enabled;
    
    /**
     * 订阅版本号
     */
    private long version;
    
    /**
     * 默认构造函数
     */
    public SubscriptionData() {
        this.tagsSet = new HashSet<>();
        this.subscribeTime = System.currentTimeMillis();
        this.lastUpdateTime = this.subscribeTime;
        this.enabled = true;
        this.version = 1;
    }
    
    /**
     * 构造函数
     */
    public SubscriptionData(String topic, String subString, MessageListener messageListener) {
        this();
        this.topic = topic;
        this.subString = subString;
        this.messageListener = messageListener;
        this.tagsSet = parseTagsFromSubString(subString);
    }
    
    // Getter和Setter方法
    
    public String getTopic() {
        return topic;
    }
    
    public void setTopic(String topic) {
        this.topic = topic;
        this.lastUpdateTime = System.currentTimeMillis();
        this.version++;
    }
    
    public String getSubString() {
        return subString;
    }
    
    public void setSubString(String subString) {
        this.subString = subString;
        this.tagsSet = parseTagsFromSubString(subString);
        this.lastUpdateTime = System.currentTimeMillis();
        this.version++;
    }
    
    public Set<String> getTagsSet() {
        return new HashSet<>(tagsSet);
    }
    
    public void setTagsSet(Set<String> tagsSet) {
        this.tagsSet = tagsSet != null ? new HashSet<>(tagsSet) : new HashSet<>();
        this.lastUpdateTime = System.currentTimeMillis();
        this.version++;
    }
    
    public MessageListener getMessageListener() {
        return messageListener;
    }
    
    public void setMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
        this.lastUpdateTime = System.currentTimeMillis();
        this.version++;
    }
    
    public long getSubscribeTime() {
        return subscribeTime;
    }
    
    public void setSubscribeTime(long subscribeTime) {
        this.subscribeTime = subscribeTime;
    }
    
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }
    
    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        this.lastUpdateTime = System.currentTimeMillis();
        this.version++;
    }
    
    public long getVersion() {
        return version;
    }
    
    public void setVersion(long version) {
        this.version = version;
    }
    
    /**
     * 检查是否匹配指定的标签
     */
    public boolean matchTag(String tag) {
        if (tag == null) {
            return false;
        }
        
        // 如果订阅了所有标签（*），则匹配任何标签
        if (tagsSet.contains("*")) {
            return true;
        }
        
        // 检查是否包含指定标签
        return tagsSet.contains(tag);
    }
    
    /**
     * 检查订阅数据是否有效
     */
    public boolean isValid() {
        return topic != null && !topic.trim().isEmpty() &&
               subString != null && !subString.trim().isEmpty() &&
               messageListener != null &&
               enabled;
    }
    
    /**
     * 添加标签
     */
    public void addTag(String tag) {
        if (tag != null && !tag.trim().isEmpty()) {
            tagsSet.add(tag.trim());
            this.lastUpdateTime = System.currentTimeMillis();
            this.version++;
        }
    }
    
    /**
     * 移除标签
     */
    public void removeTag(String tag) {
        if (tag != null) {
            tagsSet.remove(tag.trim());
            this.lastUpdateTime = System.currentTimeMillis();
            this.version++;
        }
    }
    
    /**
     * 清空所有标签
     */
    public void clearTags() {
        tagsSet.clear();
        this.lastUpdateTime = System.currentTimeMillis();
        this.version++;
    }
    
    /**
     * 获取标签数量
     */
    public int getTagCount() {
        return tagsSet.size();
    }
    
    /**
     * 检查是否订阅了所有标签
     */
    public boolean isSubscribeAllTags() {
        return tagsSet.contains("*");
    }
    
    /**
     * 从订阅字符串解析标签集合
     */
    private Set<String> parseTagsFromSubString(String subString) {
        Set<String> tags = new HashSet<>();
        
        if (subString == null || subString.trim().isEmpty()) {
            tags.add("*"); // 默认订阅所有标签
            return tags;
        }
        
        // 支持多种分隔符：逗号、分号、竖线
        String[] tagArray = subString.split("[,;|]");
        for (String tag : tagArray) {
            String trimmedTag = tag.trim();
            if (!trimmedTag.isEmpty()) {
                tags.add(trimmedTag);
            }
        }
        
        // 如果没有解析到任何标签，默认订阅所有标签
        if (tags.isEmpty()) {
            tags.add("*");
        }
        
        return tags;
    }
    
    /**
     * 复制订阅数据
     */
    public SubscriptionData copy() {
        SubscriptionData copy = new SubscriptionData();
        copy.topic = this.topic;
        copy.subString = this.subString;
        copy.tagsSet = new HashSet<>(this.tagsSet);
        copy.messageListener = this.messageListener;
        copy.subscribeTime = this.subscribeTime;
        copy.lastUpdateTime = this.lastUpdateTime;
        copy.enabled = this.enabled;
        copy.version = this.version;
        return copy;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubscriptionData that = (SubscriptionData) o;
        return Objects.equals(topic, that.topic) && Objects.equals(subString, that.subString);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(topic, subString);
    }
    
    @Override
    public String toString() {
        return "SubscriptionData{" +
                "topic='" + topic + '\'' +
                ", subString='" + subString + '\'' +
                ", tagsSet=" + tagsSet +
                ", enabled=" + enabled +
                ", version=" + version +
                ", subscribeTime=" + subscribeTime +
                ", lastUpdateTime=" + lastUpdateTime +
                '}';
    }
}
