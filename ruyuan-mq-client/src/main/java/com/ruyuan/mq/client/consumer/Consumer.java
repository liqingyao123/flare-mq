package com.ruyuan.mq.client.consumer;

/**
 * Consumer接口
 * 
 * 定义消息消费者的核心功能
 * 
 * @author RuYuan MQ Team
 */
public interface Consumer {
    
    /**
     * 启动Consumer
     */
    void start() throws Exception;
    
    /**
     * 关闭Consumer
     */
    void shutdown();
    
    /**
     * 订阅Topic
     * 
     * @param topic Topic名称
     * @param tags 标签过滤（支持*表示所有）
     * @param listener 消息监听器
     */
    void subscribe(String topic, String tags, MessageListener listener);
    
    /**
     * 取消订阅Topic
     * 
     * @param topic Topic名称
     */
    void unsubscribe(String topic);
    
    /**
     * 拉取消息（Pull模式）
     * 
     * @param topic Topic名称
     * @param queueId 队列ID
     * @param offset 偏移量
     * @param maxNums 最大消息数量
     * @return 拉取结果
     */
    PullResult pullMessage(String topic, int queueId, long offset, int maxNums) throws Exception;
    
    /**
     * 拉取消息（Pull模式，带超时）
     * 
     * @param topic Topic名称
     * @param queueId 队列ID
     * @param offset 偏移量
     * @param maxNums 最大消息数量
     * @param timeoutMs 超时时间（毫秒）
     * @return 拉取结果
     */
    PullResult pullMessage(String topic, int queueId, long offset, int maxNums, long timeoutMs) throws Exception;
    
    /**
     * 确认消息消费
     * 
     * @param messageId 消息ID
     * @return 确认结果
     */
    AckResult ackMessage(String messageId) throws Exception;
    
    /**
     * 批量确认消息消费
     * 
     * @param messageIds 消息ID列表
     * @return 确认结果
     */
    AckResult ackMessages(java.util.List<String> messageIds) throws Exception;
    
    /**
     * 获取Consumer状态
     */
    ConsumerStatus getStatus();
    
    /**
     * 获取Consumer配置
     */
    ConsumerConfig getConfig();
    
    /**
     * 获取Consumer统计信息
     */
    ConsumerStats getStats();
    
    /**
     * 获取订阅信息
     */
    java.util.Map<String, SubscriptionData> getSubscriptions();
}
