package com.flare.mq.client.producer;

/**
 * Producer接口
 * 
 * 定义消息生产者的核心功能
 * 
 * @author FlareMQ Team
 */
public interface Producer {
    
    /**
     * 启动Producer
     */
    void start() throws Exception;
    
    /**
     * 关闭Producer
     */
    void shutdown();
    
    /**
     * 同步发送消息
     * 
     * @param message 要发送的消息
     * @return 发送结果
     */
    SendResult send(Message message) throws Exception;
    
    /**
     * 同步发送消息（带超时）
     * 
     * @param message 要发送的消息
     * @param timeoutMs 超时时间（毫秒）
     * @return 发送结果
     */
    SendResult send(Message message, long timeoutMs) throws Exception;
    
    /**
     * 异步发送消息
     * 
     * @param message 要发送的消息
     * @param callback 发送回调
     */
    void sendAsync(Message message, SendCallback callback);
    
    /**
     * 异步发送消息（带超时）
     * 
     * @param message 要发送的消息
     * @param callback 发送回调
     * @param timeoutMs 超时时间（毫秒）
     */
    void sendAsync(Message message, SendCallback callback, long timeoutMs);
    
    /**
     * 单向发送消息（不关心结果）
     * 
     * @param message 要发送的消息
     */
    void sendOneway(Message message) throws Exception;
    
    /**
     * 获取Producer状态
     */
    ProducerStatus getStatus();
    
    /**
     * 获取Producer配置
     */
    ProducerConfig getConfig();
    
    /**
     * 获取Producer统计信息
     */
    ProducerStats getStats();
}
