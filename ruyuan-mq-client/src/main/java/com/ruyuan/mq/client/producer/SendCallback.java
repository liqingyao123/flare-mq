package com.ruyuan.mq.client.producer;

/**
 * 发送回调接口
 * 
 * @author RuYuan MQ Team
 */
public interface SendCallback {
    
    /**
     * 发送成功回调
     * 
     * @param sendResult 发送结果
     */
    void onSuccess(SendResult sendResult);
    
    /**
     * 发送失败回调
     * 
     * @param exception 异常信息
     */
    void onException(Throwable exception);
}
