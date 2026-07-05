package com.flare.mq.protocol.client;

import com.flare.mq.protocol.ProtocolMessage;

/**
 * 响应回调接口
 * 
 * @author FlareMQ Team
 */
public interface ResponseCallback {
    
    /**
     * 响应成功时调用
     * 
     * @param response 响应消息
     */
    void onSuccess(ProtocolMessage response);
    
    /**
     * 响应失败时调用
     * 
     * @param cause 失败原因
     */
    void onFailure(Throwable cause);
    
    /**
     * 请求超时时调用
     */
    default void onTimeout() {
        onFailure(new RuntimeException("Request timeout"));
    }
}
