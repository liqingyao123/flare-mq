package com.flare.mq.protocol.client;

import com.flare.mq.protocol.ProtocolMessage;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 响应Future
 * 
 * @author FlareMQ Team
 */
public class ResponseFuture {
    
    private final int requestId;
    private final long timeoutMs;
    private final long createTime;
    private final CountDownLatch latch;
    private final ResponseCallback callback;
    
    private volatile ProtocolMessage response;
    private volatile Throwable cause;
    private volatile boolean done = false;
    
    public ResponseFuture(int requestId, long timeoutMs, ResponseCallback callback) {
        this.requestId = requestId;
        this.timeoutMs = timeoutMs;
        this.createTime = System.currentTimeMillis();
        this.latch = new CountDownLatch(1);
        this.callback = callback;
    }
    
    /**
     * 等待响应
     */
    public ProtocolMessage get() throws InterruptedException {
        latch.await();
        return response;
    }
    
    /**
     * 等待响应（带超时）
     */
    public ProtocolMessage get(long timeout, TimeUnit unit) throws InterruptedException {
        if (latch.await(timeout, unit)) {
            return response;
        } else {
            // 超时
            setFailure(new RuntimeException("Request timeout"));
            return null;
        }
    }
    
    /**
     * 设置响应成功
     */
    public void setSuccess(ProtocolMessage response) {
        if (done) {
            return;
        }
        
        this.response = response;
        this.done = true;
        latch.countDown();
        
        // 执行回调
        if (callback != null) {
            try {
                callback.onSuccess(response);
            } catch (Exception e) {
                // 忽略回调异常
            }
        }
    }
    
    /**
     * 设置响应失败
     */
    public void setFailure(Throwable cause) {
        if (done) {
            return;
        }
        
        this.cause = cause;
        this.done = true;
        latch.countDown();
        
        // 执行回调
        if (callback != null) {
            try {
                callback.onFailure(cause);
            } catch (Exception e) {
                // 忽略回调异常
            }
        }
    }
    
    /**
     * 检查是否超时
     */
    public boolean isTimeout() {
        return System.currentTimeMillis() - createTime > timeoutMs;
    }
    
    /**
     * 处理超时
     */
    public void handleTimeout() {
        if (done) {
            return;
        }
        
        this.done = true;
        latch.countDown();
        
        // 执行超时回调
        if (callback != null) {
            try {
                callback.onTimeout();
            } catch (Exception e) {
                // 忽略回调异常
            }
        }
    }
    
    /**
     * 检查是否完成
     */
    public boolean isDone() {
        return done;
    }
    
    /**
     * 获取请求ID
     */
    public int getRequestId() {
        return requestId;
    }
    
    /**
     * 获取超时时间
     */
    public long getTimeoutMs() {
        return timeoutMs;
    }
    
    /**
     * 获取创建时间
     */
    public long getCreateTime() {
        return createTime;
    }
    
    /**
     * 获取异常
     */
    public Throwable getCause() {
        return cause;
    }
}
