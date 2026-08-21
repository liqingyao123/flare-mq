package com.flare.mq.protocol;

/**
 * 响应状态码枚举
 * 
 * @author FlareMQ Team
 */
public enum ResponseCode {
    
    // ========== 成功状态 ==========
    /**
     * 成功
     */
    SUCCESS((short) 0, "成功"),
    
    // ========== 客户端错误 4xx ==========
    /**
     * 请求参数错误
     */
    BAD_REQUEST((short) 400, "请求参数错误"),
    
    /**
     * 未授权
     */
    UNAUTHORIZED((short) 401, "未授权"),
    
    /**
     * 资源不存在
     */
    NOT_FOUND((short) 404, "资源不存在"),
    
    /**
     * 请求超时
     */
    REQUEST_TIMEOUT((short) 408, "请求超时"),
    
    /**
     * Topic不存在
     */
    TOPIC_NOT_EXIST((short) 410, "Topic不存在"),
    
    /**
     * 队列不存在
     */
    QUEUE_NOT_EXIST((short) 411, "队列不存在"),
    
    // ========== 服务器错误 5xx ==========
    /**
     * 服务器内部错误
     */
    INTERNAL_ERROR((short) 500, "服务器内部错误"),
    
    /**
     * 服务不可用
     */
    SERVICE_UNAVAILABLE((short) 503, "服务不可用"),
    
    /**
     * 存储错误
     */
    STORE_ERROR((short) 510, "存储错误"),
    
    /**
     * 网络错误
     */
    NETWORK_ERROR((short) 511, "网络错误"),
    
    /**
     * 序列化错误
     */
    SERIALIZE_ERROR((short) 512, "序列化错误"),
    
    /**
     * 反序列化错误
     */
    DESERIALIZE_ERROR((short) 513, "反序列化错误"),
    
    // ========== 业务错误 6xx ==========
    /**
     * 消息发送失败
     */
    SEND_MESSAGE_FAILED((short) 600, "消息发送失败"),
    
    /**
     * 消息拉取失败
     */
    PULL_MESSAGE_FAILED((short) 601, "消息拉取失败"),
    
    /**
     * 消息确认失败
     */
    ACK_MESSAGE_FAILED((short) 602, "消息确认失败"),
    
    /**
     * Topic创建失败
     */
    CREATE_TOPIC_FAILED((short) 603, "Topic创建失败"),

    // ========== 集群错误 6xx ==========
    /**
     * epoch 过时，拒绝注册（防双主栅栏）
     */
    STALE_EPOCH((short) 604, "epoch 过时");
    
    private final short code;
    private final String message;
    
    ResponseCode(short code, String message) {
        this.code = code;
        this.message = message;
    }
    
    public short getCode() {
        return code;
    }
    
    public String getMessage() {
        return message;
    }
    
    /**
     * 根据code获取ResponseCode
     */
    public static ResponseCode valueOf(short code) {
        for (ResponseCode responseCode : values()) {
            if (responseCode.code == code) {
                return responseCode;
            }
        }
        return INTERNAL_ERROR; // 默认返回内部错误
    }
    
    /**
     * 判断是否为成功状态
     */
    public boolean isSuccess() {
        return this == SUCCESS;
    }
    
    /**
     * 判断是否为客户端错误
     */
    public boolean isClientError() {
        return code >= 400 && code < 500;
    }
    
    /**
     * 判断是否为服务器错误
     */
    public boolean isServerError() {
        return code >= 500 && code < 600;
    }
    
    /**
     * 判断是否为业务错误
     */
    public boolean isBusinessError() {
        return code >= 600;
    }
}
