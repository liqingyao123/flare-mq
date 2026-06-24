package com.ruyuan.mq.protocol;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 协议消息类
 * 
 * 协议格式：
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * | Length |  Type  | ReqId  |Status  |        Body (Variable Length)        |
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * |   4    |   2    |   4    |   2    |              Length - 12              |
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * 
 * @author RuYuan MQ Team
 */
public class ProtocolMessage {
    
    /**
     * 请求ID生成器
     */
    private static final AtomicInteger REQUEST_ID_GENERATOR = new AtomicInteger(1);
    
    /**
     * 消息总长度（包含头部）
     */
    private int length;
    
    /**
     * 消息类型
     */
    private MessageType type;
    
    /**
     * 请求ID，用于请求响应匹配
     */
    private int requestId;
    
    /**
     * 状态码
     */
    private ResponseCode status;
    
    /**
     * 消息体
     */
    private byte[] body;
    
    /**
     * 默认构造函数
     */
    public ProtocolMessage() {
    }
    
    /**
     * 构造请求消息
     */
    public ProtocolMessage(MessageType type, byte[] body) {
        this.type = type;
        this.requestId = generateRequestId();
        this.status = ResponseCode.SUCCESS;
        this.body = body;
        this.length = ProtocolConstants.HEADER_LENGTH + (body != null ? body.length : 0);
    }
    
    /**
     * 构造响应消息
     */
    public ProtocolMessage(MessageType type, int requestId, ResponseCode status, byte[] body) {
        this.type = type;
        this.requestId = requestId;
        this.status = status;
        this.body = body;
        this.length = ProtocolConstants.HEADER_LENGTH + (body != null ? body.length : 0);
    }
    
    /**
     * 生成请求ID
     */
    public static int generateRequestId() {
        return REQUEST_ID_GENERATOR.getAndIncrement();
    }
    
    /**
     * 创建心跳请求
     */
    public static ProtocolMessage createHeartbeatRequest() {
        return new ProtocolMessage(MessageType.HEARTBEAT_REQUEST, null);
    }
    
    /**
     * 创建心跳响应
     */
    public static ProtocolMessage createHeartbeatResponse(int requestId) {
        return new ProtocolMessage(MessageType.HEARTBEAT_RESPONSE, requestId, ResponseCode.SUCCESS, null);
    }
    
    /**
     * 创建成功响应
     */
    public static ProtocolMessage createSuccessResponse(MessageType responseType, int requestId, byte[] body) {
        return new ProtocolMessage(responseType, requestId, ResponseCode.SUCCESS, body);
    }
    
    /**
     * 创建错误响应
     */
    public static ProtocolMessage createErrorResponse(MessageType responseType, int requestId, ResponseCode errorCode) {
        return new ProtocolMessage(responseType, requestId, errorCode, null);
    }
    
    /**
     * 获取消息体长度
     */
    public int getBodyLength() {
        return body != null ? body.length : 0;
    }
    
    /**
     * 判断是否为请求消息
     */
    public boolean isRequest() {
        return type != null && type.isRequest();
    }
    
    /**
     * 判断是否为响应消息
     */
    public boolean isResponse() {
        return type != null && type.isResponse();
    }
    
    /**
     * 判断是否为心跳消息
     */
    public boolean isHeartbeat() {
        return type == MessageType.HEARTBEAT_REQUEST || type == MessageType.HEARTBEAT_RESPONSE;
    }
    
    /**
     * 判断响应是否成功
     */
    public boolean isSuccess() {
        return status != null && status.isSuccess();
    }
    
    // ========== Getter and Setter ==========
    
    public int getLength() {
        return length;
    }
    
    public void setLength(int length) {
        this.length = length;
    }
    
    public MessageType getType() {
        return type;
    }
    
    public void setType(MessageType type) {
        this.type = type;
    }
    
    public int getRequestId() {
        return requestId;
    }
    
    public void setRequestId(int requestId) {
        this.requestId = requestId;
    }
    
    public ResponseCode getStatus() {
        return status;
    }
    
    public void setStatus(ResponseCode status) {
        this.status = status;
    }
    
    public byte[] getBody() {
        return body;
    }
    
    public void setBody(byte[] body) {
        this.body = body;
        // 更新长度
        this.length = ProtocolConstants.HEADER_LENGTH + (body != null ? body.length : 0);
    }
    
    @Override
    public String toString() {
        return "ProtocolMessage{" +
                "length=" + length +
                ", type=" + type +
                ", requestId=" + requestId +
                ", status=" + status +
                ", bodyLength=" + getBodyLength() +
                '}';
    }
}
