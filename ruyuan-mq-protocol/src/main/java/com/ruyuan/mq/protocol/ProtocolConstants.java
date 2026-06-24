package com.ruyuan.mq.protocol;

/**
 * 协议常量定义
 * 
 * @author RuYuan MQ Team
 */
public class ProtocolConstants {
    
    /**
     * 协议头长度：12字节
     * Length(4) + Type(2) + ReqId(4) + Status(2) = 12
     */
    public static final int HEADER_LENGTH = 12;
    
    /**
     * 魔数，用于识别协议
     */
    public static final int MAGIC_CODE = 0x12345678;
    
    /**
     * 协议版本
     */
    public static final byte PROTOCOL_VERSION = 1;
    
    /**
     * 最大消息体长度：16MB
     */
    public static final int MAX_BODY_LENGTH = 16 * 1024 * 1024;
    
    /**
     * 最小消息长度：只有协议头
     */
    public static final int MIN_MESSAGE_LENGTH = HEADER_LENGTH;
    
    /**
     * 心跳间隔：30秒
     */
    public static final int HEARTBEAT_INTERVAL = 30 * 1000;
    
    /**
     * 连接超时时间：60秒
     */
    public static final int CONNECTION_TIMEOUT = 60 * 1000;
    
    /**
     * 请求超时时间：10秒
     */
    public static final int REQUEST_TIMEOUT = 10 * 1000;
}
