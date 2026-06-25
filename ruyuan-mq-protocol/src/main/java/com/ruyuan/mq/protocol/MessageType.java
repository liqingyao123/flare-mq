package com.ruyuan.mq.protocol;

/**
 * 消息类型枚举
 * 
 * @author RuYuan MQ Team
 */
public enum MessageType {
    
    // ========== 基础通信 ==========
    /**
     * 请求消息
     */
    REQUEST((short) 1),
    
    /**
     * 响应消息
     */
    RESPONSE((short) 2),
    
    /**
     * 心跳请求
     */
    HEARTBEAT_REQUEST((short) 3),
    
    /**
     * 心跳响应
     */
    HEARTBEAT_RESPONSE((short) 4),
    
    // ========== 生产者相关 ==========
    /**
     * 发送消息请求
     */
    SEND_MESSAGE_REQUEST((short) 10),
    
    /**
     * 发送消息响应
     */
    SEND_MESSAGE_RESPONSE((short) 11),
    
    // ========== 消费者相关 ==========
    /**
     * 拉取消息请求
     */
    PULL_MESSAGE_REQUEST((short) 20),
    
    /**
     * 拉取消息响应
     */
    PULL_MESSAGE_RESPONSE((short) 21),
    
    /**
     * 消息确认请求
     */
    ACK_MESSAGE_REQUEST((short) 22),
    
    /**
     * 消息确认响应
     */
    ACK_MESSAGE_RESPONSE((short) 23),
    
    // ========== 管理相关 ==========
    /**
     * 创建Topic请求
     */
    CREATE_TOPIC_REQUEST((short) 30),

    /**
     * 创建Topic响应
     */
    CREATE_TOPIC_RESPONSE((short) 31),

    /**
     * 查询Topic请求
     */
    QUERY_TOPIC_REQUEST((short) 32),

    /**
     * 查询Topic响应
     */
    QUERY_TOPIC_RESPONSE((short) 33),

    /**
     * 获取路由信息请求
     */
    GET_ROUTEINFO_BY_TOPIC_REQUEST((short) 34),

    /**
     * 获取路由信息响应
     */
    GET_ROUTEINFO_BY_TOPIC_RESPONSE((short) 35),

    /**
     * Broker注册Topic路由请求
     */
    REGISTER_TOPIC_ROUTE_REQUEST((short) 36),

    /**
     * Broker注册Topic路由响应
     */
    REGISTER_TOPIC_ROUTE_RESPONSE((short) 37),

    /**
     * Broker注册请求
     */
    REGISTER_BROKER_REQUEST((short) 38),

    /**
     * Broker注册响应
     */
    REGISTER_BROKER_RESPONSE((short) 39),

    /**
     * 删除Topic请求
     */
    DELETE_TOPIC_REQUEST((short) 40),

    /**
     * 删除Topic响应
     */
    DELETE_TOPIC_RESPONSE((short) 41),

    /**
     * 列出所有Topic请求
     */
    LIST_TOPICS_REQUEST((short) 42),

    /**
     * 列出所有Topic响应
     */
    LIST_TOPICS_RESPONSE((short) 43);
    
    private final short code;
    
    MessageType(short code) {
        this.code = code;
    }
    
    public short getCode() {
        return code;
    }
    
    /**
     * 根据code获取MessageType
     */
    public static MessageType valueOf(short code) {
        for (MessageType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown message type code: " + code);
    }
    
    /**
     * 判断是否为请求类型
     */
    public boolean isRequest() {
        return this == REQUEST || this == HEARTBEAT_REQUEST ||
               this == SEND_MESSAGE_REQUEST || this == PULL_MESSAGE_REQUEST ||
               this == ACK_MESSAGE_REQUEST || this == CREATE_TOPIC_REQUEST ||
               this == QUERY_TOPIC_REQUEST || this == GET_ROUTEINFO_BY_TOPIC_REQUEST ||
               this == REGISTER_TOPIC_ROUTE_REQUEST || this == REGISTER_BROKER_REQUEST ||
               this == DELETE_TOPIC_REQUEST || this == LIST_TOPICS_REQUEST;
    }

    /**
     * 判断是否为响应类型
     */
    public boolean isResponse() {
        return !isRequest();
    }
}
