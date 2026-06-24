package com.ruyuan.mq.nameserver.routing.model;

import java.util.Map;

/**
 * 可路由的消息接口
 * 
 * 为路由决策提供必要的消息信息
 * 
 * @author RuYuan MQ Team
 */
public interface RoutableMessage {
    
    /**
     * 获取Topic名称
     */
    String getTopic();
    
    /**
     * 获取消息标签
     */
    String getTags();
    
    /**
     * 获取消息Key
     */
    String getKeys();
    
    /**
     * 获取消息体
     */
    byte[] getBody();
    
    /**
     * 获取消息属性
     */
    Map<String, String> getProperties();
    
    /**
     * 获取消息大小
     */
    default int getMessageSize() {
        byte[] body = getBody();
        return body != null ? body.length : 0;
    }
    
    /**
     * 获取地理区域信息
     */
    default String getRegion() {
        Map<String, String> props = getProperties();
        return props != null ? props.get("region") : null;
    }
    
    /**
     * 获取业务优先级
     */
    default int getBusinessPriority() {
        Map<String, String> props = getProperties();
        if (props != null && props.containsKey("priority")) {
            try {
                return Integer.parseInt(props.get("priority"));
            } catch (NumberFormatException e) {
                // 忽略解析错误，返回默认优先级
            }
        }
        return 5; // 默认中等优先级
    }
    
    /**
     * 获取消息类型
     */
    default String getMessageType() {
        Map<String, String> props = getProperties();
        return props != null ? props.get("messageType") : "normal";
    }
    
    /**
     * 是否为顺序消息
     */
    default boolean isOrderedMessage() {
        Map<String, String> props = getProperties();
        return props != null && "true".equals(props.get("ordered"));
    }
    
    /**
     * 是否为事务消息
     */
    default boolean isTransactionMessage() {
        Map<String, String> props = getProperties();
        return props != null && "true".equals(props.get("transaction"));
    }
}
