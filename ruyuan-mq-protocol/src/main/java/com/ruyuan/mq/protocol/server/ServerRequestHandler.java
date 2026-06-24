package com.ruyuan.mq.protocol.server;

import com.ruyuan.mq.protocol.ProtocolMessage;
import io.netty.channel.ChannelHandlerContext;

/**
 * 服务器请求处理器接口
 * 
 * @author RuYuan MQ Team
 */
public interface ServerRequestHandler {
    
    /**
     * 处理客户端请求
     * 
     * @param ctx 通道上下文
     * @param request 请求消息
     * @return 响应消息，如果返回null则不发送响应
     */
    ProtocolMessage handleRequest(ChannelHandlerContext ctx, ProtocolMessage request);
    
    /**
     * 客户端连接建立时调用
     * 
     * @param ctx 通道上下文
     */
    default void onChannelActive(ChannelHandlerContext ctx) {
        // 默认实现为空
    }
    
    /**
     * 客户端连接断开时调用
     * 
     * @param ctx 通道上下文
     */
    default void onChannelInactive(ChannelHandlerContext ctx) {
        // 默认实现为空
    }
    
    /**
     * 发生异常时调用
     * 
     * @param ctx 通道上下文
     * @param cause 异常原因
     */
    default void onExceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // 默认实现为空
    }
}
