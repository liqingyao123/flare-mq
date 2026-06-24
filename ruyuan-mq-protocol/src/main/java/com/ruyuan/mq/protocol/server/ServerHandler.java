package com.ruyuan.mq.protocol.server;

import com.ruyuan.mq.protocol.ProtocolMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 服务器处理器
 * 
 * @author RuYuan MQ Team
 */
public class ServerHandler extends ChannelInboundHandlerAdapter {
    
    private static final Logger logger = LoggerFactory.getLogger(ServerHandler.class);
    
    private final ServerRequestHandler requestHandler;
    
    public ServerHandler(ServerRequestHandler requestHandler) {
        this.requestHandler = requestHandler;
    }
    
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.info("client connection established: {}", ctx.channel().remoteAddress());
        requestHandler.onChannelActive(ctx);
        super.channelActive(ctx);
    }
    
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.info("client connection closed: {}", ctx.channel().remoteAddress());
        requestHandler.onChannelInactive(ctx);
        super.channelInactive(ctx);
    }
    
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof ProtocolMessage)) {
            logger.warn("Received non-protocol message: {}", msg.getClass().getName());
            return;
        }
        
        ProtocolMessage request = (ProtocolMessage) msg;
        
        try {
            // 处理请求
            ProtocolMessage response = requestHandler.handleRequest(ctx, request);
            
            // 发送响应（如果有）
            if (response != null) {
                ctx.writeAndFlush(response);
            }
            
        } catch (Exception e) {
            logger.error("Failed to handle request: " + request, e);
            
            // 发送错误响应
            ProtocolMessage errorResponse = ProtocolMessage.createErrorResponse(
                    request.getType(),
                    request.getRequestId(),
                    com.ruyuan.mq.protocol.ResponseCode.INTERNAL_ERROR
            );
            ctx.writeAndFlush(errorResponse);
        }
    }
    
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            
            if (event.state() == IdleState.READER_IDLE) {
                logger.warn("Client read idle timeout, closing connection: {}", ctx.channel().remoteAddress());
                ctx.close();
            } else if (event.state() == IdleState.WRITER_IDLE) {
                logger.debug("Client write idle, sending heartbeat: {}", ctx.channel().remoteAddress());
                // 服务器端不主动发送心跳，由客户端发送
            } else if (event.state() == IdleState.ALL_IDLE) {
                logger.warn("Client read-write idle timeout, closing connection: {}", ctx.channel().remoteAddress());
                ctx.close();
            }
        }
        
        super.userEventTriggered(ctx, evt);
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Connection exception: " + ctx.channel().remoteAddress(), cause);
        requestHandler.onExceptionCaught(ctx, cause);
        ctx.close();
    }
}
