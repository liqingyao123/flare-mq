package com.flare.mq.protocol.server;

import com.flare.mq.protocol.MessageType;
import com.flare.mq.protocol.ProtocolMessage;
import com.flare.mq.protocol.ResponseCode;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;

/**
 * 默认服务器请求处理器
 * 
 * @author FlareMQ Team
 */
public class DefaultServerRequestHandler implements ServerRequestHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(DefaultServerRequestHandler.class);
    
    @Override
    public ProtocolMessage handleRequest(ChannelHandlerContext ctx, ProtocolMessage request) {
        logger.debug("received request: {}", request);
        
        // 处理心跳请求
        if (request.getType() == MessageType.HEARTBEAT_REQUEST) {
            return ProtocolMessage.createHeartbeatResponse(request.getRequestId());
        }
        
        // 处理其他请求类型
        switch (request.getType()) {
            case SEND_MESSAGE_REQUEST:
                return handleSendMessage(ctx, request);
                
            case PULL_MESSAGE_REQUEST:
                return handlePullMessage(ctx, request);
                
            case ACK_MESSAGE_REQUEST:
                return handleAckMessage(ctx, request);
                
            case CREATE_TOPIC_REQUEST:
                return handleCreateTopic(ctx, request);
                
            case QUERY_TOPIC_REQUEST:
                return handleQueryTopic(ctx, request);
                
            default:
                logger.warn("Unknown request type: {}", request.getType());
                return ProtocolMessage.createErrorResponse(
                        MessageType.RESPONSE, 
                        request.getRequestId(), 
                        ResponseCode.BAD_REQUEST
                );
        }
    }
    
    /**
     * 处理发送消息请求
     */
    private ProtocolMessage handleSendMessage(ChannelHandlerContext ctx, ProtocolMessage request) {
        logger.info("handling send message request: {}", request.getRequestId());

        try {
            byte[] body = request.getBody();
            if (body == null || body.length == 0) {
                logger.warn("send request body is empty, requestId={}", request.getRequestId());
                return ProtocolMessage.createErrorResponse(
                        MessageType.SEND_MESSAGE_RESPONSE,
                        request.getRequestId(),
                        ResponseCode.BAD_REQUEST
                );
            }
            // 暂不做业务存储处理，仅校验并返回成功
            return ProtocolMessage.createSuccessResponse(
                    MessageType.SEND_MESSAGE_RESPONSE,
                    request.getRequestId(),
                    "message sent successfully".getBytes(StandardCharsets.UTF_8)
            );
        } catch (Exception e) {
            logger.error("handle send message error, requestId=" + request.getRequestId(), e);
            return ProtocolMessage.createErrorResponse(
                    MessageType.SEND_MESSAGE_RESPONSE,
                    request.getRequestId(),
                    ResponseCode.INTERNAL_ERROR
            );
        }
    }

    /**
     * 处理拉取消息请求
     */
    private ProtocolMessage handlePullMessage(ChannelHandlerContext ctx, ProtocolMessage request) {
        logger.info("handling pull message request: {}", request.getRequestId());

        try {
            // 简化实现：暂不返回消息，表示无新消息
            String payload = "{\"messages\":[],\"nextBeginOffset\":0,\"minOffset\":0,\"maxOffset\":0}";
            return ProtocolMessage.createSuccessResponse(
                    MessageType.PULL_MESSAGE_RESPONSE,
                    request.getRequestId(),
                    payload.getBytes(StandardCharsets.UTF_8)
            );
        } catch (Exception e) {
            logger.error("handle pull message error, requestId=" + request.getRequestId(), e);
            return ProtocolMessage.createErrorResponse(
                    MessageType.PULL_MESSAGE_RESPONSE,
                    request.getRequestId(),
                    ResponseCode.INTERNAL_ERROR
            );
        }
    }

    /**
     * 处理消息确认请求
     */
    private ProtocolMessage handleAckMessage(ChannelHandlerContext ctx, ProtocolMessage request) {
        logger.info("handling ack message request: {}", request.getRequestId());

        try {
            // 简化实现：认为确认成功
            return ProtocolMessage.createSuccessResponse(
                    MessageType.ACK_MESSAGE_RESPONSE,
                    request.getRequestId(),
                    "OK".getBytes(StandardCharsets.UTF_8)
            );
        } catch (Exception e) {
            logger.error("handle ack message error, requestId=" + request.getRequestId(), e);
            return ProtocolMessage.createErrorResponse(
                    MessageType.ACK_MESSAGE_RESPONSE,
                    request.getRequestId(),
                    ResponseCode.INTERNAL_ERROR
            );
        }
    }

    /**
     * 处理创建Topic请求
     */
    private ProtocolMessage handleCreateTopic(ChannelHandlerContext ctx, ProtocolMessage request) {
        logger.info("handling create topic request: {}", request.getRequestId());

        try {
            // 简化实现：直接返回成功
            return ProtocolMessage.createSuccessResponse(
                    MessageType.CREATE_TOPIC_RESPONSE,
                    request.getRequestId(),
                    "OK".getBytes(StandardCharsets.UTF_8)
            );
        } catch (Exception e) {
            logger.error("handle create topic error, requestId=" + request.getRequestId(), e);
            return ProtocolMessage.createErrorResponse(
                    MessageType.CREATE_TOPIC_RESPONSE,
                    request.getRequestId(),
                    ResponseCode.INTERNAL_ERROR
            );
        }
    }

    /**
     * 处理查询Topic请求
     */
    private ProtocolMessage handleQueryTopic(ChannelHandlerContext ctx, ProtocolMessage request) {
        logger.info("handling query topic request: {}", request.getRequestId());

        try {
            // 简化实现：返回空信息占位
            String payload = "{\"exists\":true}";
            return ProtocolMessage.createSuccessResponse(
                    MessageType.QUERY_TOPIC_RESPONSE,
                    request.getRequestId(),
                    payload.getBytes(StandardCharsets.UTF_8)
            );
        } catch (Exception e) {
            logger.error("handle query topic error, requestId=" + request.getRequestId(), e);
            return ProtocolMessage.createErrorResponse(
                    MessageType.QUERY_TOPIC_RESPONSE,
                    request.getRequestId(),
                    ResponseCode.INTERNAL_ERROR
            );
        }
    }

    @Override
    public void onChannelActive(ChannelHandlerContext ctx) {
        logger.info("client connection established: {}", ctx.channel().remoteAddress());
    }
    
    @Override
    public void onChannelInactive(ChannelHandlerContext ctx) {
        logger.info("client connection closed: {}", ctx.channel().remoteAddress());
    }
    
    @Override
    public void onExceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("exception while handling request: " + ctx.channel().remoteAddress(), cause);
    }
}
