package com.ruyuan.mq.protocol.client;

import com.ruyuan.mq.protocol.MessageType;
import com.ruyuan.mq.protocol.ProtocolMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Client handler
 *
 * @author RuYuan MQ Team
 */
public class ClientHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(ClientHandler.class);

    /**
     * Response table for request-response matching
     */
    private final ConcurrentMap<Integer, ResponseFuture> responseTable = new ConcurrentHashMap<>();
    
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.info("Connected to server successfully: {}", ctx.channel().remoteAddress());
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.info("Disconnected from server: {}", ctx.channel().remoteAddress());
        
        // Clean up all pending requests
        for (ResponseFuture future : responseTable.values()) {
            future.setFailure(new RuntimeException("Connection closed"));
        }
        responseTable.clear();
        
        super.channelInactive(ctx);
    }
    
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof ProtocolMessage)) {
            logger.warn("Received non-protocol message: {}", msg.getClass().getName());
            return;
        }

        ProtocolMessage response = (ProtocolMessage) msg;

        // Find corresponding request Future
        ResponseFuture future = responseTable.remove(response.getRequestId());
        if (future != null) {
            future.setSuccess(response);
            logger.debug("Response matched successfully: requestId={}, type={}",
                        response.getRequestId(), response.getType());
        } else {
            // Handle unmatched responses more gracefully
            handleUnmatchedResponse(response);
        }
    }
    
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            
            if (event.state() == IdleState.WRITER_IDLE) {
                // Send heartbeat
                logger.debug("Sending heartbeat to server: {}", ctx.channel().remoteAddress());
                ProtocolMessage heartbeat = ProtocolMessage.createHeartbeatRequest();
                ctx.writeAndFlush(heartbeat);
            }
        }
        
        super.userEventTriggered(ctx, evt);
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Connection exception: " + ctx.channel().remoteAddress(), cause);
        
        // Notify all pending requests
        for (ResponseFuture future : responseTable.values()) {
            future.setFailure(cause);
        }
        responseTable.clear();
        
        ctx.close();
    }
    
    /**
     * Add response Future
     */
    public void addResponseFuture(int requestId, ResponseFuture future) {
        responseTable.put(requestId, future);
    }

    /**
     * Remove response Future
     */
    public ResponseFuture removeResponseFuture(int requestId) {
        return responseTable.remove(requestId);
    }

    /**
     * Clean up timeout requests
     */
    public void cleanupTimeoutRequests() {
        responseTable.entrySet().removeIf(entry -> {
            ResponseFuture future = entry.getValue();
            if (future.isTimeout()) {
                future.handleTimeout();
                return true;
            }
            return false;
        });
    }
    
    /**
     * Get pending request count
     */
    public int getPendingRequestCount() {
        return responseTable.size();
    }

    /**
     * Handle unmatched response more gracefully
     */
    private void handleUnmatchedResponse(ProtocolMessage response) {
        // Check if it's a heartbeat response - these are often unmatched due to timing
        if (response.getType() == MessageType.HEARTBEAT_RESPONSE) {
            logger.debug("Received heartbeat response (normal): requestId={}", response.getRequestId());
            return;
        }

        // Check if it's a response to a request that might have timed out
        if (response.getRequestId() > 0) {
            logger.debug("Received response for request that may have timed out: requestId={}, type={}",
                        response.getRequestId(), response.getType());
        } else {
            // Invalid request ID
            logger.warn("Received response with invalid request ID: {}, type={}",
                       response.getRequestId(), response.getType());
        }
    }
}
