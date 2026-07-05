package com.flare.mq.protocol.codec;

import com.flare.mq.protocol.MessageType;
import com.flare.mq.protocol.ProtocolConstants;
import com.flare.mq.protocol.ProtocolMessage;
import com.flare.mq.protocol.ResponseCode;
import com.flare.mq.protocol.zerocopy.DirectBufferPool;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 协议解码器（零拷贝优化版）
 *
 * 将字节流解码为ProtocolMessage，使用DirectBuffer池化减少GC压力
 *
 * @author FlareMQ Team
 */
public class ProtocolDecoder extends ByteToMessageDecoder {

    private static final Logger logger = LoggerFactory.getLogger(ProtocolDecoder.class);

    /**
     * DirectBuffer池
     */
    private final DirectBufferPool bufferPool;

    /**
     * 构造函数
     */
    public ProtocolDecoder() {
        this.bufferPool = DirectBufferPool.getInstance();
    }
    
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        try {
            // 检查是否有足够的字节读取协议头
            if (in.readableBytes() < ProtocolConstants.HEADER_LENGTH) {
                return; // 等待更多数据
            }
            
            // 标记读取位置，以便回滚
            in.markReaderIndex();
            
            // 读取消息长度
            int length = in.readInt();
            
            // 验证消息长度
            if (length < ProtocolConstants.MIN_MESSAGE_LENGTH || length > ProtocolConstants.MAX_BODY_LENGTH + ProtocolConstants.HEADER_LENGTH) {
                logger.error("无效的消息长度: {}", length);
                ctx.close(); // 关闭连接
                return;
            }
            
            // 检查是否有足够的字节读取完整消息
            if (in.readableBytes() < length - 4) { // 减去已读取的length字段
                in.resetReaderIndex(); // 回滚读取位置
                return; // 等待更多数据
            }
            
            // 读取消息类型
            short typeCode = in.readShort();
            MessageType messageType;
            try {
                messageType = MessageType.valueOf(typeCode);
            } catch (IllegalArgumentException e) {
                logger.error("未知的消息类型: {}", typeCode);
                ctx.close(); // 关闭连接
                return;
            }
            
            // 读取请求ID
            int requestId = in.readInt();
            
            // 读取状态码
            short statusCode = in.readShort();
            ResponseCode responseCode = ResponseCode.valueOf(statusCode);
            
            // 读取消息体（使用DirectBuffer优化大消息）
            int bodyLength = length - ProtocolConstants.HEADER_LENGTH;
            byte[] body = null;
            if (bodyLength > 0) {
                if (bodyLength > 4096) {
                    // 大消息使用DirectBuffer
                    body = readBodyWithDirectBuffer(in, bodyLength);
                } else {
                    // 小消息直接读取
                    body = new byte[bodyLength];
                    in.readBytes(body);
                }
            }
            
            // 创建协议消息
            ProtocolMessage message = new ProtocolMessage();
            message.setLength(length);
            message.setType(messageType);
            message.setRequestId(requestId);
            message.setStatus(responseCode);
            message.setBody(body);
            
            // 添加到输出列表
            out.add(message);
            
            if (logger.isDebugEnabled()) {
                logger.debug("解码消息成功: {}", message);
            }
            
        } catch (Exception e) {
            logger.error("解码消息失败", e);
            ctx.close(); // 关闭连接
        }
    }

    /**
     * 使用DirectBuffer读取大消息体
     */
    private byte[] readBodyWithDirectBuffer(ByteBuf in, int bodyLength) {
        java.nio.ByteBuffer directBuffer = null;
        try {
            // 从池中获取DirectBuffer
            directBuffer = bufferPool.acquire(bodyLength);

            // 从输入缓冲区读取到DirectBuffer
            in.readBytes(directBuffer);

            // 转换为字节数组
            directBuffer.flip();
            byte[] body = new byte[bodyLength];
            directBuffer.get(body);

            logger.debug("使用DirectBuffer读取大消息体: size={}", bodyLength);
            return body;

        } finally {
            // 释放DirectBuffer回池
            if (directBuffer != null) {
                bufferPool.release(directBuffer);
            }
        }
    }
}
