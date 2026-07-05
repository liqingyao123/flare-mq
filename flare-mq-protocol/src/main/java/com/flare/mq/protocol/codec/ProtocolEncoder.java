package com.flare.mq.protocol.codec;

import com.flare.mq.protocol.ProtocolMessage;
import com.flare.mq.protocol.ResponseCode;
import com.flare.mq.protocol.zerocopy.DirectBufferPool;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 协议编码器（零拷贝优化版）
 *
 * 将ProtocolMessage编码为字节流，使用DirectBuffer池化减少GC压力
 *
 * @author FlareMQ Team
 */
public class ProtocolEncoder extends MessageToByteEncoder<ProtocolMessage> {

    private static final Logger logger = LoggerFactory.getLogger(ProtocolEncoder.class);

    /**
     * DirectBuffer池
     */
    private final DirectBufferPool bufferPool;

    /**
     * 构造函数
     */
    public ProtocolEncoder() {
        this.bufferPool = DirectBufferPool.getInstance();
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ProtocolMessage msg, ByteBuf out) throws Exception {
        try {
            // 使用DirectBuffer优化大消息的编码
            if (msg.getBody() != null && msg.getBody().length > 4096) {
                encodeWithDirectBuffer(msg, out);
            } else {
                // 小消息直接使用原有方式
                encodeDirectly(msg, out);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("编码消息成功: {}", msg);
            }

        } catch (Exception e) {
            logger.error("编码消息失败: " + msg, e);
            throw e;
        }
    }

    /**
     * 直接编码（小消息）
     */
    private void encodeDirectly(ProtocolMessage msg, ByteBuf out) {
        // 写入消息长度
        out.writeInt(msg.getLength());

        // 写入消息类型
        out.writeShort(msg.getType().getCode());

        // 写入请求ID
        out.writeInt(msg.getRequestId());

        // 写入状态码
        ResponseCode status = msg.getStatus();
        if (status != null) {
            out.writeShort(status.getCode());
        } else {
            // 对于请求消息，状态码可以为null，使用默认值0
            out.writeShort(0);
        }

        // 写入消息体
        if (msg.getBody() != null && msg.getBody().length > 0) {
            out.writeBytes(msg.getBody());
        }
    }

    /**
     * 使用DirectBuffer编码（大消息）
     */
    private void encodeWithDirectBuffer(ProtocolMessage msg, ByteBuf out) {
        java.nio.ByteBuffer directBuffer = null;
        try {
            // 从池中获取DirectBuffer
            int totalSize = msg.getLength();
            directBuffer = bufferPool.acquire(totalSize);

            // 写入协议头
            directBuffer.putInt(msg.getLength());
            directBuffer.putShort(msg.getType().getCode());
            directBuffer.putInt(msg.getRequestId());

            // 写入状态码
            ResponseCode status = msg.getStatus();
            if (status != null) {
                directBuffer.putShort(status.getCode());
            } else {
                // 对于请求消息，状态码可以为null，使用默认值0
                directBuffer.putShort((short) 0);
            }

            // 写入消息体
            if (msg.getBody() != null && msg.getBody().length > 0) {
                directBuffer.put(msg.getBody());
            }

            // 准备读取
            directBuffer.flip();

            // 写入到输出缓冲区
            out.writeBytes(directBuffer);

            logger.debug("使用DirectBuffer编码大消息: size={}", totalSize);

        } finally {
            // 释放DirectBuffer回池
            if (directBuffer != null) {
                bufferPool.release(directBuffer);
            }
        }
    }
}
