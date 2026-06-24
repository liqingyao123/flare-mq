package com.ruyuan.mq.protocol.codec;

import com.ruyuan.mq.protocol.MessageType;
import com.ruyuan.mq.protocol.ProtocolMessage;
import com.ruyuan.mq.protocol.ResponseCode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 协议编解码器测试
 * 
 * @author RuYuan MQ Team
 */
class ProtocolCodecTest {
    
    @Test
    void testEncodeAndDecode() {
        // 创建测试消息
        byte[] body = "Hello RuYuan MQ".getBytes();
        ProtocolMessage originalMessage = new ProtocolMessage(MessageType.SEND_MESSAGE_REQUEST, body);
        
        // 创建编解码器通道
        EmbeddedChannel channel = new EmbeddedChannel(
                new ProtocolEncoder(),
                new ProtocolDecoder()
        );
        
        // 编码
        assertTrue(channel.writeOutbound(originalMessage));
        
        // 获取编码后的数据
        ByteBuf encoded = channel.readOutbound();
        assertNotNull(encoded);
        
        // 解码
        assertTrue(channel.writeInbound(encoded));
        
        // 获取解码后的消息
        ProtocolMessage decodedMessage = channel.readInbound();
        assertNotNull(decodedMessage);
        
        // 验证消息内容
        assertEquals(originalMessage.getLength(), decodedMessage.getLength());
        assertEquals(originalMessage.getType(), decodedMessage.getType());
        assertEquals(originalMessage.getRequestId(), decodedMessage.getRequestId());
        assertEquals(originalMessage.getStatus(), decodedMessage.getStatus());
        assertArrayEquals(originalMessage.getBody(), decodedMessage.getBody());
        
        channel.close();
    }
    
    @Test
    void testEncodeHeartbeat() {
        ProtocolMessage heartbeat = ProtocolMessage.createHeartbeatRequest();
        
        EmbeddedChannel channel = new EmbeddedChannel(
                new ProtocolEncoder(),
                new ProtocolDecoder()
        );
        
        // 编码解码心跳消息
        assertTrue(channel.writeOutbound(heartbeat));
        ByteBuf encoded = channel.readOutbound();
        assertNotNull(encoded);
        
        assertTrue(channel.writeInbound(encoded));
        ProtocolMessage decoded = channel.readInbound();
        
        assertNotNull(decoded);
        assertEquals(MessageType.HEARTBEAT_REQUEST, decoded.getType());
        assertEquals(ResponseCode.SUCCESS, decoded.getStatus());
        assertNull(decoded.getBody());
        
        channel.close();
    }
    
    @Test
    void testDecodePartialMessage() {
        // 创建测试消息
        ProtocolMessage message = new ProtocolMessage(MessageType.SEND_MESSAGE_REQUEST, "test".getBytes());
        
        EmbeddedChannel encodeChannel = new EmbeddedChannel(new ProtocolEncoder());
        assertTrue(encodeChannel.writeOutbound(message));
        ByteBuf fullMessage = encodeChannel.readOutbound();
        
        // 创建解码通道
        EmbeddedChannel decodeChannel = new EmbeddedChannel(new ProtocolDecoder());
        
        // 分两次发送数据，第一次只发送部分数据
        int halfLength = fullMessage.readableBytes() / 2;
        ByteBuf firstPart = fullMessage.readSlice(halfLength);
        ByteBuf secondPart = fullMessage.readSlice(fullMessage.readableBytes());
        
        // 发送第一部分，应该没有输出
        assertFalse(decodeChannel.writeInbound(firstPart.copy()));
        assertNull(decodeChannel.readInbound());
        
        // 发送第二部分，应该有完整消息输出
        assertTrue(decodeChannel.writeInbound(secondPart.copy()));
        ProtocolMessage decoded = decodeChannel.readInbound();
        
        assertNotNull(decoded);
        assertEquals(message.getType(), decoded.getType());
        assertEquals(message.getRequestId(), decoded.getRequestId());
        
        encodeChannel.close();
        decodeChannel.close();
    }
    
    @Test
    void testDecodeMultipleMessages() {
        // 创建多个测试消息
        ProtocolMessage msg1 = ProtocolMessage.createHeartbeatRequest();
        ProtocolMessage msg2 = new ProtocolMessage(MessageType.SEND_MESSAGE_REQUEST, "test1".getBytes());
        ProtocolMessage msg3 = new ProtocolMessage(MessageType.PULL_MESSAGE_REQUEST, "test2".getBytes());
        
        // 编码所有消息
        EmbeddedChannel encodeChannel = new EmbeddedChannel(new ProtocolEncoder());
        assertTrue(encodeChannel.writeOutbound(msg1));
        assertTrue(encodeChannel.writeOutbound(msg2));
        assertTrue(encodeChannel.writeOutbound(msg3));
        
        // 合并所有编码数据
        ByteBuf combined = Unpooled.buffer();
        ByteBuf encoded1 = encodeChannel.readOutbound();
        ByteBuf encoded2 = encodeChannel.readOutbound();
        ByteBuf encoded3 = encodeChannel.readOutbound();
        combined.writeBytes(encoded1);
        combined.writeBytes(encoded2);
        combined.writeBytes(encoded3);
        
        // 解码
        EmbeddedChannel decodeChannel = new EmbeddedChannel(new ProtocolDecoder());
        assertTrue(decodeChannel.writeInbound(combined));
        
        // 验证解码出3个消息
        ProtocolMessage decoded1 = decodeChannel.readInbound();
        ProtocolMessage decoded2 = decodeChannel.readInbound();
        ProtocolMessage decoded3 = decodeChannel.readInbound();
        
        assertNotNull(decoded1);
        assertNotNull(decoded2);
        assertNotNull(decoded3);
        
        assertEquals(msg1.getType(), decoded1.getType());
        assertEquals(msg2.getType(), decoded2.getType());
        assertEquals(msg3.getType(), decoded3.getType());
        
        encodeChannel.close();
        decodeChannel.close();
    }
}
