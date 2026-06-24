package com.ruyuan.mq.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ProtocolMessage单元测试
 * 
 * @author RuYuan MQ Team
 */
class ProtocolMessageTest {
    
    @Test
    void testCreateHeartbeatRequest() {
        ProtocolMessage message = ProtocolMessage.createHeartbeatRequest();
        
        assertNotNull(message);
        assertEquals(MessageType.HEARTBEAT_REQUEST, message.getType());
        assertEquals(ResponseCode.SUCCESS, message.getStatus());
        assertNull(message.getBody());
        assertEquals(ProtocolConstants.HEADER_LENGTH, message.getLength());
        assertTrue(message.isRequest());
        assertTrue(message.isHeartbeat());
        assertFalse(message.isResponse());
    }
    
    @Test
    void testCreateHeartbeatResponse() {
        int requestId = 123;
        ProtocolMessage message = ProtocolMessage.createHeartbeatResponse(requestId);
        
        assertNotNull(message);
        assertEquals(MessageType.HEARTBEAT_RESPONSE, message.getType());
        assertEquals(requestId, message.getRequestId());
        assertEquals(ResponseCode.SUCCESS, message.getStatus());
        assertNull(message.getBody());
        assertEquals(ProtocolConstants.HEADER_LENGTH, message.getLength());
        assertTrue(message.isResponse());
        assertTrue(message.isHeartbeat());
        assertFalse(message.isRequest());
    }
    
    @Test
    void testCreateSuccessResponse() {
        int requestId = 456;
        byte[] body = "test response".getBytes();
        ProtocolMessage message = ProtocolMessage.createSuccessResponse(
                MessageType.SEND_MESSAGE_RESPONSE, requestId, body);
        
        assertNotNull(message);
        assertEquals(MessageType.SEND_MESSAGE_RESPONSE, message.getType());
        assertEquals(requestId, message.getRequestId());
        assertEquals(ResponseCode.SUCCESS, message.getStatus());
        assertArrayEquals(body, message.getBody());
        assertEquals(ProtocolConstants.HEADER_LENGTH + body.length, message.getLength());
        assertTrue(message.isResponse());
        assertTrue(message.isSuccess());
        assertFalse(message.isRequest());
    }
    
    @Test
    void testCreateErrorResponse() {
        int requestId = 789;
        ProtocolMessage message = ProtocolMessage.createErrorResponse(
                MessageType.SEND_MESSAGE_RESPONSE, requestId, ResponseCode.INTERNAL_ERROR);
        
        assertNotNull(message);
        assertEquals(MessageType.SEND_MESSAGE_RESPONSE, message.getType());
        assertEquals(requestId, message.getRequestId());
        assertEquals(ResponseCode.INTERNAL_ERROR, message.getStatus());
        assertNull(message.getBody());
        assertEquals(ProtocolConstants.HEADER_LENGTH, message.getLength());
        assertTrue(message.isResponse());
        assertFalse(message.isSuccess());
        assertFalse(message.isRequest());
    }
    
    @Test
    void testRequestIdGeneration() {
        int id1 = ProtocolMessage.generateRequestId();
        int id2 = ProtocolMessage.generateRequestId();
        
        assertTrue(id2 > id1);
    }
    
    @Test
    void testSetBody() {
        ProtocolMessage message = new ProtocolMessage();
        byte[] body = "test body".getBytes();
        
        message.setBody(body);
        
        assertArrayEquals(body, message.getBody());
        assertEquals(ProtocolConstants.HEADER_LENGTH + body.length, message.getLength());
        assertEquals(body.length, message.getBodyLength());
    }
    
    @Test
    void testSetNullBody() {
        ProtocolMessage message = new ProtocolMessage();
        
        message.setBody(null);
        
        assertNull(message.getBody());
        assertEquals(ProtocolConstants.HEADER_LENGTH, message.getLength());
        assertEquals(0, message.getBodyLength());
    }
}
