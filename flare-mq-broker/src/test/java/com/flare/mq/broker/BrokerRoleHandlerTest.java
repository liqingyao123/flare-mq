package com.flare.mq.broker;

import com.flare.mq.broker.BrokerRequestHandler.ClusterRoleListener;
import com.flare.mq.protocol.MessageType;
import com.flare.mq.protocol.ProtocolMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

public class BrokerRoleHandlerTest {

    @Test
    public void testBecomeMasterInvokesListener() {
        AtomicLong receivedEpoch = new AtomicLong(-1);
        ClusterRoleListener listener = new ClusterRoleListener() {
            @Override public void onBecomeMaster(long epoch) { receivedEpoch.set(epoch); }
            @Override public void onStandDown() { }
            @Override public boolean isAcceptingWrites() { return true; }
        };

        BrokerRequestHandler handler = new BrokerRequestHandler(null, null, null, null, null);
        handler.setClusterRoleListener(listener);

        ProtocolMessage msg = new ProtocolMessage(MessageType.BECOME_MASTER_REQUEST,
                "{\"epoch\":7}".getBytes(StandardCharsets.UTF_8));
        ProtocolMessage resp = handler.handleRequest(null, msg);

        assertTrue(resp.isSuccess());
        assertEquals(7L, receivedEpoch.get());
    }

    @Test
    public void testSendRejectedWhenNotAcceptingWrites() {
        ClusterRoleListener listener = new ClusterRoleListener() {
            @Override public void onBecomeMaster(long epoch) { }
            @Override public void onStandDown() { }
            @Override public boolean isAcceptingWrites() { return false; }
        };

        BrokerRequestHandler handler = new BrokerRequestHandler(null, null, null, null, null);
        handler.setClusterRoleListener(listener);

        String body = "{\"topic\":\"t\",\"body\":\"hello\",\"messageId\":\"m1\"}";
        ProtocolMessage msg = new ProtocolMessage(MessageType.SEND_MESSAGE_REQUEST,
                body.getBytes(StandardCharsets.UTF_8));
        ProtocolMessage resp = handler.handleRequest(null, msg);

        assertFalse(resp.isSuccess());   // 停写时拒收新消息
    }
}
