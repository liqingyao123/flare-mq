package com.ruyuan.mq.protocol.client;

import com.ruyuan.mq.protocol.ProtocolConstants;
import com.ruyuan.mq.protocol.ProtocolMessage;
import com.ruyuan.mq.protocol.codec.ProtocolDecoder;
import com.ruyuan.mq.protocol.codec.ProtocolEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Netty client
 *
 * @author RuYuan MQ Team
 */
public class NettyClient {
    
    private static final Logger logger = LoggerFactory.getLogger(NettyClient.class);
    
    private final String host;
    private final int port;
    private final EventLoopGroup workerGroup;
    private final Bootstrap bootstrap;
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    
    private volatile Channel channel;
    private volatile ClientHandler clientHandler;
    
    public NettyClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.workerGroup = new NioEventLoopGroup();
        this.bootstrap = new Bootstrap();
        
        initBootstrap();
        startTimeoutChecker();
    }
    
    /**
     * Initialize Bootstrap
     */
    private void initBootstrap() {
        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_SNDBUF, 65536)
                .option(ChannelOption.SO_RCVBUF, 65536)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, ProtocolConstants.CONNECTION_TIMEOUT)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        
                        // Idle detection: read idle 60s, write idle 30s, read-write idle 90s
                        pipeline.addLast("idleStateHandler",
                                new IdleStateHandler(60, 30, 90, TimeUnit.SECONDS));

                        // Protocol codec
                        pipeline.addLast("decoder", new ProtocolDecoder());
                        pipeline.addLast("encoder", new ProtocolEncoder());

                        // Client handler
                        clientHandler = new ClientHandler();
                        pipeline.addLast("clientHandler", clientHandler);
                    }
                });
    }
    
    /**
     * Connect to server
     */
    public void connect() throws InterruptedException {
        if (connected.get()) {
            logger.warn("Client already connected to server: {}:{}", host, port);
            return;
        }

        try {
            ChannelFuture future = bootstrap.connect(host, port).sync();
            channel = future.channel();
            connected.set(true);

            logger.info("Connected to server successfully: {}:{}", host, port);

        } catch (Exception e) {
            logger.error("Failed to connect to server: {}:{}", host, port, e);
            connected.set(false);
            throw e;
        }
    }
    
    /**
     * Disconnect
     */
    public void disconnect() {
        if (!connected.compareAndSet(true, false)) {
            logger.warn("Client not connected or already disconnected");
            return;
        }

        try {
            if (channel != null) {
                channel.close().sync();
            }

            logger.info("Disconnected from server successfully");

        } catch (Exception e) {
            logger.error("Failed to disconnect from server", e);
        }
    }

    /**
     * Close client
     */
    public void shutdown() {
        disconnect();

        // Close scheduler
        scheduler.shutdown();

        // Close thread group
        workerGroup.shutdownGracefully();
    }

    /**
     * Send message synchronously
     */
    public ProtocolMessage sendSync(ProtocolMessage request, long timeoutMs) throws InterruptedException {
        if (!isConnected()) {
            throw new RuntimeException("Client not connected");
        }
        
        ResponseFuture future = new ResponseFuture(request.getRequestId(), timeoutMs, null);
        clientHandler.addResponseFuture(request.getRequestId(), future);
        
        try {
            // Send request
            channel.writeAndFlush(request);

            // Wait for response
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
            
        } catch (Exception e) {
            clientHandler.removeResponseFuture(request.getRequestId());
            throw new RuntimeException("Send request failed", e);
        }
    }
    
    /**
     * Send message asynchronously
     */
    public void sendAsync(ProtocolMessage request, ResponseCallback callback) {
        if (!isConnected()) {
            callback.onFailure(new RuntimeException("Client not connected"));
            return;
        }

        ResponseFuture future = new ResponseFuture(
                request.getRequestId(),
                ProtocolConstants.REQUEST_TIMEOUT,
                callback
        );
        clientHandler.addResponseFuture(request.getRequestId(), future);

        // Send request
        channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                if (!channelFuture.isSuccess()) {
                    clientHandler.removeResponseFuture(request.getRequestId());
                    callback.onFailure(channelFuture.cause());
                }
            }
        });
    }
    
    /**
     * Check if connected
     */
    public boolean isConnected() {
        return connected.get() && channel != null && channel.isActive();
    }

    /**
     * Start timeout checker
     */
    private void startTimeoutChecker() {
        scheduler.scheduleAtFixedRate(() -> {
            if (clientHandler != null) {
                clientHandler.cleanupTimeoutRequests();
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    /**
     * Get server address
     */
    public String getServerAddress() {
        return host + ":" + port;
    }

    /**
     * Get pending request count
     */
    public int getPendingRequestCount() {
        return clientHandler != null ? clientHandler.getPendingRequestCount() : 0;
    }
}
