package com.flare.mq.protocol.client;

import com.flare.mq.protocol.ProtocolConstants;
import com.flare.mq.protocol.ProtocolMessage;
import com.flare.mq.protocol.codec.ProtocolDecoder;
import com.flare.mq.protocol.codec.ProtocolEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Netty client — connection lifecycle managed via Netty's EventLoop,
 * with automatic reconnection on disconnect using exponential backoff.
 *
 * @author FlareMQ Team
 */
public class NettyClient {

    private static final Logger logger = LoggerFactory.getLogger(NettyClient.class);

    private static final long INITIAL_RECONNECT_DELAY_MS = 1000;
    private static final long MAX_RECONNECT_DELAY_MS = 30000;

    private final String host;
    private final int port;
    private final EventLoopGroup workerGroup;
    private final Bootstrap bootstrap;
    private final AtomicBoolean connected = new AtomicBoolean(false);

    private volatile Channel channel;
    private volatile ClientHandler clientHandler;
    private volatile boolean shutdown = false;
    private int reconnectAttempts = 0;

    public NettyClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.workerGroup = new NioEventLoopGroup();
        this.bootstrap = new Bootstrap();

        initBootstrap();
        startTimeoutChecker();
    }

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
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();

                        pipeline.addLast("idleStateHandler",
                                new IdleStateHandler(60, 30, 90, TimeUnit.SECONDS));

                        pipeline.addLast("decoder", new ProtocolDecoder());
                        pipeline.addLast("encoder", new ProtocolEncoder());

                        clientHandler = new ClientHandler();
                        pipeline.addLast("clientHandler", clientHandler);
                    }
                });
    }

    // ======================== Connection lifecycle ========================

    /**
     * Connect to server (blocking). Prefer this for initial connection.
     */
    public void connect() throws InterruptedException {
        if (connected.get()) {
            logger.warn("Client already connected to server: {}:{}", host, port);
            return;
        }

        ChannelFuture future = bootstrap.connect(host, port).sync();
        if (future.isSuccess()) {
            onConnectSuccess(future.channel());
        } else {
            throw new RuntimeException("Failed to connect to " + getServerAddress(), future.cause());
        }
    }

    /**
     * Non-blocking connect. Starts reconnection loop on Netty's EventLoop.
     */
    public void connectAsync() {
        if (connected.get()) {
            logger.warn("Client already connected to server: {}:{}", host, port);
            return;
        }
        doConnect();
    }

    /**
     * Core connect — non-blocking, schedules retry on EventLoop on failure.
     */
    private void doConnect() {
        if (shutdown) {
            return;
        }

        logger.info("Connecting to {}:{} (attempt {})", host, port, reconnectAttempts + 1);

        bootstrap.connect(host, port).addListener((ChannelFuture f) -> {
            if (shutdown) {
                if (f.channel() != null) {
                    f.channel().close();
                }
                return;
            }

            if (f.isSuccess()) {
                onConnectSuccess(f.channel());
            } else {
                onConnectFailure();
            }
        });
    }

    private void onConnectSuccess(Channel ch) {
        channel = ch;
        connected.set(true);
        reconnectAttempts = 0;

        logger.info("Connected to server successfully: {}", getServerAddress());

        // When the connection drops, trigger reconnection automatically.
        ch.closeFuture().addListener((ChannelFuture cf) -> {
            logger.warn("Connection to {} closed", getServerAddress());
            connected.set(false);
            if (!shutdown) {
                doConnect();
            }
        });
    }

    private void onConnectFailure() {
        reconnectAttempts++;
        connected.set(false);

        long delay = Math.min(
                INITIAL_RECONNECT_DELAY_MS * (1L << Math.min(reconnectAttempts - 1, 5)),
                MAX_RECONNECT_DELAY_MS);

        logger.info("Connect to {} failed, retrying in {}ms (attempt {})",
                getServerAddress(), delay, reconnectAttempts);

        // Schedule retry on EventLoop — no extra thread required.
        workerGroup.next().schedule(this::doConnect, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * Blocking reconnect helper for callers that need the connection now.
     */
    public boolean tryReconnect(long timeoutMs) {
        if (connected.get()) {
            return true;
        }

        logger.info("Blocking reconnect to {} (timeout={}ms)", getServerAddress(), timeoutMs);
        long deadline = System.currentTimeMillis() + timeoutMs;

        while (!connected.get() && System.currentTimeMillis() < deadline) {
            try {
                connect();
                if (connected.get()) {
                    return true;
                }
            } catch (Exception e) {
                logger.debug("Reconnect attempt to {} failed: {}", getServerAddress(), e.getMessage());
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        return connected.get();
    }

    // ======================== Disconnect / Shutdown ========================

    public void disconnect() {
        if (!connected.compareAndSet(true, false)) {
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

    public void shutdown() {
        shutdown = true;
        disconnect();
        workerGroup.shutdownGracefully();
    }

    // ======================== Messaging ========================

    public ProtocolMessage sendSync(ProtocolMessage request, long timeoutMs) throws InterruptedException {
        if (!isConnected()) {
            if (!tryReconnect(timeoutMs)) {
                throw new RuntimeException("Client not connected to " + getServerAddress());
            }
        }

        ResponseFuture future = new ResponseFuture(request.getRequestId(), timeoutMs, null);
        clientHandler.addResponseFuture(request.getRequestId(), future);

        try {
            Channel ch = channel;
            if (ch == null || !ch.isActive()) {
                throw new RuntimeException("Channel is not active");
            }
            ch.writeAndFlush(request);

            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            clientHandler.removeResponseFuture(request.getRequestId());
            throw new RuntimeException("Send request failed", e);
        }
    }

    public void sendAsync(ProtocolMessage request, ResponseCallback callback) {
        if (!isConnected()) {
            if (!tryReconnect(5000)) {
                callback.onFailure(new RuntimeException("Client not connected to " + getServerAddress()));
                return;
            }
        }

        ResponseFuture future = new ResponseFuture(
                request.getRequestId(),
                ProtocolConstants.REQUEST_TIMEOUT,
                callback);
        clientHandler.addResponseFuture(request.getRequestId(), future);

        Channel ch = channel;
        if (ch == null || !ch.isActive()) {
            clientHandler.removeResponseFuture(request.getRequestId());
            callback.onFailure(new RuntimeException("Channel is not active"));
            return;
        }

        ch.writeAndFlush(request).addListener((ChannelFuture cf) -> {
            if (!cf.isSuccess()) {
                clientHandler.removeResponseFuture(request.getRequestId());
                callback.onFailure(cf.cause());
            }
        });
    }

    // ======================== Helpers ========================

    public boolean isConnected() {
        return connected.get() && channel != null && channel.isActive();
    }

    public String getServerAddress() {
        return host + ":" + port;
    }

    /**
     * Periodically clean up timed-out pending requests.
     * Scheduled on Netty's EventLoop, not a separate thread.
     */
    private void startTimeoutChecker() {
        workerGroup.next().scheduleAtFixedRate(() -> {
            if (clientHandler != null) {
                clientHandler.cleanupTimeoutRequests();
            }
        }, 1, 1, TimeUnit.SECONDS);
    }
}
