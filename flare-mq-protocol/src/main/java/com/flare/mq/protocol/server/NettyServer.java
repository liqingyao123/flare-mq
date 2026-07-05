package com.flare.mq.protocol.server;

import com.flare.mq.protocol.codec.ProtocolDecoder;
import com.flare.mq.protocol.codec.ProtocolEncoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Netty服务器
 * 
 * @author FlareMQ Team
 */
public class NettyServer {
    
    private static final Logger logger = LoggerFactory.getLogger(NettyServer.class);
    
    private final int port;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ServerRequestHandler requestHandler;
    private final AtomicBoolean started = new AtomicBoolean(false);
    
    private Channel serverChannel;
    
    public NettyServer(int port) {
        this(port, new DefaultServerRequestHandler());
    }
    
    public NettyServer(int port, ServerRequestHandler requestHandler) {
        this.port = port;
        this.requestHandler = requestHandler;
        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup();
    }
    
    /**
     * 启动服务器
     */
    public void start() throws InterruptedException {
        if (!started.compareAndSet(false, true)) {
            logger.warn("server already started on port: {}", port);
            return;
        }
        
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_SNDBUF, 65536)
                    .childOption(ChannelOption.SO_RCVBUF, 65536)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            
                            // 空闲检测：读空闲60秒，写空闲30秒，读写空闲90秒
                            pipeline.addLast("idleStateHandler", 
                                    new IdleStateHandler(60, 30, 90, TimeUnit.SECONDS));
                            
                            // 协议编解码器
                            pipeline.addLast("decoder", new ProtocolDecoder());
                            pipeline.addLast("encoder", new ProtocolEncoder());
                            
                            // 业务处理器
                            pipeline.addLast("serverHandler", new ServerHandler(requestHandler));
                        }
                    });
            
            // 绑定端口并启动服务器
            ChannelFuture future = bootstrap.bind(port).sync();
            serverChannel = future.channel();
            
            logger.info("Netty server started successfully on port: {}", port);
            
        } catch (Exception e) {
            logger.error("failed to start Netty server", e);
            started.set(false);
            throw e;
        }
    }
    
    /**
     * 关闭服务器
     */
    public void shutdown() {
        if (!started.compareAndSet(true, false)) {
            logger.warn("server not started or already shutdown");
            return;
        }
        
        try {
            // 关闭服务器通道
            if (serverChannel != null) {
                serverChannel.close().sync();
            }
            
            logger.info("Netty server shutdown completed");
            
        } catch (Exception e) {
            logger.error("failed to shutdown Netty server", e);
        } finally {
            // 优雅关闭线程组
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
    
    /**
     * 等待服务器关闭
     */
    public void waitForShutdown() throws InterruptedException {
        if (serverChannel != null) {
            serverChannel.closeFuture().sync();
        }
    }
    
    /**
     * 检查服务器是否已启动
     */
    public boolean isStarted() {
        return started.get();
    }
    
    /**
     * 获取监听端口
     */
    public int getPort() {
        return port;
    }
}
