package com.alibaba.jstream.network.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;

import java.security.cert.CertificateException;

import static java.util.Objects.requireNonNull;

public class Server {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private final boolean isSSL;
    private final String host;
    private final int port;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;

    public Server(boolean isSSL, String host, int port, int bossThreads, int workerThreads) {
        this.isSSL = isSSL;
        this.host = requireNonNull(host);
        this.port = port;
        bossGroup = new NioEventLoopGroup(bossThreads);
        workerGroup = new NioEventLoopGroup(workerThreads);
    }

    public void start() throws CertificateException, InterruptedException, SSLException {
        final SslContext sslCtx;
        if (isSSL) {
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
        } else {
            sslCtx = null;
        }

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            if (sslCtx != null) {
                                p.addLast(sslCtx.newHandler(ch.alloc()));
                            }
                            p.addLast(
                                    new ResponseEncoder(),
                                    new RequestDecoder(),
                                    new ServerHandler());
                        }
                    });

            b.bind(host, port).sync().channel().closeFuture().sync();
        } finally {
            close();
        }
    }

    public void close() {
        if (!bossGroup.isShuttingDown() && !bossGroup.isShutdown() && !bossGroup.isTerminated()) {
            bossGroup.shutdownGracefully();
        }
        if (!workerGroup.isShuttingDown() && !workerGroup.isShutdown() && !workerGroup.isTerminated()) {
            workerGroup.shutdownGracefully();
        }
    }
}
