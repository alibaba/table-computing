package com.alibaba.tc.network.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import javax.net.ssl.SSLException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@NotThreadSafe
public class Client {
    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    private final SslContext sslCtx;
    private Channel channel;
    private final ClientHandler clientHandler;
    private final EventLoopGroup group;
    private volatile boolean closed = true;
    private final Duration requestTimeout;
    private final ReentrantLock reentrantLock = new ReentrantLock();
    private final Condition condition = reentrantLock.newCondition();
    private volatile Throwable throwable;

    private class ClientHandler extends ChannelInboundHandlerAdapter {
        private ByteBuf responseByteBuf;

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object object) {
            try {
                reentrantLock.lock();
                responseByteBuf = (ByteBuf) object;
                condition.signal();
            } finally {
                reentrantLock.unlock();
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("", cause);
            try {
                reentrantLock.lock();
                throwable = cause;
                condition.signal();
                ctx.close();
                close();
            } finally {
                reentrantLock.unlock();
            }
        }
    }

    public Client(boolean isSSL, final String host, final int port, Duration requestTimeout) throws SSLException, InterruptedException {
        this.requestTimeout = requestTimeout;
        this.clientHandler = new ClientHandler();

        if (isSSL) {
            sslCtx = SslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        } else {
            sslCtx = null;
        }

        group = new NioEventLoopGroup();
        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        if (sslCtx != null) {
                            p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
                        }

                        p.addLast(
                                new RequestEncoder(),
                                new ResponseDecoder(),
                                clientHandler);
                    }
                });

        channel = b.connect(host, port).sync().channel();
        closed = false;
    }

    public int request(String cmd, Object... args) throws InterruptedException {
        try {
            reentrantLock.lock();
            List<Object> objects = new ArrayList<>(args.length + 1);
            objects.add(cmd);
            for (int i = 0; i < args.length; i++) {
                objects.add(args[i]);
            }
            channel.writeAndFlush(objects);
            long nanos = condition.awaitNanos(requestTimeout.toNanos());
            if (nanos <= 0) {
                throw new RuntimeException("request timeout");
            }

            int ret = clientHandler.responseByteBuf.readInt();
            clientHandler.responseByteBuf.release();
            return ret;
        } finally {
            reentrantLock.unlock();
            if (null != throwable) {
                Throwable tmp = throwable;
                throwable = null;
                throw new RuntimeException(tmp);
            }
        }
    }

    public void close() {
        try {
            reentrantLock.lock();
            group.shutdownGracefully();
            closed = true;
        } finally {
            reentrantLock.unlock();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (!closed) {
            group.shutdownGracefully();
            logger.error("not closed before finalize");
        }
    }
}
