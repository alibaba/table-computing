package com.alibaba.tc.network.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import static com.alibaba.tc.network.client.RequestEncoder.LENGTH_FIELD_LENGTH;

public class ResponseEncoder extends MessageToByteEncoder<Integer> {
    private static final byte[] LENGTH_PLACEHOLDER = new byte[LENGTH_FIELD_LENGTH];

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, Integer returnValue, ByteBuf byteBuf) throws Exception {
        int startIdx = byteBuf.writerIndex();
        byteBuf.writeBytes(LENGTH_PLACEHOLDER);
        byteBuf.writeInt(returnValue);
        int endIdx = byteBuf.writerIndex();
        byteBuf.setInt(startIdx, endIdx - startIdx - LENGTH_FIELD_LENGTH);
    }
}
