package com.alibaba.jstream.network.client;

import com.alibaba.jstream.exception.UnknownCommandException;
import com.alibaba.jstream.table.Table;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.alibaba.jstream.network.Command.REHASH;
import static com.alibaba.jstream.network.Command.REHASH_FINISHED;
import static com.alibaba.jstream.network.LZ4.compress;

public class RequestEncoder extends MessageToByteEncoder<List<Object>> {
    public static final int LENGTH_FIELD_LENGTH = 4;
    private static final byte[] LENGTH_PLACEHOLDER = new byte[LENGTH_FIELD_LENGTH];

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, List<Object> objects, ByteBuf byteBuf) {
        if (null == objects || objects.isEmpty()) {
            throw new IllegalArgumentException();
        }

        byteBuf.writeBytes(LENGTH_PLACEHOLDER);
        int startIdx = byteBuf.writerIndex();

        String cmd = (String) objects.get(0);
        writeString(cmd, byteBuf);
        if (cmd.equals(REHASH)) {
            writeString((String) objects.get(1), byteBuf);

            int thread = (int) objects.get(2);
            byteBuf.writeInt(thread);

            Table table = (Table) objects.get(3);
            byte[] bytes = table.serialize();
            byteBuf.writeInt(bytes.length);
            bytes = compress(bytes);
            byteBuf.writeBytes(bytes);
        } else if (cmd.equals(REHASH_FINISHED)) {
            writeString((String) objects.get(1), byteBuf);

            int server = (int) objects.get(2);
            byteBuf.writeInt(server);
        } else {
            throw new UnknownCommandException(cmd);
        }

        int endIdx = byteBuf.writerIndex();
        byteBuf.setInt(startIdx - LENGTH_FIELD_LENGTH, endIdx - startIdx);
    }

    private void writeString(String string, ByteBuf byteBuf) {
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        byteBuf.writeInt(bytes.length);
        byteBuf.writeBytes(bytes);
    }
}
