package com.alibaba.tc.network.server;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import static com.alibaba.tc.network.client.RequestEncoder.LENGTH_FIELD_LENGTH;

public class RequestDecoder extends LengthFieldBasedFrameDecoder {
    public RequestDecoder() {
        super(Integer.MAX_VALUE, 0, LENGTH_FIELD_LENGTH, 0, LENGTH_FIELD_LENGTH, true);
    }
}
