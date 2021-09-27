package com.alibaba.tc.network.client;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import static com.alibaba.tc.network.client.RequestEncoder.LENGTH_FIELD_LENGTH;

public class ResponseDecoder extends LengthFieldBasedFrameDecoder {
    public ResponseDecoder() {
        super(Integer.MAX_VALUE, 0, LENGTH_FIELD_LENGTH, 0, LENGTH_FIELD_LENGTH, true);
    }
}
