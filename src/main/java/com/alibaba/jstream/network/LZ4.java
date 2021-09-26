/*
 * Copyright (C) Alibaba Cloud Computing All rights reserved.
 */

package com.alibaba.jstream.network;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.nio.ByteBuffer;

import static java.lang.String.format;

public class LZ4 {
    public static byte[] compress(byte[] data) {
        final int rawSize = data.length;
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4Compressor compressor = factory.fastCompressor();
        int maxCompressedLength = compressor.maxCompressedLength(rawSize);
        byte[] rawCompressed = new byte[maxCompressedLength];
        int compressedSize = compressor.compress(data, 0, rawSize, rawCompressed, 0, maxCompressedLength);
        if (compressedSize <= 0) {
            throw new RuntimeException(format("lz4 compress failed, compressedSize: %d", compressedSize));
        }
        byte[] ret = new byte[compressedSize];
        System.arraycopy(rawCompressed, 0, ret, 0, compressedSize);

        return ret;
    }

    public static byte[] decompress(byte[] compressedData, int restoredSize) {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4FastDecompressor decompressor = factory.fastDecompressor();
        byte[] restored = new byte[restoredSize];
        decompressor.decompress(compressedData, 0, restored, 0, restoredSize);
        return restored;
    }

    public static ByteBuffer decompress(ByteBuffer compressedData, int restoredSize) {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4FastDecompressor decompressor = factory.fastDecompressor();
        ByteBuffer restored = ByteBuffer.allocate(restoredSize);
        decompressor.decompress(compressedData, 0, restored, 0, restoredSize);
        return restored;
    }
}
