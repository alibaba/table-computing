package com.alibaba.jstream.sp.input;

import com.alibaba.jstream.offheap.ByteArray;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.util.VarintUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.jstream.offheap.InternalUnsafe.getInt;
import static com.alibaba.jstream.offheap.InternalUnsafe.getObject;
import static com.alibaba.jstream.offheap.InternalUnsafe.objectFieldOffset;

public class SlsParser {
    private static final Logger logger = LoggerFactory.getLogger(SlsParser.class);

    public interface Callback {
        void end(ByteArray category, ByteArray topic, ByteArray source, ByteArray machineUUID);

        void nextLog(int time);

        void keyValue(byte[] rawBytes, int keyOffset, int keyLength, int valueOffset, int valueLength);
    }

    private final byte[] rawBytes;
    private final int beginOffset;
    private final int endOffset;
    private final Callback callback;
    private int time;

    private int categoryOffset = -1;
    private int topicOffset = -1;
    private int sourceOffset = -1;
    private int machineUUIDOffset = -1;

    private static long offsetOffset;
    private static long lengthOffset;
    private static long rawBytesOffset;

    static {
        try {
            rawBytesOffset = objectFieldOffset(LogGroupData.class.getDeclaredField("rawBytes"));
            offsetOffset = objectFieldOffset(LogGroupData.class.getDeclaredField("offset"));
            lengthOffset = objectFieldOffset(LogGroupData.class.getDeclaredField("length"));
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    public SlsParser(LogGroupData logGroupData, Callback callback) {
        this.rawBytes = (byte[]) getObject(logGroupData, rawBytesOffset);
        this.beginOffset = getInt(logGroupData, offsetOffset);
        this.endOffset = this.beginOffset + getInt(logGroupData, lengthOffset);
        this.callback = callback;
        parse();
    }

    private boolean parse() {
        int pos = this.beginOffset;
        int mode, index;
        while (pos < this.endOffset) {
            int[] value = VarintUtil.DecodeVarInt32(this.rawBytes, pos, this.endOffset);
            if (value[0] == 0) {
                return false;
            }
            mode = value[1] & 0x7;
            index = value[1] >> 3;
            if (mode == 0) {
                pos = value[2];
                value = VarintUtil.DecodeVarInt32(this.rawBytes, pos, this.endOffset);
                if (value[0] == 0) {
                    return false;
                }
                pos = value[2];
            } else if (mode == 1) {
                pos = value[2] + 8;
            } else if (mode == 2) {
                switch (index) {
                    case 1:
                        //logs
                        break;
                    case 2:
                        this.categoryOffset = pos;
                        break;
                    case 3:
                        this.topicOffset = value[2];
                        break;
                    case 4:
                        this.sourceOffset = value[2];
                        break;
                    case 5:
                        this.machineUUIDOffset = value[2];
                        break;
                    case 6:
                        //tags
                        break;
                    default:
                }
                pos = value[2];
                value = VarintUtil.DecodeVarInt32(this.rawBytes, pos, this.endOffset);
                if (value[0] == 0) {
                    return false;
                }
                pos = value[2] + value[1];
                if (index == 1) {
                    parseLog(rawBytes, value[2], value[2] + value[1]);
                    callback.nextLog(time);
                } else if (index == 6) {
                    parseTag(rawBytes, value[2], value[2] + value[1]);
                }
            } else if (mode == 5) {
                pos = value[2] + 4;
            } else {
                return false;
            }
        }
        callback.end(getCategory(), getTopic(), getSource(), getMachineUUID());
        return (pos == this.endOffset);
    }

    private boolean parseTag(byte[] rawBytes, int beginOffset, int endOffset) {
        int keyOffset = -1, keyLength = 0, valueOffset = -1, valueLength = 0;
        int pos = beginOffset;
        int mode, index;
        while (pos < endOffset) {
            int[] value = VarintUtil.DecodeVarInt32(rawBytes, pos, endOffset);
            if (value[0] == 0) {
                return false;
            }
            pos = value[2];
            mode = value[1] & 0x7;
            index = value[1] >> 3;
            if (mode == 0) {
                value = VarintUtil.DecodeVarInt32(rawBytes, pos, endOffset);
                if (value[0] == 0) {
                    return false;
                }
                pos = value[2];
            } else if (mode == 1) {
                pos += 8;
            } else if (mode == 2) {
                value = VarintUtil.DecodeVarInt32(rawBytes, pos, endOffset);
                if (value[0] == 0) {
                    return false;
                }
                pos = value[2] + value[1];
                if (index == 1) {
                    keyOffset = value[2];
                    keyLength = value[1];
                } else if (index == 2) {
                    valueOffset = value[2];
                    valueLength = value[1];
                }
            } else if (mode == 5) {
                pos += 4;
            } else {
                return false;
            }
        }

        this.callback.keyValue(rawBytes, keyOffset, keyLength, valueOffset, valueLength);
        return (keyOffset != -1 && valueOffset != -1 && pos == endOffset);
    }

    private boolean parseLog(byte[] rawBytes, int beginOffset, int endOffset) {
        int pos = beginOffset;
        int mode, index;
        boolean findTime = false;
        while (pos < endOffset) {
            int[] value = VarintUtil.DecodeVarInt32(rawBytes, pos, endOffset);
            if (value[0] == 0) {
                return false;
            }
            mode = value[1] & 0x7;
            index = value[1] >> 3;
            if (mode == 0) {
                pos = value[2];
                value = VarintUtil.DecodeVarInt32(rawBytes, pos, endOffset);
                if (value[0] == 0) {
                    return false;
                }
                pos = value[2];
                if (index == 1) {
                    time = value[1];
                    findTime = true;
                }
            } else if (mode == 1) {
                pos = value[2] + 8;
            } else if (mode == 2) {
                pos = value[2];
                value = VarintUtil.DecodeVarInt32(rawBytes, pos, endOffset);
                if (value[0] == 0) {
                    return false;
                }
                pos = value[2] + value[1];
                if (index == 2) {
                    parseLogContent(rawBytes, value[2], value[2] + value[1]);
                }
            } else if (mode == 5) {
                pos = value[2] + 4;
            } else {
                return false;
            }
        }
        return findTime && (pos == this.endOffset);
    }

    private boolean parseLogContent(byte[] rawBytes, int beginOffset, int endOffset) {
        int keyOffset = -1, keyLength = 0, valueOffset = -1, valueLength = 0;
        int pos = beginOffset;
        int index, mode;
        while (pos < endOffset) {
            int[] value = VarintUtil.DecodeVarInt32(rawBytes, pos, endOffset);
            if (value[0] == 0) {
                return false;
            }
            mode = value[1] & 0x7;
            index = value[1] >> 3;
            pos = value[2];
            if (mode == 0) {
                value = VarintUtil.DecodeVarInt32(rawBytes, pos, endOffset);
                if (value[0] == 0) {
                    return false;
                }
                pos = value[2];
            } else if (mode == 1) {
                pos += 8;
            } else if (mode == 2) {
                value = VarintUtil.DecodeVarInt32(rawBytes, pos, endOffset);
                if (value[0] == 0) {
                    return false;
                }
                pos = value[2] + value[1];
                if (index == 1) {
                    keyOffset = value[2];
                    keyLength = value[1];
                } else if (index == 2) {
                    valueOffset = value[2];
                    valueLength = value[1];
                }
            } else if (mode == 5) {
                pos += 4;
            } else {
                return false;
            }
        }

        this.callback.keyValue(rawBytes, keyOffset, keyLength, valueOffset, valueLength);
        return (keyOffset != -1 && valueOffset != -1 && pos == this.endOffset);
    }

    private ByteArray getCategory() {
        if (this.categoryOffset < 0) {
            return null;
        }
        int[] value = VarintUtil.DecodeVarInt32(this.rawBytes, this.categoryOffset, this.endOffset);
        if (value[0] == 0) {
            return null;
        }
        int mode = value[1] & 0x7;
        int index = value[1] >> 3;
        if (mode != 2 && index != 2) {
            return null;
        }
        value = VarintUtil.DecodeVarInt32(this.rawBytes, value[2], this.endOffset);
        if (value[0] == 0) {
            return null;
        }
        return new ByteArray(this.rawBytes, value[2], value[1]);
    }

    private ByteArray getTopic() {
        if (this.topicOffset < 0) {
            return null;
        }
        int[] value = VarintUtil.DecodeVarInt32(this.rawBytes, this.topicOffset, this.endOffset);
        if (value[0] == 0) {
            return null;
        }
        return new ByteArray(this.rawBytes, value[2], value[1]);
    }

    private ByteArray getSource() {
        if (this.sourceOffset < 0) {
            return null;
        }
        int[] value = VarintUtil.DecodeVarInt32(this.rawBytes, this.sourceOffset, this.endOffset);
        if (value[0] == 0) {
            return null;
        }
        return new ByteArray(this.rawBytes, value[2], value[1]);
    }

    private ByteArray getMachineUUID() {
        if (this.machineUUIDOffset < 0) {
            return null;
        }
        int[] value = VarintUtil.DecodeVarInt32(this.rawBytes, this.machineUUIDOffset, this.endOffset);
        if (value[0] == 0) {
            return null;
        }
        return new ByteArray(this.rawBytes, value[2], value[1]);
    }
}
