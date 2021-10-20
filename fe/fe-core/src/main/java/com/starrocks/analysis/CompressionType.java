// This file is made available under Elastic License 2.0.

package com.starrocks.analysis;

import com.starrocks.thrift.TCompressionType;

public enum CompressionType {
    UNKNOWN_COMPRESSION,
    DEFAULT_COMPRESSION,
    NO_COMPRESSION,
    SNAPPY,
    LZ4,
    LZ4_FRAME,
    ZLIB,
    ZSTD;

    public TCompressionType toThrift() {
        switch (this) {
            case UNKNOWN_COMPRESSION:
                return TCompressionType.UNKNOWN_COMPRESSION;
            case NO_COMPRESSION:
                return TCompressionType.NO_COMPRESSION;
            case SNAPPY:
                return TCompressionType.SNAPPY;
            case LZ4:
                return TCompressionType.LZ4;
            case LZ4_FRAME:
                return TCompressionType.LZ4_FRAME;
            case ZLIB:
                return TCompressionType.ZLIB;
            case ZSTD:
                return TCompressionType.ZSTD;
            default:
                return null;
        }
    }
}
