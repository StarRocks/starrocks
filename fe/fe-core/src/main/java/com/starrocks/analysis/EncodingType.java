// This file is made available under Elastic License 2.0.

package com.starrocks.analysis;

import com.starrocks.thrift.TEncodingType;

public enum EncodingType {
    UNKNOWN_ENCODING,
    DEFAULT_ENCODING,
    PLAIN_ENCODING,
    PREFIX_ENCODING,
    RLE,
    DICT_ENCODING,
    BIT_SHUFFLE,
    FOR_ENCODING;

    public TEncodingType toThrift() {
        switch (this) {
            case UNKNOWN_ENCODING:
                return TEncodingType.UNKNOWN_ENCODING;
            case DEFAULT_ENCODING:
                return TEncodingType.DEFAULT_ENCODING;
            case PLAIN_ENCODING:
                return TEncodingType.PLAIN_ENCODING;
            case PREFIX_ENCODING:
                return TEncodingType.PREFIX_ENCODING;
            case RLE:
                return TEncodingType.RLE;
            case DICT_ENCODING:
                return TEncodingType.DICT_ENCODING;
            case BIT_SHUFFLE:
                return TEncodingType.BIT_SHUFFLE;
            case FOR_ENCODING:
                return TEncodingType.FOR_ENCODING;
            default:
                return null;
        }
    }
}