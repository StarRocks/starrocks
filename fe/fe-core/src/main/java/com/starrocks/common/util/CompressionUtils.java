// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.starrocks.thrift.TCompressionType;

public class CompressionUtils {
    private static final ImmutableMap<String, TCompressionType> tCompressionByName =
            (new ImmutableSortedMap.Builder<String, TCompressionType>(String.CASE_INSENSITIVE_ORDER))
                    .put("NO_COMPRESSION", TCompressionType.NO_COMPRESSION)
                    .put("LZ4", TCompressionType.LZ4)
                    .put("LZ4_FRAME", TCompressionType.LZ4_FRAME)
                    .put("SNAPPY", TCompressionType.SNAPPY)
                    .put("ZLIB", TCompressionType.ZLIB)
                    .put("ZSTD", TCompressionType.ZSTD)
                    .put("GZIP", TCompressionType.GZIP)
                    .put("DEFLATE", TCompressionType.DEFLATE)
                    .put("BZIP2", TCompressionType.BZIP2)
                    .build();

    // Return TCompressionType according to input name.
    // Return null if input name is an invalid compression type.
    public static TCompressionType findTCompressionByName(String name) {
        return tCompressionByName.get(name);
    }

    // Return TCompressionType according to input name for some specified compression types.
    // Return null if input name is an invalid compression type.
    public static TCompressionType getCompressTypeByName(String name) {
        TCompressionType compressionType = tCompressionByName.get(name);

        // Only lz4, zlib, zstd is available.
        if (compressionType == null) {
            return null;
        } else if (compressionType == TCompressionType.LZ4
                   || compressionType == TCompressionType.LZ4_FRAME
                   || compressionType == TCompressionType.ZLIB
                   || compressionType == TCompressionType.ZSTD) {
            return compressionType;
        } else {
            return null;
        }
    }
}
