// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common.util;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.starrocks.thrift.TCompressionType;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressionUtils {
    private static final ImmutableMap<String, TCompressionType> T_COMPRESSION_BY_NAME =
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
        return T_COMPRESSION_BY_NAME.get(name);
    }

    // Return TCompressionType according to input name for some specified compression types.
    // Return null if input name is an invalid compression type.
    public static TCompressionType getCompressTypeByName(String name) {
        TCompressionType compressionType = T_COMPRESSION_BY_NAME.get(name);

        // Only lz4, zlib, zstd, snappy is available.
        if (compressionType == null) {
            return null;
        } else if (compressionType == TCompressionType.LZ4
                   || compressionType == TCompressionType.LZ4_FRAME
                   || compressionType == TCompressionType.ZLIB
                   || compressionType == TCompressionType.ZSTD
                   || compressionType == TCompressionType.SNAPPY) {
            return compressionType;
        } else {
            return null;
        }
    }

    /**
     * Compress the string with gzip format.
     *
     * @param origStr the original string to be compressed
     * @return the compressed data in byte array
     * @throws IOException
     */
    public static byte[] gzipCompressString(String origStr) throws IOException {
        if (Strings.isNullOrEmpty(origStr)) {
            return null;
        }

        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(outStream);
        gzip.write(origStr.getBytes());
        gzip.flush();
        gzip.close();
        return outStream.toByteArray();
    }


    /**
     * Decompress the string in gzip format
     *
     * @param compressedStr the compressed data in byte array
     * @return the original string
     * @throws IOException
     */
    public static String gzipDecompressString(byte[] compressedStr) throws IOException {
        if (compressedStr == null || compressedStr.length == 0) {
            return "";
        }

        final StringBuilder outStr = new StringBuilder();

        if (isGzipCompressed(compressedStr)) {
            GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(compressedStr));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(gis, "UTF-8"));

            int readLen;
            final int BUF_SIZE = 1024;
            char[] cbuf = new char[BUF_SIZE];
            while ((readLen = bufferedReader.read(cbuf, 0, BUF_SIZE)) != -1) {
                if (readLen == BUF_SIZE) {
                    outStr.append(cbuf);
                } else {
                    outStr.append(Arrays.copyOfRange(cbuf, 0, readLen));
                }
            }
        } else {
            outStr.append(compressedStr);
        }
        return outStr.toString();
    }

    public static boolean isGzipCompressed(byte[] compressedStr) {
        return (compressedStr[0] == (byte) (GZIPInputStream.GZIP_MAGIC)) &&
                (compressedStr[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8));
    }
}
