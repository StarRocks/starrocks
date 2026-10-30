// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.mysql;

import com.github.luben.zstd.Zstd;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * MySQL compressed protocol, used after the handshake when the client asked for zlib or zstd.
 * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_compression_packet.html
 *
 * <p>Every compressed packet starts with a 7-byte header: the 3-byte payload length, a 1-byte compressed
 * sequence id and the 3-byte length of the payload before compression, which is 0 when the payload is sent
 * as is. The payload carries ordinary MySQL packets, which may span compressed packets.
 */
public final class MysqlCompression {
    public enum Algorithm {
        ZLIB,
        ZSTD
    }

    public static final int HEADER_LEN = 7;
    // Payloads shorter than this are sent uncompressed, like the MySQL server does.
    static final int MIN_COMPRESS_LENGTH = 50;
    // Result sets are large, so favour FE CPU over the last few percent of ratio (MySQL itself uses level 6).
    static final int ZLIB_LEVEL = Deflater.BEST_SPEED;
    static final int DEFAULT_ZSTD_LEVEL = 3;
    // The client picks the zstd level (1-22). Higher levels cost a lot of FE CPU for little gain on result sets.
    static final int MAX_ZSTD_LEVEL = 9;

    private final Algorithm algorithm;
    private final int zstdLevel;

    MysqlCompression(Algorithm algorithm, int zstdLevel) {
        this.algorithm = algorithm;
        this.zstdLevel = zstdLevel;
    }

    /**
     * Picks the algorithm from the client's handshake response, or returns null if the client did not ask for
     * compression. Like libmysqlclient, zlib wins when the client sets both flags.
     */
    public static MysqlCompression negotiate(MysqlAuthPacket authPacket) {
        MysqlCapability client = authPacket.getCapability();
        if (client.isCompress()) {
            return new MysqlCompression(Algorithm.ZLIB, 0);
        }
        if (client.isZstdCompress()) {
            int level = authPacket.getZstdCompressionLevel();
            level = level <= 0 ? DEFAULT_ZSTD_LEVEL : Math.min(level, MAX_ZSTD_LEVEL);
            return new MysqlCompression(Algorithm.ZSTD, level);
        }
        return null;
    }

    public Algorithm getAlgorithm() {
        return algorithm;
    }

    public int getZstdLevel() {
        return zstdLevel;
    }

    /**
     * Wraps {@code len} bytes of {@code src} (at most {@link MysqlChannel#MAX_PHYSICAL_PACKET_LENGTH}) into one
     * compressed packet, sending the bytes as is when they are short or do not shrink.
     */
    public ByteBuffer encodePacket(byte[] src, int off, int len, int sequenceId) {
        byte[] compressed = len < MIN_COMPRESS_LENGTH ? null : compress(src, off, len);
        byte[] payload = compressed != null ? compressed : src;
        int payloadOff = compressed != null ? 0 : off;
        int payloadLen = compressed != null ? compressed.length : len;

        ByteBuffer packet = ByteBuffer.allocate(HEADER_LEN + payloadLen);
        writeInt3(packet, payloadLen);
        packet.put((byte) sequenceId);
        writeInt3(packet, compressed != null ? len : 0);
        packet.put(payload, payloadOff, payloadLen);
        packet.flip();
        return packet;
    }

    /** Returns the original bytes of a compressed packet's payload. */
    public byte[] decodePayload(byte[] payload, int uncompressedLen) throws IOException {
        if (uncompressedLen == 0) {
            return payload;
        }
        byte[] out = new byte[uncompressedLen];
        if (algorithm == Algorithm.ZSTD) {
            long n = Zstd.decompressByteArray(out, 0, out.length, payload, 0, payload.length);
            if (Zstd.isError(n) || n != uncompressedLen) {
                throw new IOException("Bad zstd compressed MySQL packet: " +
                        (Zstd.isError(n) ? Zstd.getErrorName(n) : n + " bytes, expected " + uncompressedLen));
            }
            return out;
        }
        Inflater inflater = new Inflater();
        try {
            inflater.setInput(payload);
            int n = 0;
            while (n < out.length && !inflater.finished()) {
                int got = inflater.inflate(out, n, out.length - n);
                if (got == 0 && (inflater.needsInput() || inflater.needsDictionary())) {
                    break;
                }
                n += got;
            }
            if (n != uncompressedLen) {
                throw new IOException("Bad zlib compressed MySQL packet: " + n + " bytes, expected " + uncompressedLen);
            }
            return out;
        } catch (DataFormatException e) {
            throw new IOException("Bad zlib compressed MySQL packet", e);
        } finally {
            inflater.end();
        }
    }

    // Returns null when the compressed form is not smaller than the input.
    private byte[] compress(byte[] src, int off, int len) {
        if (algorithm == Algorithm.ZSTD) {
            byte[] out = new byte[(int) Zstd.compressBound(len)];
            long n = Zstd.compressByteArray(out, 0, out.length, src, off, len, zstdLevel);
            if (Zstd.isError(n) || n >= len) {
                return null;
            }
            byte[] result = new byte[(int) n];
            System.arraycopy(out, 0, result, 0, (int) n);
            return result;
        }
        Deflater deflater = new Deflater(ZLIB_LEVEL);
        try {
            deflater.setInput(src, off, len);
            deflater.finish();
            byte[] out = new byte[len];
            int n = 0;
            while (!deflater.finished() && n < out.length) {
                n += deflater.deflate(out, n, out.length - n);
            }
            if (!deflater.finished()) {
                return null;
            }
            byte[] result = new byte[n];
            System.arraycopy(out, 0, result, 0, n);
            return result;
        } finally {
            deflater.end();
        }
    }

    static int readInt3(ByteBuffer buffer) {
        return (buffer.get() & 0xFF) | ((buffer.get() & 0xFF) << 8) | ((buffer.get() & 0xFF) << 16);
    }

    private static void writeInt3(ByteBuffer buffer, int value) {
        buffer.put((byte) value);
        buffer.put((byte) (value >> 8));
        buffer.put((byte) (value >> 16));
    }
}
