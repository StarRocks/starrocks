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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class MysqlCompressionTest {
    private static final int ZSTD = MysqlCapability.Flag.CLIENT_ZSTD_COMPRESSION_ALGORITHM.getFlagBit();
    private static final int ZLIB = MysqlCapability.Flag.CLIENT_COMPRESS.getFlagBit();

    @Test
    public void testNegotiate() {
        Assertions.assertNull(MysqlCompression.negotiate(authPacket(0, -1)));

        MysqlCompression zlib = MysqlCompression.negotiate(authPacket(ZLIB, -1));
        Assertions.assertEquals(MysqlCompression.Algorithm.ZLIB, zlib.getAlgorithm());
        // like libmysqlclient, zlib wins when the client sets both flags
        Assertions.assertEquals(MysqlCompression.Algorithm.ZLIB,
                MysqlCompression.negotiate(authPacket(ZLIB | ZSTD, 7)).getAlgorithm());

        MysqlCompression zstd = MysqlCompression.negotiate(authPacket(ZSTD, 7));
        Assertions.assertEquals(MysqlCompression.Algorithm.ZSTD, zstd.getAlgorithm());
        Assertions.assertEquals(7, zstd.getZstdLevel());
        Assertions.assertEquals(MysqlCompression.MAX_ZSTD_LEVEL,
                MysqlCompression.negotiate(authPacket(ZSTD, 22)).getZstdLevel());
        Assertions.assertEquals(MysqlCompression.DEFAULT_ZSTD_LEVEL,
                MysqlCompression.negotiate(authPacket(ZSTD, 0)).getZstdLevel());
    }

    @Test
    public void testAuthPacketReadsZstdLevelAfterConnectAttrs() {
        MysqlAuthPacket packet = authPacket(ZSTD | MysqlCapability.Flag.CLIENT_CONNECT_ATTRS.getFlagBit(), 5);
        Assertions.assertEquals("test_user", packet.getUser());
        Assertions.assertEquals("mysql", packet.getConnectAttributes().get("_client_name"));
        Assertions.assertEquals("9.7.1", packet.getConnectAttributes().get("_client_version"));
        Assertions.assertEquals(5, packet.getZstdCompressionLevel());
    }

    @Test
    public void testHandshakeOffersCompressionOnlyWhenAsked() {
        Assertions.assertEquals(0, handshakeFlags(false) & (ZLIB | ZSTD));
        Assertions.assertEquals(ZLIB | ZSTD, handshakeFlags(true) & (ZLIB | ZSTD));
    }

    @Test
    public void testEncodeDecode() throws IOException {
        for (MysqlCompression.Algorithm algorithm : MysqlCompression.Algorithm.values()) {
            MysqlCompression compression = new MysqlCompression(algorithm, 3);
            byte[] text = "1783198684001\t33.67444806120894\t-117.95815752704918\n".repeat(2000)
                    .getBytes(StandardCharsets.UTF_8);

            ByteBuffer packet = compression.encodePacket(text, 0, text.length, 9);
            int payloadLen = MysqlCompression.readInt3(packet);
            Assertions.assertEquals(9, packet.get() & 0xFF);
            Assertions.assertEquals(text.length, MysqlCompression.readInt3(packet));
            Assertions.assertTrue(payloadLen < text.length / 10, algorithm + " payload " + payloadLen);
            byte[] payload = new byte[payloadLen];
            packet.get(payload);
            Assertions.assertArrayEquals(text, compression.decodePayload(payload, text.length));

            // short and incompressible payloads go as is, with uncompressed length 0
            assertSentAsIs(compression, "SELECT 1".getBytes(StandardCharsets.UTF_8));
            byte[] random = new byte[4096];
            new Random(42).nextBytes(random);
            assertSentAsIs(compression, random);
        }
    }

    @Test
    public void testChannelRoundTrip() throws IOException {
        for (MysqlCompression.Algorithm algorithm : MysqlCompression.Algorithm.values()) {
            MysqlCompression compression = new MysqlCompression(algorithm, 3);
            byte[] query = "SELECT * FROM t WHERE c = 'abcdefghijklmnopqrstuvwxyz0123456789abcdefghij'"
                    .getBytes(StandardCharsets.UTF_8);
            // the client's command: MySQL packet with sequence id 0 inside compressed packet 0
            ByteBuffer request = compression.encodePacket(mysqlPacket(0, query), 0, query.length + 4, 0);
            CapturingChannel channel = new CapturingChannel(request);
            channel.setNegotiatedCompression(compression);
            // not used before startCompression(), e.g. for the auth OK
            Assertions.assertNull(channel.getCompression());
            channel.startCompression();

            channel.setSequenceId(0);
            ByteBuffer read = channel.fetchOnePacket();
            byte[] readBytes = new byte[read.remaining()];
            read.get(readBytes);
            Assertions.assertArrayEquals(query, readBytes);

            // answer with rows larger than the send buffer, so several compressed packets go out
            List<byte[]> rows = new ArrayList<>();
            for (int i = 0; i < 20000; i++) {
                rows.add(("row " + i + "\t33.67444806120894\t-117.95815752704918").getBytes(StandardCharsets.UTF_8));
            }
            for (byte[] row : rows) {
                channel.sendOnePacket(ByteBuffer.wrap(row));
            }
            channel.flush();

            // the response continues the client's compressed sequence: 1, 2, ...
            MysqlCompressedPacketDecoder decoder = new MysqlCompressedPacketDecoder(compression);
            decoder.consume(ByteBuffer.wrap(channel.sent.toByteArray()));
            MysqlPackageDecoder packageDecoder = new MysqlPackageDecoder(false);
            int expectedSeq = 1;
            MysqlCompressedPacketDecoder.Packet packet;
            while ((packet = decoder.poll()) != null) {
                Assertions.assertEquals(expectedSeq++, packet.sequenceId());
                packageDecoder.consume(packet.payload());
            }
            Assertions.assertTrue(expectedSeq > 2, "expected several compressed packets");
            Assertions.assertTrue(channel.sent.size() < rows.size() * 20, "response was not compressed");
            for (byte[] row : rows) {
                RequestPackage pkg = packageDecoder.poll();
                byte[] got = new byte[pkg.byteBuffer().remaining()];
                pkg.byteBuffer().get(got);
                Assertions.assertArrayEquals(row, got);
            }
            Assertions.assertNull(packageDecoder.poll());
        }
    }

    @Test
    public void testDecoderHandlesSplitInput() throws IOException {
        MysqlCompression compression = new MysqlCompression(MysqlCompression.Algorithm.ZSTD, 3);
        byte[] first = "x".repeat(1000).getBytes(StandardCharsets.UTF_8);
        byte[] second = "SELECT 1".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(toArray(compression.encodePacket(first, 0, first.length, 0)));
        stream.write(toArray(compression.encodePacket(second, 0, second.length, 1)));

        MysqlCompressedPacketDecoder decoder = new MysqlCompressedPacketDecoder(compression);
        for (byte b : stream.toByteArray()) {
            decoder.consume(ByteBuffer.wrap(new byte[] {b}));
        }
        MysqlCompressedPacketDecoder.Packet packet = decoder.poll();
        Assertions.assertEquals(0, packet.sequenceId());
        Assertions.assertArrayEquals(first, toArray(packet.payload()));
        packet = decoder.poll();
        Assertions.assertEquals(1, packet.sequenceId());
        Assertions.assertArrayEquals(second, toArray(packet.payload()));
        Assertions.assertNull(decoder.poll());
    }

    private static void assertSentAsIs(MysqlCompression compression, byte[] data) throws IOException {
        ByteBuffer packet = compression.encodePacket(data, 0, data.length, 0);
        Assertions.assertEquals(data.length, MysqlCompression.readInt3(packet));
        packet.get();
        Assertions.assertEquals(0, MysqlCompression.readInt3(packet));
        byte[] payload = toArray(packet);
        Assertions.assertArrayEquals(data, payload);
        Assertions.assertArrayEquals(data, compression.decodePayload(payload, 0));
    }

    private static byte[] mysqlPacket(int seq, byte[] payload) {
        byte[] packet = new byte[payload.length + 4];
        packet[0] = (byte) payload.length;
        packet[1] = (byte) (payload.length >> 8);
        packet[2] = (byte) (payload.length >> 16);
        packet[3] = (byte) seq;
        System.arraycopy(payload, 0, packet, 4, payload.length);
        return packet;
    }

    private static byte[] toArray(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static int handshakeFlags(boolean supportCompression) {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        new MysqlHandshakePacket(1, false, new byte[20], supportCompression).writeTo(serializer);
        ByteBuffer buffer = serializer.toByteBuffer();
        // protocol version, server version, connection id, 8 bytes of auth data, filler
        buffer.get();
        while (buffer.get() != 0) {
            // skip the server version
        }
        buffer.position(buffer.position() + 4 + 8 + 1);
        int lower = MysqlCodec.readInt2(buffer);
        // character set, status flags
        buffer.position(buffer.position() + 1 + 2);
        int upper = MysqlCodec.readInt2(buffer);
        return lower | (upper << 16);
    }

    // zstdLevel < 0: no level byte
    private static MysqlAuthPacket authPacket(int extraFlags, int zstdLevel) {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        int flags = MysqlCapability.DEFAULT_CAPABILITY.getFlags() & ~MysqlCapability.Flag.CLIENT_CONNECT_ATTRS.getFlagBit();
        serializer.writeInt4(flags | extraFlags);
        serializer.writeInt4(1024000);
        serializer.writeInt1(33);
        serializer.writeBytes(new byte[23]);
        serializer.writeNulTerminateString("test_user");
        // auth response, length encoded
        serializer.writeInt1(0);
        serializer.writeNulTerminateString("");
        serializer.writeNulTerminateString("mysql_native_password");
        if ((extraFlags & MysqlCapability.Flag.CLIENT_CONNECT_ATTRS.getFlagBit()) != 0) {
            MysqlSerializer attrs = MysqlSerializer.newInstance();
            for (String kv : Arrays.asList("_client_name", "mysql", "_client_version", "9.7.1")) {
                attrs.writeLenEncodedString(kv);
            }
            byte[] attrBytes = toArray(attrs.toByteBuffer());
            serializer.writeVInt(attrBytes.length);
            serializer.writeBytes(attrBytes);
        }
        if (zstdLevel >= 0) {
            serializer.writeInt1(zstdLevel);
        }
        MysqlAuthPacket packet = new MysqlAuthPacket();
        Assertions.assertTrue(packet.readFrom(serializer.toByteBuffer()));
        return packet;
    }

    // MysqlChannel without a socket: reads come from a buffer, writes are captured
    private static class CapturingChannel extends MysqlChannel {
        final ByteArrayOutputStream sent = new ByteArrayOutputStream();
        private final ByteBuffer input;

        CapturingChannel(ByteBuffer input) {
            super(null);
            this.input = input;
        }

        @Override
        public int realNetRead(ByteBuffer dstBuf) {
            if (!input.hasRemaining()) {
                return -1;
            }
            int n = Math.min(dstBuf.remaining(), input.remaining());
            ByteBuffer slice = input.duplicate();
            slice.limit(slice.position() + n);
            dstBuf.put(slice);
            input.position(input.position() + n);
            return n;
        }

        @Override
        public void realNetSend(ByteBuffer buffer) {
            sent.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
            buffer.position(buffer.limit());
        }
    }
}
