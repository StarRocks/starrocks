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

package com.starrocks.common.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;

public class OffHeapDeflateTest {
    private static final int MB = 1024 * 1024;

    private static List<byte[]> payloads() {
        List<byte[]> payloads = new ArrayList<>();
        payloads.add(new byte[0]);
        payloads.add(new byte[] {42});
        StringBuilder profile = new StringBuilder();
        for (int i = 0; profile.length() < 3 * MB; i++) {
            profile.append("  - OperatorTotalTime: ").append(i % 997).append("ms\n");
        }
        payloads.add(profile.toString().getBytes(StandardCharsets.UTF_8));
        // Random bytes do not compress: the output must still fit the bound.
        byte[] random = new byte[MB + 7];
        new Random(42).nextBytes(random);
        payloads.add(random);
        return payloads;
    }

    private static byte[] readAll(InputStream in) throws IOException {
        try (InputStream stream = in) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            for (int n; (n = stream.read(buffer)) > 0; ) {
                out.write(buffer, 0, n);
            }
            return out.toByteArray();
        }
    }

    @Test
    public void testGzipIsReadBackByGzipInputStream() throws IOException {
        for (byte[] payload : payloads()) {
            byte[] compressed = OffHeapDeflate.gzip(payload);
            Assertions.assertArrayEquals(payload, readAll(new GZIPInputStream(new ByteArrayInputStream(compressed))),
                    "payload of " + payload.length + " bytes");
        }
    }

    @Test
    public void testGzipHeaderMatchesGzipOutputStream() throws IOException {
        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(expected)) {
            gzip.write("profile".getBytes(StandardCharsets.UTF_8));
        }
        byte[] actual = OffHeapDeflate.gzip("profile".getBytes(StandardCharsets.UTF_8));
        Assertions.assertArrayEquals(Arrays.copyOf(expected.toByteArray(), 10), Arrays.copyOf(actual, 10));
    }

    @Test
    public void testOutputIsByteForByteWhatTheJdkStreamsWrite() throws IOException {
        // Streaming the payload chunk by chunk through one Deflater must not cost any compression: the output has to
        // be exactly what compressing it in one go produces, for payloads larger than a chunk too.
        for (byte[] payload : payloads()) {
            ByteArrayOutputStream gzip = new ByteArrayOutputStream();
            try (GZIPOutputStream out = new GZIPOutputStream(gzip)) {
                out.write(payload);
            }
            Assertions.assertArrayEquals(gzip.toByteArray(), OffHeapDeflate.gzip(payload),
                    "gzip of " + payload.length + " bytes");

            ByteArrayOutputStream zlib = new ByteArrayOutputStream();
            try (DeflaterOutputStream out = new DeflaterOutputStream(zlib)) {
                out.write(payload);
            }
            Assertions.assertArrayEquals(zlib.toByteArray(), OffHeapDeflate.zlib(payload),
                    "zlib of " + payload.length + " bytes");
        }
    }

    @Test
    public void testZlibIsReadBackByInflaterInputStream() throws IOException {
        for (byte[] payload : payloads()) {
            byte[] compressed = OffHeapDeflate.zlib(payload);
            Assertions.assertArrayEquals(payload, readAll(new InflaterInputStream(new ByteArrayInputStream(compressed))),
                    "payload of " + payload.length + " bytes");
        }
    }

    @Test
    public void testFallsBackOnHeapWhenThePoolIsFull() throws IOException {
        // Room for one chunk buffer only, so every payload is compressed from the heap; the output must not differ.
        DirectBufferPool tiny = new DirectBufferPool(() -> OffHeapDeflate.CHUNK_SIZE);
        DirectBufferPool roomy = new DirectBufferPool(() -> MB);
        for (boolean gzip : new boolean[] {true, false}) {
            for (byte[] payload : payloads()) {
                for (DirectBufferPool pool : new DirectBufferPool[] {tiny, roomy}) {
                    long before = OffHeapDeflate.onHeapFallbacks();
                    byte[] compressed = OffHeapDeflate.deflate(pool, payload, gzip);
                    InputStream in = new ByteArrayInputStream(compressed);
                    Assertions.assertArrayEquals(payload,
                            readAll(gzip ? new GZIPInputStream(in) : new InflaterInputStream(in)));
                    Assertions.assertEquals(pool == tiny ? 1 : 0, OffHeapDeflate.onHeapFallbacks() - before,
                            "payload of " + payload.length + " bytes");
                }
            }
        }
        // The one buffer the tiny pool could hand out went back.
        Assertions.assertEquals(OffHeapDeflate.CHUNK_SIZE, tiny.allocatedBytes());
    }

    @Test
    public void testPayloadsLargerThanAChunkAreStreamed() throws IOException {
        // Several times the chunk size, in a pool that only has room for the two chunk buffers.
        DirectBufferPool pool = new DirectBufferPool(() -> 2L * OffHeapDeflate.CHUNK_SIZE);
        byte[] payload = payloads().get(2);
        Assertions.assertTrue(payload.length > 10 * OffHeapDeflate.CHUNK_SIZE);
        long before = OffHeapDeflate.onHeapFallbacks();
        byte[] compressed = OffHeapDeflate.deflate(pool, payload, true);
        Assertions.assertEquals(before, OffHeapDeflate.onHeapFallbacks());
        Assertions.assertArrayEquals(payload, readAll(new GZIPInputStream(new ByteArrayInputStream(compressed))));
        // Same bytes as compressing from the heap: the gzip trailer's CRC32 and length are computed alike.
        Assertions.assertArrayEquals(compressed,
                OffHeapDeflate.deflate(new DirectBufferPool(() -> 0L), payload, true));
    }

    @Test
    public void testZeroBoundTurnsOffHeapCompressionOff() throws IOException {
        AtomicLong bound = new AtomicLong(64L * MB);
        DirectBufferPool pool = new DirectBufferPool(bound::get);
        byte[] payload = payloads().get(2);
        byte[] expected = OffHeapDeflate.deflate(pool, payload, true);
        Assertions.assertEquals(2L * OffHeapDeflate.CHUNK_SIZE, pool.allocatedBytes());

        // Turned off at runtime: same output, from the heap, not counted as a fallback, and the idle buffers go.
        bound.set(0);
        long before = OffHeapDeflate.onHeapFallbacks();
        Assertions.assertArrayEquals(expected, OffHeapDeflate.deflate(pool, payload, true));
        Assertions.assertEquals(before, OffHeapDeflate.onHeapFallbacks());
        Assertions.assertEquals(0, pool.allocatedBytes());

        // And back on.
        bound.set(64L * MB);
        Assertions.assertArrayEquals(expected, OffHeapDeflate.deflate(pool, payload, true));
        Assertions.assertEquals(2L * OffHeapDeflate.CHUNK_SIZE, pool.allocatedBytes());
    }

    @Test
    public void testBuffersGoBackToThePool() throws IOException {
        DirectBufferPool pool = new DirectBufferPool(() -> 64L * MB);
        byte[] payload = payloads().get(2);
        for (int i = 0; i < 10; i++) {
            OffHeapDeflate.deflate(pool, payload, true);
            // Two chunk buffers per compression, whatever the payload size, reused every time.
            Assertions.assertEquals(2L * OffHeapDeflate.CHUNK_SIZE, pool.allocatedBytes());
        }
    }
}
