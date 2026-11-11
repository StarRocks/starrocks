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

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.common.Config;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;
import java.util.zip.Deflater;

/**
 * gzip and zlib compression that does not hold the JVM's GC locker.
 *
 * <p>java.util.zip.Deflater pins every byte[] it reads or writes inside a JNI critical section for the whole native
 * call. While any thread is in one, the collector cannot run: G1 lets an allocation wait for it only a few times
 * (GCLockerRetryAllocationCount) and then throws OutOfMemoryError although a collection would have freed plenty, and
 * ZGC fails allocations that waited through a whole delayed cycle. Compressing large payloads such as query profiles
 * from many threads in one call each keeps the locker held most of the time.
 *
 * <p>A Deflater that reads from and writes to direct buffers works on their addresses and enters no critical section.
 * The payload is therefore streamed through two pooled direct buffers of {@link #CHUNK_SIZE} each, whatever its size,
 * so the pool, bounded by {@link Config#offheap_deflate_buffer_pool_max_bytes}, serves that many compressions at once
 * (256 at the default 128MB). When it cannot, the payload is compressed from the heap instead, in chunks of
 * {@link #ON_HEAP_CHUNK_SIZE} so that each critical section stays short; setting the bound to 0 turns the off-heap
 * path off and compresses everything this way. Either way the output is the format
 * GZIPOutputStream and DeflaterOutputStream produce, so readers need no change.
 */
public final class OffHeapDeflate {
    @VisibleForTesting
    static final int CHUNK_SIZE = 256 * 1024;
    @VisibleForTesting
    static final int ON_HEAP_CHUNK_SIZE = 64 * 1024;

    // What GZIPOutputStream writes: magic, CM = deflate, no flags, no mtime, no extra flags, OS 255 (unknown).
    private static final byte[] GZIP_HEADER = {0x1f, (byte) 0x8b, Deflater.DEFLATED, 0, 0, 0, 0, 0, 0, (byte) 0xff};
    private static final int GZIP_TRAILER_SIZE = 8;

    private static final DirectBufferPool POOL = new DirectBufferPool(() -> Config.offheap_deflate_buffer_pool_max_bytes);
    private static final AtomicLong ON_HEAP_FALLBACKS = new AtomicLong();

    private OffHeapDeflate() {
    }

    /** Compresses {@code input} into a gzip member, as GZIPOutputStream does. */
    public static byte[] gzip(byte[] input) {
        return deflate(POOL, input, true);
    }

    /** Compresses {@code input} into a zlib stream, as DeflaterOutputStream with a default Deflater does. */
    public static byte[] zlib(byte[] input) {
        return deflate(POOL, input, false);
    }

    /**
     * How many payloads were compressed from the heap because the pool had no buffers to spare. Payloads compressed
     * from the heap because off-heap compression is turned off are not counted.
     */
    public static long onHeapFallbacks() {
        return ON_HEAP_FALLBACKS.get();
    }

    @VisibleForTesting
    static byte[] deflate(DirectBufferPool pool, byte[] input, boolean gzip) {
        if (pool.isDisabled()) {
            // Off-heap compression is turned off: not a fallback, so not counted as one. Give back the buffers the pool
            // still keeps idle from before, which nothing would acquire again.
            pool.trim();
            return deflateOnHeap(input, gzip);
        }
        ByteBuffer in = pool.acquire(CHUNK_SIZE);
        ByteBuffer out = in == null ? null : pool.acquire(CHUNK_SIZE);
        if (out == null) {
            if (in != null) {
                pool.release(in);
            }
            ON_HEAP_FALLBACKS.incrementAndGet();
            return deflateOnHeap(input, gzip);
        }
        try {
            return deflateOffHeap(input, gzip, in, out);
        } finally {
            pool.release(in);
            pool.release(out);
        }
    }

    private static byte[] deflateOffHeap(byte[] input, boolean gzip, ByteBuffer in, ByteBuffer out) {
        Output result = new Output(input.length, gzip);
        CRC32 crc = gzip ? new CRC32() : null;
        // GZIPOutputStream writes the gzip header and trailer around a raw (nowrap) deflate stream itself.
        Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, gzip);
        try {
            int consumed = 0;
            if (input.length == 0) {
                deflater.finish();
            }
            while (!deflater.finished()) {
                if (deflater.needsInput() && consumed < input.length) {
                    int n = Math.min(in.capacity(), input.length - consumed);
                    in.clear();
                    in.put(input, consumed, n).flip();
                    if (crc != null) {
                        crc.update(in.duplicate());
                    }
                    deflater.setInput(in);
                    consumed += n;
                    if (consumed == input.length) {
                        deflater.finish();
                    }
                }
                out.clear();
                deflater.deflate(out);
                out.flip();
                result.write(out);
            }
        } finally {
            deflater.end();
        }
        return result.finish(crc, input.length);
    }

    /** The heap path: same output, in chunks so that no JNI critical section spans the whole payload. */
    private static byte[] deflateOnHeap(byte[] input, boolean gzip) {
        Output result = new Output(input.length, gzip);
        CRC32 crc = gzip ? new CRC32() : null;
        byte[] chunk = new byte[ON_HEAP_CHUNK_SIZE];
        Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, gzip);
        try {
            int consumed = 0;
            if (input.length == 0) {
                deflater.finish();
            }
            while (!deflater.finished()) {
                if (deflater.needsInput() && consumed < input.length) {
                    int n = Math.min(ON_HEAP_CHUNK_SIZE, input.length - consumed);
                    if (crc != null) {
                        crc.update(input, consumed, n);
                    }
                    deflater.setInput(input, consumed, n);
                    consumed += n;
                    if (consumed == input.length) {
                        deflater.finish();
                    }
                }
                int n = deflater.deflate(chunk);
                result.write(chunk, n);
            }
        } finally {
            deflater.end();
        }
        return result.finish(crc, input.length);
    }

    /** The compressed bytes, with the gzip header and trailer when asked for. */
    private static final class Output {
        private byte[] bytes;
        private int length;

        Output(int inputLength, boolean gzip) {
            bytes = new byte[Math.max(256, inputLength / 4)];
            if (gzip) {
                write(GZIP_HEADER, GZIP_HEADER.length);
            }
        }

        void write(ByteBuffer buffer) {
            int n = buffer.remaining();
            ensureCapacity(n);
            buffer.get(bytes, length, n);
            length += n;
        }

        void write(byte[] source, int n) {
            ensureCapacity(n);
            System.arraycopy(source, 0, bytes, length, n);
            length += n;
        }

        byte[] finish(CRC32 crc, int inputLength) {
            if (crc != null) {
                ensureCapacity(GZIP_TRAILER_SIZE);
                writeIntLE((int) crc.getValue());
                writeIntLE(inputLength);
            }
            return bytes.length == length ? bytes : Arrays.copyOf(bytes, length);
        }

        private void writeIntLE(int value) {
            bytes[length++] = (byte) value;
            bytes[length++] = (byte) (value >>> 8);
            bytes[length++] = (byte) (value >>> 16);
            bytes[length++] = (byte) (value >>> 24);
        }

        private void ensureCapacity(int extra) {
            if (length + extra > bytes.length) {
                bytes = Arrays.copyOf(bytes, Math.max(length + extra, bytes.length * 2));
            }
        }
    }
}
