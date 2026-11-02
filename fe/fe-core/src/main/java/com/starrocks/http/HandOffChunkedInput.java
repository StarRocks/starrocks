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

package com.starrocks.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A streamed body produced by the thread that handles the request and sent by {@link ChunkedWriteHandler} from the
 * channel's event loop, so the event loop only moves buffers and never runs the work of producing them. Chunks are
 * handed over through a queue of a few chunks; the producer waits while it is full, which keeps the body to a
 * bounded number of chunks in heap and paces the producer by how fast the client reads. The producer stops as soon
 * as the transfer ends without it: the connection closed, or the channel gave the body up.
 */
public class HandOffChunkedInput implements ChunkedInput<ByteBuf> {
    static final int QUEUED_CHUNKS = 2;
    private static final long WAIT_MS = 100;

    private final Channel channel;
    private final ChunkedWriteHandler writer;
    private final BlockingQueue<ByteBuf> chunks = new ArrayBlockingQueue<>(QUEUED_CHUNKS);
    private volatile boolean produced = false;
    private volatile Throwable failure = null;
    private volatile boolean closed = false;
    // Read and written on the event loop only.
    private long progress = 0;

    public HandOffChunkedInput(Channel channel, ChunkedWriteHandler writer) {
        this.channel = channel;
        this.writer = writer;
    }

    /**
     * Produces all of {@code source} on the calling thread, which must not be the channel's event loop. Returns once
     * every chunk is handed over, or early if the transfer ends first. A failure of the source, an Error included,
     * is handed over rather than thrown: the event loop then fails the body and closes the connection, where a
     * failure left to the caller would leave the channel waiting for chunks that never come.
     */
    public void produce(ChunkedInput<ByteBuf> source, ByteBufAllocator allocator) {
        try {
            while (!source.isEndOfInput()) {
                ByteBuf chunk = source.readChunk(allocator);
                if (chunk != null && !handOver(chunk)) {
                    return;
                }
            }
            produced = true;
        } catch (Exception | Error e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            failure = e;
        } finally {
            try {
                source.close();
            } catch (Exception e) {
                // The body is complete or given up by now; there is nothing left to tell the client.
            }
            writer.resumeTransfer();
        }
    }

    // Returns false when the transfer ended before the chunk could be handed over; the chunk is then released.
    private boolean handOver(ByteBuf chunk) throws InterruptedException {
        while (!chunks.offer(chunk, WAIT_MS, TimeUnit.MILLISECONDS)) {
            if (closed || !channel.isActive()) {
                chunk.release();
                return false;
            }
        }
        if (closed) {
            // close() may have emptied the queue just before this chunk went in.
            releaseQueued();
            return false;
        }
        writer.resumeTransfer();
        return true;
    }

    private void releaseQueued() {
        ByteBuf chunk;
        while ((chunk = chunks.poll()) != null) {
            chunk.release();
        }
    }

    @Override
    public boolean isEndOfInput() {
        return produced && chunks.isEmpty();
    }

    @Override
    public void close() {
        closed = true;
        releaseQueued();
    }

    @Override
    @SuppressWarnings("deprecation")
    public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
        return readChunk(ctx.alloc());
    }

    // Returns null while the producer has nothing ready: ChunkedWriteHandler then waits for the next resumeTransfer.
    @Override
    public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
        Throwable cause = failure;
        if (cause != null) {
            throw new IOException("failed to produce the response body", cause);
        }
        ByteBuf chunk = chunks.poll();
        if (chunk != null) {
            progress += chunk.readableBytes();
        }
        return chunk;
    }

    @Override
    public long length() {
        return -1;
    }

    @Override
    public long progress() {
        return progress;
    }
}
