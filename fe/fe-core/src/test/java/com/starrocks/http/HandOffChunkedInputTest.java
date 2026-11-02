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

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.starrocks.qe.QueryDetail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * The producer runs on its own thread as it does on the HTTP handler; the test thread plays the event loop. The
 * ChunkedWriteHandler is not in a pipeline, so its resumeTransfer does nothing and the test polls instead.
 */
public class HandOffChunkedInputTest {
    private static final ByteBufAllocator ALLOCATOR = UnpooledByteBufAllocator.DEFAULT;

    private final EmbeddedChannel channel = new EmbeddedChannel();
    private final HandOffChunkedInput handOff = new HandOffChunkedInput(channel, new ChunkedWriteHandler());

    @AfterEach
    public void closeChannel() {
        channel.close();
    }

    // A source of {@code count} chunks of 1 KiB that keeps every chunk it made, and fails at chunk {@code failAt}.
    private static class CountingSource implements ChunkedInput<ByteBuf> {
        private final int count;
        private final int failAt;
        private final Throwable failure;
        final List<ByteBuf> made = new ArrayList<>();
        volatile boolean closed = false;

        CountingSource(int count) {
            this(count, -1, null);
        }

        CountingSource(int count, int failAt, Throwable failure) {
            this.count = count;
            this.failAt = failAt;
            this.failure = failure;
        }

        synchronized int madeCount() {
            return made.size();
        }

        @Override
        public boolean isEndOfInput() {
            return madeCount() == count;
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        @SuppressWarnings("deprecation")
        public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
            return readChunk(ctx.alloc());
        }

        @Override
        public synchronized ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
            if (made.size() == failAt) {
                if (failure instanceof Error) {
                    throw (Error) failure;
                }
                throw (Exception) failure;
            }
            ByteBuf chunk = Unpooled.wrappedBuffer(new byte[1024]);
            made.add(chunk);
            return chunk;
        }

        @Override
        public long length() {
            return -1;
        }

        @Override
        public long progress() {
            return 0;
        }
    }

    private Thread produceInBackground(ChunkedInput<ByteBuf> source) {
        Thread producer = new Thread(() -> handOff.produce(source, ALLOCATOR), "hand-off-producer");
        producer.start();
        return producer;
    }

    // Plays the event loop: takes chunks as they come until the body ends.
    private String drain() throws Exception {
        StringBuilder body = new StringBuilder();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!handOff.isEndOfInput()) {
            Assertions.assertTrue(System.nanoTime() < deadline, "body never ended");
            ByteBuf chunk = handOff.readChunk(ALLOCATOR);
            if (chunk == null) {
                Thread.sleep(1);
                continue;
            }
            body.append(chunk.toString(StandardCharsets.UTF_8));
            chunk.release();
        }
        return body.toString();
    }

    private static void waitFor(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            Assertions.assertTrue(System.nanoTime() < deadline, "condition never held");
            Thread.sleep(5);
        }
    }

    @Test
    public void testHandsOverTheWholeBodyInOrder() throws Exception {
        Gson gson = new Gson();
        List<QueryDetail> details = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            QueryDetail detail = new QueryDetail();
            detail.setQueryId("query-" + i);
            detail.setSql("select '" + Strings.repeat("中", 300) + "'");
            details.add(detail);
        }
        Thread producer = produceInBackground(new JsonArrayChunkedInput<>(gson, QueryDetail.class, details, 512));

        String body = drain();

        producer.join(TimeUnit.SECONDS.toMillis(10));
        Assertions.assertFalse(producer.isAlive());
        Assertions.assertEquals(gson.toJson(details), body);
        Assertions.assertEquals(gson.toJson(details).getBytes(StandardCharsets.UTF_8).length, handOff.progress());
        Assertions.assertNull(handOff.readChunk(ALLOCATOR));
    }

    @Test
    public void testProducerWaitsWhileTheQueueIsFull() throws Exception {
        CountingSource source = new CountingSource(100);
        Thread producer = produceInBackground(source);

        // Two chunks queued and a third waiting to go in; nothing more is made until the consumer takes one.
        waitFor(() -> source.madeCount() == HandOffChunkedInput.QUEUED_CHUNKS + 1);
        Thread.sleep(300);
        Assertions.assertEquals(HandOffChunkedInput.QUEUED_CHUNKS + 1, source.madeCount());
        Assertions.assertTrue(producer.isAlive());

        handOff.readChunk(ALLOCATOR).release();
        waitFor(() -> source.madeCount() == HandOffChunkedInput.QUEUED_CHUNKS + 2);
        handOff.close();
        producer.join(TimeUnit.SECONDS.toMillis(10));
        Assertions.assertFalse(producer.isAlive());
    }

    @Test
    public void testCloseStopsTheProducerAndReleasesWhatItMade() throws Exception {
        CountingSource source = new CountingSource(100);
        Thread producer = produceInBackground(source);
        waitFor(() -> source.madeCount() == HandOffChunkedInput.QUEUED_CHUNKS + 1);

        handOff.close();

        producer.join(TimeUnit.SECONDS.toMillis(10));
        Assertions.assertFalse(producer.isAlive());
        Assertions.assertTrue(source.closed);
        for (ByteBuf chunk : source.made) {
            Assertions.assertEquals(0, chunk.refCnt());
        }
    }

    @Test
    public void testConnectionGoneStopsTheProducer() throws Exception {
        CountingSource source = new CountingSource(100);
        Thread producer = produceInBackground(source);
        waitFor(() -> source.madeCount() == HandOffChunkedInput.QUEUED_CHUNKS + 1);

        // The channel can go away before ChunkedWriteHandler ever saw the body, so nobody calls close().
        channel.close();

        producer.join(TimeUnit.SECONDS.toMillis(10));
        Assertions.assertFalse(producer.isAlive());
        Assertions.assertTrue(source.closed);
    }

    @Test
    public void testFailureOfTheSourceIsHandedOver() throws Exception {
        CountingSource source = new CountingSource(10, 1, new IllegalStateException("cannot serialize"));
        Thread producer = produceInBackground(source);
        producer.join(TimeUnit.SECONDS.toMillis(10));
        Assertions.assertFalse(producer.isAlive());

        IOException e = Assertions.assertThrows(IOException.class, () -> handOff.readChunk(ALLOCATOR));
        Assertions.assertEquals("cannot serialize", e.getCause().getMessage());
        Assertions.assertFalse(handOff.isEndOfInput());
        Assertions.assertTrue(source.closed);
    }

    @Test
    public void testErrorOfTheSourceIsHandedOverRatherThanThrown() throws Exception {
        CountingSource source = new CountingSource(10, 0, new OutOfMemoryError("Java heap space (injected)"));
        Thread producer = produceInBackground(source);
        producer.join(TimeUnit.SECONDS.toMillis(10));
        Assertions.assertFalse(producer.isAlive());

        IOException e = Assertions.assertThrows(IOException.class, () -> handOff.readChunk(ALLOCATOR));
        Assertions.assertInstanceOf(OutOfMemoryError.class, e.getCause());
    }
}
