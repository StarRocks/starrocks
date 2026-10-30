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

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A JSON array written one element at a time, as the channel asks for it, so the whole array is never held in
 * memory. Each chunk carries whole elements until it reaches about {@code chunkSize} bytes (one element larger than
 * that goes out alone), and {@link io.netty.handler.stream.ChunkedWriteHandler} reads the next chunk only once the
 * channel can take it: a response costs about one chunk of heap however many elements it carries, and is produced
 * as fast as the client reads it. The bytes are those {@code gson.toJson(elements)} would give.
 */
public class JsonArrayChunkedInput<T> implements ChunkedInput<ByteBuf> {
    public static final int DEFAULT_CHUNK_SIZE = 64 * 1024;

    private final Gson gson;
    private final Type elementType;
    private final int chunkSize;
    private Iterator<T> elements;
    private boolean opened = false;
    private boolean endOfInput = false;
    private long written = 0;
    private long progress = 0;

    public JsonArrayChunkedInput(Gson gson, Type elementType, List<T> elements) {
        this(gson, elementType, elements, DEFAULT_CHUNK_SIZE);
    }

    public JsonArrayChunkedInput(Gson gson, Type elementType, List<T> elements, int chunkSize) {
        Preconditions.checkArgument(chunkSize > 0, "chunkSize must be positive");
        this.gson = gson;
        this.elementType = elementType;
        this.elements = elements.iterator();
        this.chunkSize = chunkSize;
    }

    @Override
    public boolean isEndOfInput() {
        return endOfInput;
    }

    @Override
    public void close() {
        // Called when the transfer ends or the channel goes away: drop the references to the elements.
        elements = Collections.emptyIterator();
    }

    @Deprecated
    @Override
    public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
        return readChunk(ctx.alloc());
    }

    @Override
    public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
        if (endOfInput) {
            return null;
        }
        ByteBuf chunk = allocator.buffer(chunkSize);
        boolean done = false;
        try {
            // Encodes straight into the chunk; the writer is flushed after every element so the size check
            // sees the element's bytes.
            Writer writer = new OutputStreamWriter(new ByteBufOutputStream(chunk), StandardCharsets.UTF_8);
            if (!opened) {
                writer.write('[');
                opened = true;
            }
            while (elements.hasNext() && chunk.readableBytes() < chunkSize) {
                if (written++ > 0) {
                    writer.write(',');
                }
                gson.toJson(elements.next(), elementType, writer);
                writer.flush();
            }
            if (!elements.hasNext()) {
                writer.write(']');
                endOfInput = true;
            }
            writer.flush();
            progress += chunk.readableBytes();
            done = true;
            return chunk;
        } finally {
            if (!done) {
                chunk.release();
            }
        }
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
