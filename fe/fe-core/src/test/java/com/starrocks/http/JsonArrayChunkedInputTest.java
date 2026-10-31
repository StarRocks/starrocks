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
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.starrocks.qe.QueryDetail;
import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class JsonArrayChunkedInputTest {
    private static final Gson GSON = new Gson();
    private static final ByteBufAllocator ALLOCATOR = UnpooledByteBufAllocator.DEFAULT;

    private static QueryDetail detail(int i, String sql) {
        QueryDetail detail = new QueryDetail();
        detail.setQueryId("query-" + i);
        detail.setUser("user_" + i);
        detail.setSql(sql);
        detail.setEventTime(i);
        return detail;
    }

    private static List<QueryDetail> details(int count, int sqlLength) {
        List<QueryDetail> details = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            details.add(detail(i, "select '" + Strings.repeat("x", sqlLength) + "'"));
        }
        return details;
    }

    // Reads the input to its end; each chunk is decoded on its own, so an element split across chunks would
    // show up as a mismatch.
    private static List<String> drain(JsonArrayChunkedInput<?> input) throws Exception {
        List<String> chunks = new ArrayList<>();
        while (!input.isEndOfInput()) {
            ByteBuf chunk = input.readChunk(ALLOCATOR);
            Assertions.assertNotNull(chunk);
            try {
                chunks.add(chunk.toString(StandardCharsets.UTF_8));
            } finally {
                chunk.release();
            }
        }
        Assertions.assertNull(input.readChunk(ALLOCATOR));
        return chunks;
    }

    @Test
    public void testEmptyListIsEmptyArray() throws Exception {
        List<String> chunks = drain(new JsonArrayChunkedInput<>(GSON, QueryDetail.class, new ArrayList<>()));
        Assertions.assertEquals(List.of("[]"), chunks);
    }

    @Test
    public void testBytesMatchGsonToJson() throws Exception {
        List<QueryDetail> details = new ArrayList<>();
        details.add(detail(0, "select 1"));
        // Multi-byte text, a surrogate pair, characters Gson escapes, and control characters.
        details.add(detail(1, "select '中文', '😀', '<a href=\"x\">&\\'=</a>', '\n\t'"));
        QueryDetail sparse = new QueryDetail();
        sparse.setQueryId("only-an-id");
        details.add(sparse);
        details.add(detail(3, Strings.repeat("长", 5000)));
        String expected = GSON.toJson(details);

        for (int chunkSize : new int[] {1, 7, 1024, JsonArrayChunkedInput.DEFAULT_CHUNK_SIZE}) {
            JsonArrayChunkedInput<QueryDetail> input =
                    new JsonArrayChunkedInput<>(GSON, QueryDetail.class, details, chunkSize);
            List<String> chunks = drain(input);
            Assertions.assertEquals(expected, String.join("", chunks), "chunkSize=" + chunkSize);
            Assertions.assertEquals(expected.getBytes(StandardCharsets.UTF_8).length, input.progress());
        }
    }

    @Test
    public void testChunksCarryWholeElementsUpToTheChunkSize() throws Exception {
        int chunkSize = 4096;
        List<QueryDetail> details = details(200, 1000);
        int maxElementSize = details.stream().mapToInt(d -> GSON.toJson(d).length()).max().getAsInt();

        List<String> chunks = drain(new JsonArrayChunkedInput<>(GSON, QueryDetail.class, details, chunkSize));

        Assertions.assertTrue(chunks.size() > 10, "chunks: " + chunks.size());
        for (String chunk : chunks) {
            // A chunk stops at the first element that takes it to the chunk size.
            Assertions.assertTrue(chunk.length() < chunkSize + maxElementSize + 2, "chunk of " + chunk.length());
        }
        Assertions.assertEquals(GSON.toJson(details), String.join("", chunks));
    }

    @Test
    public void testElementLargerThanTheChunkGoesOutAlone() throws Exception {
        List<QueryDetail> details = details(3, 10 * 1024);

        List<String> chunks = drain(new JsonArrayChunkedInput<>(GSON, QueryDetail.class, details, 1024));

        Assertions.assertEquals(3, chunks.size());
        Assertions.assertEquals("[" + GSON.toJson(details.get(0)), chunks.get(0));
        Assertions.assertEquals("," + GSON.toJson(details.get(1)), chunks.get(1));
        Assertions.assertEquals("," + GSON.toJson(details.get(2)) + "]", chunks.get(2));
    }

    @Test
    public void testFailureReleasesTheChunkAndPropagates() {
        Gson failing = new GsonBuilder().registerTypeAdapter(QueryDetail.class, new TypeAdapter<QueryDetail>() {
            @Override
            public void write(JsonWriter out, QueryDetail value) throws IOException {
                if (value.getQueryId().equals("query-1")) {
                    throw new IOException("cannot write " + value.getQueryId());
                }
                out.value(value.getQueryId());
            }

            @Override
            public QueryDetail read(JsonReader in) {
                throw new UnsupportedOperationException();
            }
        }).create();
        List<ByteBuf> allocated = new ArrayList<>();
        ByteBufAllocator tracking = new AbstractByteBufAllocator(false) {
            @Override
            protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
                ByteBuf buf = Unpooled.buffer(initialCapacity, maxCapacity);
                allocated.add(buf);
                return buf;
            }

            @Override
            protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
                return newHeapBuffer(initialCapacity, maxCapacity);
            }

            @Override
            public boolean isDirectBufferPooled() {
                return false;
            }
        };
        JsonArrayChunkedInput<QueryDetail> input =
                new JsonArrayChunkedInput<>(failing, QueryDetail.class, details(3, 10), 1024);

        Assertions.assertThrows(JsonIOException.class, () -> input.readChunk(tracking));
        Assertions.assertFalse(input.isEndOfInput());
        Assertions.assertEquals(1, allocated.size());
        Assertions.assertEquals(0, allocated.get(0).refCnt());
    }

    @Test
    public void testRejectsNonPositiveChunkSize() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new JsonArrayChunkedInput<>(GSON, QueryDetail.class, new ArrayList<QueryDetail>(), 0));
    }
}
