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

package com.starrocks.context;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.thrift.TResultBatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Shared helpers for the SQL access path used across the semantic-context module. Every internal
 * read goes through {@link SimpleExecutor#executeDQL} and decodes its {@link TResultBatch} payload
 * into a flat {@link JsonArray} of row objects ({@code {"data": [...]}}). Centralizing the decode
 * loop here removes ~10 near-duplicate copies that drifted in subtle ways (logging, exception
 * handling, metric counts) and gives every caller one canonical shape to reason about.
 *
 * <p>Exception / retry policy stays at the call site by design: daemons want to swallow errors and
 * tick again, write executors want to propagate, the consistency gate carries the error string
 * upward. Use {@link #executeDql(String)} when you want propagate-on-failure; wrap it in your own
 * try/catch if you want swallow-and-log.
 */
public final class ContextSqlSupport {

    private ContextSqlSupport() {
    }

    /**
     * Decode a {@link TResultBatch} list — the wire format returned by {@code SimpleExecutor} —
     * into a flat {@link JsonArray} of one element per row. Each row element is the parsed JSON
     * shipped by the BE result encoder; callers pull the {@code data} array out via
     * {@code row.getAsJsonObject().getAsJsonArray("data")}.
     */
    public static JsonArray collectRows(List<TResultBatch> batches) {
        JsonArray rows = new JsonArray();
        for (TResultBatch batch : batches) {
            if (batch.getRows() == null) {
                continue;
            }
            for (ByteBuffer buf : batch.getRows()) {
                ByteBuf copied = Unpooled.copiedBuffer(buf);
                rows.add(JsonParser.parseString(copied.toString(Charset.defaultCharset())));
            }
        }
        return rows;
    }

    /**
     * Run a DQL statement through the repo-pool {@link SimpleExecutor} and decode the result.
     * Propagates any underlying execution / parse error; wrap at the call site if you need a
     * swallow-and-empty fallback.
     */
    public static JsonArray executeDql(String sql) {
        return collectRows(SimpleExecutor.getRepoExecutor().executeDQL(sql));
    }
}
