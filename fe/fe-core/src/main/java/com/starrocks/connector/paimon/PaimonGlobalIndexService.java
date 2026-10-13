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

package com.starrocks.connector.paimon;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.connector.index.IndexTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexResultSerializer;
import org.apache.paimon.io.DataInputDeserializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

/**
 * Executes and aggregates the first, distributed stage of a Paimon Global Index scan.
 * The internal SQL uses the caller's identity and the virtual table's normal authorization checks;
 * base-table privileges are not mapped to the virtual table. Authorization failures propagate to
 * the scan planner, which retains the original predicate and falls back to SDK split planning.
 */
public final class PaimonGlobalIndexService {
    interface QueryExecutor {
        List<TResultBatch> execute(String sql, int timeoutSeconds);
    }

    private final PaimonTable table;
    private final PaimonGlobalIndexRequest request;
    private final QueryExecutor executor;

    public PaimonGlobalIndexService(PaimonTable table, PaimonGlobalIndexRequest request) {
        this(table, request,
                (sql, timeout) -> new SimpleExecutor("PaimonGlobalIndex", TResultSinkType.HTTP_PROTOCAL)
                        // Every shard bitmap is required, regardless of the caller's SELECT cap.
                        .executeDQLAsCaller(sql, timeout, ConnectContext.get(), SessionVariable.DEFAULT_SELECT_LIMIT));
    }

    PaimonGlobalIndexService(
            PaimonTable table, PaimonGlobalIndexRequest request, QueryExecutor executor) {
        this.table = table;
        this.request = request;
        this.executor = executor;
    }

    public PaimonGlobalIndexResult evaluate() {
        int timeout = Math.max(1, SimpleExecutor.outerRemainingQueryTimeoutS());
        List<TResultBatch> batches = executor.execute(buildSql(request.toTransportString()), timeout);

        GlobalIndexResult aggregate = null;
        GlobalIndexResultSerializer serializer = new GlobalIndexResultSerializer();
        for (TResultBatch batch : batches) {
            for (ByteBuffer row : batch.getRows()) {
                JsonArray data = parseData(row);
                if (data.size() == 0 || data.get(0).isJsonNull()) {
                    throw new IllegalStateException("Missing Paimon Global Index shard result");
                }
                byte[] serialized = Base64.getDecoder().decode(data.get(0).getAsString());
                try {
                    GlobalIndexResult partial = serializer.deserialize(new DataInputDeserializer(serialized));
                    aggregate = aggregate == null ? partial : aggregate.or(partial);
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to deserialize Paimon Global Index result", e);
                }
            }
        }
        if (aggregate == null) {
            throw new IllegalStateException("Paimon Global Index query returned no shard results");
        }
        return new PaimonGlobalIndexResult(
                request.getTablePath(), request.getBranch(), request.getSnapshotId(), aggregate);
    }

    String buildSql(String requestTransport) {
        return String.format("select to_base64(`%s`) from `%s`.`%s`.`%s%s` where `%s`='%s'",
                IndexTable.INDEX_RESULT_COLUMN_NAME,
                quoteIdentifier(table.getCatalogName()),
                quoteIdentifier(table.getCatalogDBName()),
                quoteIdentifier(table.getCatalogTableName()),
                IndexTable.INDEX_TABLE_SUFFIX,
                IndexTable.ARGS_COLUMN_NAME,
                requestTransport);
    }

    private static String quoteIdentifier(String identifier) {
        return identifier.replace("`", "``");
    }

    private static JsonArray parseData(ByteBuffer row) {
        ByteBuf copied = Unpooled.copiedBuffer(row);
        try {
            JsonElement json = JsonParser.parseString(copied.toString(StandardCharsets.UTF_8));
            return json.getAsJsonObject().getAsJsonArray("data");
        } finally {
            copied.release();
        }
    }
}
