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
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.connector.index.IndexTable;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexResultSerializer;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Executes and aggregates the first, distributed stage of a Paimon Global Index scan. */
public final class PaimonGlobalIndexService {
    interface QueryExecutor {
        List<TResultBatch> execute(String sql, int timeoutSeconds);
    }

    private final PaimonTable table;
    private final IndexCondition condition;
    private final long snapshotId;
    private final QueryExecutor executor;

    public PaimonGlobalIndexService(PaimonTable table, IndexCondition condition, long snapshotId) {
        this(table, condition, snapshotId,
                (sql, timeout) -> new SimpleExecutor("PaimonGlobalIndex", TResultSinkType.HTTP_PROTOCAL)
                        .executeDQL(sql, timeout));
    }

    PaimonGlobalIndexService(
            PaimonTable table, IndexCondition condition, long snapshotId, QueryExecutor executor) {
        this.table = table;
        this.condition = condition;
        this.snapshotId = snapshotId;
        this.executor = executor;
    }

    public PaimonGlobalIndexResult evaluate() {
        PaimonGlobalIndexRequest request = PaimonGlobalIndexRequest.create(snapshotId, condition);
        int timeout = Math.max(1, SimpleExecutor.outerRemainingQueryTimeoutS());
        List<TResultBatch> batches = executor.execute(buildSql(request.toTransportString()), timeout);

        GlobalIndexResult aggregate = request.isTopN()
                ? collectScoredTopN(batches) : collectPredicateResults(batches);
        return new PaimonGlobalIndexResult(snapshotId, aggregate);
    }

    private static GlobalIndexResult collectPredicateResults(List<TResultBatch> batches) {
        GlobalIndexResult aggregate = null;
        GlobalIndexResultSerializer serializer = new GlobalIndexResultSerializer();
        for (TResultBatch batch : batches) {
            for (ByteBuffer row : batch.getRows()) {
                JsonArray data = parseData(row);
                if (data.size() == 0 || data.get(0).isJsonNull()) {
                    continue;
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
            aggregate = GlobalIndexResult.createEmpty();
        }
        return aggregate;
    }

    private static ScoredGlobalIndexResult collectScoredTopN(List<TResultBatch> batches) {
        RoaringNavigableMap64 rows = new RoaringNavigableMap64();
        Map<Long, Float> scores = new HashMap<>();
        for (TResultBatch batch : batches) {
            for (ByteBuffer row : batch.getRows()) {
                JsonArray data = parseData(row);
                if (data.size() != 2 || data.get(0).isJsonNull() || data.get(1).isJsonNull()) {
                    throw new IllegalStateException("Malformed Paimon Vector TopN result row");
                }
                long rowId = data.get(0).getAsLong();
                float score = data.get(1).getAsFloat();
                if (rowId < 0 || !Float.isFinite(score)) {
                    throw new IllegalStateException("Invalid Paimon Vector TopN row id or score");
                }
                rows.add(rowId);
                scores.putIfAbsent(rowId, score);
            }
        }
        rows.runOptimize();
        return ScoredGlobalIndexResult.create(rows, rowId -> scores.getOrDefault(rowId, 0.0f));
    }

    String buildSql(String requestTransport) {
        PaimonGlobalIndexRequest request = PaimonGlobalIndexRequest.parseTransport(requestTransport);
        if (request.isTopN()) {
            return String.format("select `%s`, `%s` from `%s`.`%s`.`%s%s` where `%s`='%s' "
                            + "order by `%s` %s limit %d",
                    IndexTable.ROW_ID_COLUMN_NAME,
                    IndexTable.SCORE_COLUMN_NAME,
                    quoteIdentifier(table.getCatalogName()),
                    quoteIdentifier(table.getCatalogDBName()),
                    quoteIdentifier(table.getCatalogTableName()),
                    IndexTable.INDEX_TABLE_SUFFIX,
                    IndexTable.ARGS_COLUMN_NAME,
                    requestTransport,
                    IndexTable.SCORE_COLUMN_NAME,
                    request.isAscending() ? "asc" : "desc",
                    request.getLocalLimit());
        }
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
