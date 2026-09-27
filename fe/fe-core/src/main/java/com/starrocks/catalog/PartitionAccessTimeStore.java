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

package com.starrocks.catalog;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.starrocks.common.Config;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TResultBatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringJoiner;

/**
 * All SQL against the internal {@code _statistics_.partition_access_time} table lives here.
 * <p>The table is a pure durability record: the leader flushes increments into it and loads it back into the
 * authoritative in-memory map once, when it becomes leader. The read hot path never queries it (see
 * {@link PartitionAccessTimeMgr}).
 */
public class PartitionAccessTimeStore {
    private static final Logger LOG = LogManager.getLogger(PartitionAccessTimeStore.class);
    private static final String TABLE =
            StatsConstants.STATISTICS_DB_NAME + "." + StatsConstants.PARTITION_ACCESS_TIME_TABLE_NAME;
    private static final String LOAD_ALL_SQL =
            "SELECT db_id, table_id, partition_id, last_access_time_ms FROM " + TABLE;

    public static String buildUpsertSql(List<PartitionAccessTimeEntry> entries) {
        StringJoiner values = new StringJoiner(", ");
        for (PartitionAccessTimeEntry e : entries) {
            values.add("(" + e.getDbId() + ", " + e.getTableId() + ", "
                    + e.getPartitionId() + ", " + e.getAccessTimeMs() + ")");
        }
        return "INSERT INTO " + TABLE + " VALUES " + values;
    }

    public static String buildDeleteByPartitionIdsSql(Collection<Long> partitionIds) {
        return "DELETE FROM " + TABLE + " WHERE partition_id IN (" + join(partitionIds) + ")";
    }

    private static String join(Collection<Long> ids) {
        StringJoiner j = new StringJoiner(", ");
        for (Long id : ids) {
            j.add(String.valueOf(id));
        }
        return j.toString();
    }

    public void upsert(List<PartitionAccessTimeEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return;
        }
        // An INSERT ... VALUES row list is capped by the parser at Config.expr_children_limit rows
        // (insertRowsExceedLimit). Chunk with the same /2 margin the statistics cleanup uses so a large flush
        // never trips the cap.
        int chunkSize = Math.max(1, Config.expr_children_limit / 2);
        for (List<PartitionAccessTimeEntry> chunk : ListUtils.partition(entries, chunkSize)) {
            SimpleExecutor.getRepoExecutor().executeDML(buildUpsertSql(chunk));
        }
    }

    public void deleteByPartitionIds(Collection<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return;
        }
        int chunkSize = Math.max(1, Math.min(Config.expr_children_limit / 2, Config.max_allowed_in_element_num_of_delete));
        for (List<Long> chunk : ListUtils.partition(new ArrayList<>(ids), chunkSize)) {
            SimpleExecutor.getRepoExecutor().executeDML(buildDeleteByPartitionIdsSql(chunk));
        }
    }

    /**
     * The full persisted baseline as flat entries. The leader loads this once into memory when it becomes
     * leader. Propagates a query failure so the caller can retry the one-time load next cycle rather than
     * start from an empty baseline.
     */
    public List<PartitionAccessTimeEntry> loadAll() {
        return parseEntries(SimpleExecutor.getRepoExecutor().executeDQL(LOAD_ALL_SQL));
    }

    /**
     * Decode rows shaped like {@code (db_id, table_id, partition_id, last_access_time_ms)} into flat entries.
     * <p>The repo executor runs with {@code TResultSinkType.HTTP_PROTOCAL}, so each row is a JSON document of
     * the form {@code {"data": [col1, col2, ...]}} carried as a single buffer inside
     * {@link TResultBatch#getRows()}; this mirrors the decode idiom used by {@code TaskRunStatus#fromResultBatch}
     * and {@code PipeFileRecord#fromResultBatch}.
     */
    private static List<PartitionAccessTimeEntry> parseEntries(List<TResultBatch> batches) {
        List<PartitionAccessTimeEntry> out = new ArrayList<>();
        for (TResultBatch batch : ListUtils.emptyIfNull(batches)) {
            for (ByteBuffer buffer : batch.getRows()) {
                try {
                    ByteBuf copied = Unpooled.copiedBuffer(buffer);
                    String jsonString = copied.toString(Charset.defaultCharset());
                    JsonArray data = JsonParser.parseString(jsonString).getAsJsonObject().get("data").getAsJsonArray();
                    out.add(new PartitionAccessTimeEntry(
                            data.get(0).getAsLong(),
                            data.get(1).getAsLong(),
                            data.get(2).getAsLong(),
                            data.get(3).getAsLong()));
                } catch (Exception e) {
                    LOG.warn("failed to parse partition access time row: {}", e.getMessage());
                }
            }
        }
        return out;
    }
}
