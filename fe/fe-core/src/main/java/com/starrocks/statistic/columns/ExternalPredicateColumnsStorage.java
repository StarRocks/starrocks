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

package com.starrocks.statistic.columns;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Persistent storage for predicate/join/group-by columns of connector (external) tables.
 * <p>
 * Connector tables use UUID-based identity, so they cannot share the numeric-ID-keyed
 * {@code predicate_columns} table. This class owns the {@code external_predicate_columns}
 * system table and provides an in-memory write-through cache with periodic persistence.
 * <p>
 * Multi-FE: each FE records what it observes; the PK {@code (table_uuid, column_name)}
 * is shared across FEs so any FE's auto-collect job sees the union of all observations
 * when reading from the table.
 */
public class ExternalPredicateColumnsStorage {

    private static final Logger LOG = LogManager.getLogger(ExternalPredicateColumnsStorage.class);

    public static final String DATABASE_NAME = StatsConstants.STATISTICS_DB_NAME;
    public static final String TABLE_NAME = "external_predicate_columns";
    public static final String TABLE_FULL_NAME = DATABASE_NAME + "." + TABLE_NAME;

    private static final String TABLE_DDL =
            "CREATE TABLE " + TABLE_NAME + "(" +
            "table_uuid  VARCHAR(65530) NOT NULL," +
            "column_name VARCHAR(65530) NOT NULL," +
            "use_cases   VARCHAR(65530) NOT NULL DEFAULT ''," +
            "last_used   DATETIME NOT NULL," +
            "created     DATETIME DEFAULT CURRENT_TIMESTAMP" +
            ") " +
            "PRIMARY KEY(table_uuid, column_name) " +
            "DISTRIBUTED BY HASH(table_uuid, column_name) BUCKETS 8 " +
            "PROPERTIES('replication_num'='1')";

    private static final TableKeeper KEEPER = new TableKeeper(DATABASE_NAME, TABLE_NAME, TABLE_DDL, null);

    private static final VelocityEngine VELOCITY_ENGINE;

    static {
        VELOCITY_ENGINE = new VelocityEngine();
        VELOCITY_ENGINE.setProperty(VelocityEngine.RUNTIME_LOG_REFERENCE_LOG_INVALID, false);
    }

    private static final int INSERT_BATCH_SIZE = 512;

    // INSERT with ON DUPLICATE KEY handled by StarRocks PRIMARY KEY table
    private static final String INSERT_PREFIX =
            "INSERT INTO " + TABLE_FULL_NAME + "(table_uuid, column_name, use_cases, last_used) VALUES ";
    private static final String INSERT_ROW_TEMPLATE =
            "('$tableUUID', '$columnName', '$useCases', '$lastUsed')";

    private static final String QUERY_ALL =
            "SELECT table_uuid, column_name, last_used FROM " + TABLE_FULL_NAME;

    private static final String QUERY_BY_UUID =
            "SELECT column_name FROM " + TABLE_FULL_NAME + " WHERE table_uuid = '$tableUUID'";

    private static final String VACUUM_SQL =
            "DELETE FROM " + TABLE_FULL_NAME + " WHERE last_used < '$lastUsed'";

    private static final ExternalPredicateColumnsStorage INSTANCE = new ExternalPredicateColumnsStorage();

    // tableUUID -> columnName -> entry
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, ColumnEntry>> state =
            new ConcurrentHashMap<>();

    private final LocalDateTime systemStartTime = TimeUtils.getSystemNow();
    // MIN means "not yet restored; do not persist"
    private volatile LocalDateTime lastPersist = LocalDateTime.MIN;

    private final SimpleExecutor executor;

    public static ExternalPredicateColumnsStorage getInstance() {
        return INSTANCE;
    }

    public static TableKeeper createKeeper() {
        return KEEPER;
    }

    public ExternalPredicateColumnsStorage() {
        this.executor = new SimpleExecutor("ext_predicate_col", TResultSinkType.HTTP_PROTOCAL);
        this.executor.setDop(1);
    }

    // ========================= In-memory state ========================= //

    static class ColumnEntry {
        final String useCases;
        volatile LocalDateTime lastUsed;

        ColumnEntry(String useCases, LocalDateTime lastUsed) {
            this.useCases = useCases;
            this.lastUsed = lastUsed;
        }

        void touch(String newUseCases) {
            this.lastUsed = TimeUtils.getSystemNow();
        }

        boolean needPersist(LocalDateTime since) {
            return lastUsed.isAfter(since);
        }
    }

    /**
     * Record that {@code columnName} of the table identified by {@code tableUUID}
     * was used in a predicate/join/group-by during query planning.
     */
    public void record(String tableUUID, String columnName, String useCases) {
        if (tableUUID == null || tableUUID.isEmpty() || columnName == null || columnName.isEmpty()) {
            return;
        }
        LocalDateTime now = TimeUtils.getSystemNow();
        state.computeIfAbsent(tableUUID, k -> new ConcurrentHashMap<>())
                .compute(columnName, (k, existing) -> {
                    if (existing == null) {
                        return new ColumnEntry(useCases, now);
                    }
                    existing.touch(useCases);
                    return existing;
                });
    }

    /**
     * Returns the in-memory set of predicate columns for the given table UUID.
     * Empty if no columns have been recorded or the table is unknown.
     */
    public Set<String> getColumns(String tableUUID) {
        ConcurrentHashMap<String, ColumnEntry> cols = state.get(tableUUID);
        if (cols == null || cols.isEmpty()) {
            return Collections.emptySet();
        }
        return new HashSet<>(cols.keySet());
    }

    /**
     * Query predicate columns for {@code tableUUID} directly from the persistent table.
     * Used by the auto-collect job so it sees columns recorded by all FEs, including
     * those observed before the current FE's restart.
     */
    public Set<String> queryGlobalState(String tableUUID) {
        VelocityContext context = new VelocityContext();
        context.put("tableUUID", tableUUID);
        StringWriter sw = new StringWriter();
        VELOCITY_ENGINE.evaluate(context, sw, "", QUERY_BY_UUID);

        List<TResultBatch> batches;
        try {
            batches = executor.executeDQL(sw.toString());
        } catch (Exception e) {
            LOG.warn("[ExternalPredicateCols] queryGlobalState failed table_uuid={}", tableUUID, e);
            return Collections.emptySet();
        }

        Set<String> result = new HashSet<>();
        for (TResultBatch batch : ListUtils.emptyIfNull(batches)) {
            for (ByteBuffer buffer : batch.getRows()) {
                ByteBuf buf = Unpooled.copiedBuffer(buffer);
                String json = buf.toString(Charset.defaultCharset());
                try {
                    JsonElement elem = JsonParser.parseString(json);
                    JsonArray data = elem.getAsJsonObject().get("data").getAsJsonArray();
                    result.add(data.get(0).getAsString());
                } catch (Exception e) {
                    LOG.warn("[ExternalPredicateCols] failed to parse row: {}", json, e);
                }
            }
        }
        return result;
    }

    // ========================= Persistence ========================= //

    public void persist() {
        if (lastPersist == LocalDateTime.MIN) {
            return;
        }
        LocalDateTime nextPersist = TimeUtils.getSystemNow();
        Stopwatch watch = Stopwatch.createStarted();
        int count = 0;

        for (Map.Entry<String, ConcurrentHashMap<String, ColumnEntry>> tableEntry : state.entrySet()) {
            String tableUUID = tableEntry.getKey();
            List<Map.Entry<String, ColumnEntry>> dirty = Lists.newArrayList();
            for (Map.Entry<String, ColumnEntry> colEntry : tableEntry.getValue().entrySet()) {
                if (colEntry.getValue().needPersist(lastPersist)) {
                    dirty.add(colEntry);
                }
            }
            for (List<Map.Entry<String, ColumnEntry>> batch : ListUtils.partition(dirty, INSERT_BATCH_SIZE)) {
                persistBatch(tableUUID, batch);
                count += batch.size();
            }
        }

        lastPersist = nextPersist;
        LOG.info("[ExternalPredicateCols] persisted {} entries elapsed={}", count, watch);
    }

    private void persistBatch(String tableUUID, List<Map.Entry<String, ColumnEntry>> batch) {
        StringBuilder sql = new StringBuilder(INSERT_PREFIX);
        boolean first = true;
        for (Map.Entry<String, ColumnEntry> entry : batch) {
            VelocityContext context = new VelocityContext();
            context.put("tableUUID", tableUUID);
            context.put("columnName", entry.getKey());
            context.put("useCases", entry.getValue().useCases);
            context.put("lastUsed", DateUtils.formatDateTimeUnix(entry.getValue().lastUsed));
            StringWriter sw = new StringWriter();
            VELOCITY_ENGINE.evaluate(context, sw, "", INSERT_ROW_TEMPLATE);
            if (!first) {
                sql.append(", ");
            }
            sql.append(sw);
            first = false;
        }
        executor.executeDML(sql.toString());
    }

    /**
     * Load persisted state from the system table into the in-memory map.
     * Called once on startup before the first {@link #persist()} cycle.
     */
    public void restore() {
        List<TResultBatch> batches;
        try {
            batches = executor.executeDQL(QUERY_ALL);
        } catch (Exception e) {
            LOG.warn("[ExternalPredicateCols] restore failed", e);
            return;
        }

        int count = 0;
        for (TResultBatch batch : ListUtils.emptyIfNull(batches)) {
            for (ByteBuffer buffer : batch.getRows()) {
                ByteBuf buf = Unpooled.copiedBuffer(buffer);
                String json = buf.toString(Charset.defaultCharset());
                try {
                    JsonElement elem = JsonParser.parseString(json);
                    JsonArray data = elem.getAsJsonObject().get("data").getAsJsonArray();
                    String tableUUID = data.get(0).getAsString();
                    String columnName = data.get(1).getAsString();
                    LocalDateTime lastUsed = DateUtils.parseUnixDateTime(data.get(2).getAsString());
                    state.computeIfAbsent(tableUUID, k -> new ConcurrentHashMap<>())
                            .putIfAbsent(columnName, new ColumnEntry("", lastUsed));
                    count++;
                } catch (Exception e) {
                    LOG.warn("[ExternalPredicateCols] failed to parse restore row: {}", json, e);
                }
            }
        }
        LOG.info("[ExternalPredicateCols] restored {} entries", count);
    }

    /**
     * Delete rows older than {@code ttlTime} from the persistent table and in-memory map.
     */
    public void vacuum(LocalDateTime ttlTime) {
        VelocityContext context = new VelocityContext();
        context.put("lastUsed", DateUtils.formatDateTimeUnix(ttlTime));
        StringWriter sw = new StringWriter();
        VELOCITY_ENGINE.evaluate(context, sw, "", VACUUM_SQL);
        try {
            executor.executeDML(sw.toString());
        } catch (Exception e) {
            LOG.warn("[ExternalPredicateCols] vacuum failed ttl={}", ttlTime, e);
        }

        // also evict from in-memory
        for (ConcurrentHashMap<String, ColumnEntry> cols : state.values()) {
            cols.entrySet().removeIf(e -> e.getValue().lastUsed.isBefore(ttlTime));
        }
        state.entrySet().removeIf(e -> e.getValue().isEmpty());

        LOG.info("[ExternalPredicateCols] vacuum completed before {}", ttlTime);
    }

    public boolean isSystemTableReady() {
        return KEEPER.isReady();
    }

    public boolean isRestored() {
        return lastPersist != LocalDateTime.MIN;
    }

    public void finishRestore() {
        lastPersist = systemStartTime;
        LOG.info("[ExternalPredicateCols] finish restore, lastPersist={}", lastPersist);
    }
}
