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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.pipe.filelist.RepoExecutor;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TResultBatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * 1. Persistence:
 * The in-memory state is periodically persisted, so we use the ColumnUsage.lastUsage and this.lastPersist to
 * identify whether one ColumnUsage needs to be persisted.
 * 2. Query:
 * The in-memory state can only represent local state, the persisted state is shared by all fe nodes
 * 3. Vacuum:
 * If the ColumnUsage.lastUsage is less than now() - ttl, it would be removed from memory state and persisted state
 * 4. Restore:
 * Before restoring persisted state into memory, some queries may already come up, so we need to merge these two
 * kinds of state. Before restoring state we cannot persist otherwise they would conflict
 */
public class PredicateColumnsStorage {

    private static final Logger LOG = LogManager.getLogger(PredicateColumnsStorage.class);

    public static final String DATABASE_NAME = StatsConstants.STATISTICS_DB_NAME;
    public static final String TABLE_NAME = "predicate_columns";
    public static final String TABLE_FULL_NAME = DATABASE_NAME + "." + TABLE_NAME;
    private static final String TABLE_DDL = "CREATE TABLE " + TABLE_NAME + "( " +

            // PRIMARY KEYS
            "fe_id STRING NOT NULL," +
            "db_id BIGINT NOT NULL," +
            "table_id BIGINT NOT NULL," +
            "column_id BIGINT NOT NULL," +

            "usage STRING NOT NULL," +
            "last_used DATETIME NOT NULL," +
            "created DATETIME DEFAULT CURRENT_TIMESTAMP" +
            ") " +
            "PRIMARY KEY(fe_id, db_id, table_id, column_id) " +
            "DISTRIBUTED BY HASH(fe_id, db_id, table_id, column_id) BUCKETS 8\n" +
            "PROPERTIES('replication_num'='1')";

    private static final TableKeeper KEEPER = new TableKeeper(DATABASE_NAME, TABLE_NAME, TABLE_DDL, null);

    private static final VelocityEngine DEFAULT_VELOCITY_ENGINE;

    static {
        DEFAULT_VELOCITY_ENGINE = new VelocityEngine();
        DEFAULT_VELOCITY_ENGINE.setProperty(VelocityEngine.RUNTIME_LOG_REFERENCE_LOG_INVALID, false);
    }

    private static final int INSERT_BATCH_SIZE = 1024;
    private static final String SQL_COLUMN_LIST = "fe_id, db_id, table_id, column_id, usage, last_used ";

    private static final String ADD_RECORD = "INSERT INTO " + TABLE_FULL_NAME + "(" + SQL_COLUMN_LIST + ") VALUES ";
    private static final String INSERT_VALUE = "('$feId', $dbId, $tableId, $columnId, '$usage', '$lastUsed')";

    private static final String QUERY =
            "SELECT fe_id, db_id,  table_id,  column_id, usage, last_used, created FROM " + TABLE_FULL_NAME
                    + " WHERE true ";
    private static final String WHERE_THIS_FE = "fe_id = '$feId'";

    private static final String VACUUM = "DELETE FROM " + TABLE_FULL_NAME +
            " WHERE fe_id='$feId' AND last_used < '$lastUsed'";

    private static final PredicateColumnsStorage INSTANCE = new PredicateColumnsStorage();

    /**
     * MIN: the persisted state has not been restored, we cannot persist
     */
    private final LocalDateTime systemStartTime = TimeUtils.getSystemNow();
    private volatile LocalDateTime lastPersist = LocalDateTime.MIN;

    private final RepoExecutor executor;

    public static TableKeeper createKeeper() {
        return KEEPER;
    }

    public static PredicateColumnsStorage getInstance() {
        return INSTANCE;
    }

    public PredicateColumnsStorage() {
        this.executor = RepoExecutor.getInstance();
    }

    public PredicateColumnsStorage(RepoExecutor executor) {
        this.executor = executor;
    }

    /**
     * Query state from all FEs
     */
    public List<ColumnUsage> queryGlobalState(TableName tableName, EnumSet<ColumnUsage.UseCase> useCases) {
        LocalMetastore meta = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Optional<Database> db = StringUtils.isEmpty(tableName.getDb()) ? Optional.empty()
                : meta.mayGetDb(tableName.getDb());
        Optional<Table> table = StringUtils.isEmpty(tableName.getTbl()) ? Optional.empty()
                : meta.mayGetTable(tableName.getDb(), tableName.getTbl());

        StringBuilder sb = new StringBuilder(QUERY);
        db.ifPresent(database -> sb.append(" AND `db_id` = ").append(database.getId()));
        table.ifPresent(value -> sb.append(" AND `table_id` = ").append(value.getId()));

        if (!useCases.isEmpty()) {
            String useCaseRegex = useCases.stream().map(ColumnUsage.UseCase::toString).collect(Collectors.joining("|"));
            sb.append(String.format(" AND regexp(`usage`, '%s')", useCaseRegex));
        }

        String sql = sb.toString();
        List<TResultBatch> tResultBatches = executor.executeDQL(sql);
        List<ColumnUsage> columnUsages = resultToColumnUsage(tResultBatches);
        if (GlobalStateMgr.getCurrentState().getNodeMgr().getFrontends().size() == 1) {
            return columnUsages;
        } else {
            return deduplicateColumnUsages(columnUsages);
        }
    }

    public List<ColumnUsage> deduplicateColumnUsages(List<ColumnUsage> columnUsages) {
        Map<ColumnUsage, ColumnUsage> merged = columnUsages.stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        Function.identity(),
                        (existing, current) -> {
                            existing.getUseCases().addAll(current.getUseCases());
                            if (current.getLastUsed().isAfter(existing.getLastUsed())) {
                                existing.setLastUsed(current.getLastUsed());
                            }
                            return existing;
                        }
                ));

        return new ArrayList<>(merged.values());
    }

    /**
     * Persist changed records
     */
    public void persist(Collection<ColumnUsage> columnUsageList) {
        if (lastPersist == LocalDateTime.MIN) {
            return;
        }
        LocalDateTime nextPersist = TimeUtils.getSystemNow();
        Stopwatch watch = Stopwatch.createStarted();
        List<ColumnUsage> diff =
                columnUsageList.stream().filter(x -> x.needPersist(lastPersist)).collect(Collectors.toList());
        for (var batch : ListUtils.partition(diff, INSERT_BATCH_SIZE)) {
            persistDiff(batch);
        }
        lastPersist = nextPersist;
        String elapsed = watch.toString();
        LOG.info("persist {} diffed predicate columns elapsed {}, update lastPersist to {}", diff.size(), elapsed,
                lastPersist);
    }

    private void persistDiff(List<ColumnUsage> diff) {
        StringBuilder insert = new StringBuilder(ADD_RECORD);
        boolean first = true;
        LocalMetastore meta = GlobalStateMgr.getCurrentState().getLocalMetastore();
        for (ColumnUsage usage : diff) {
            StringWriter sw = new StringWriter();

            VelocityContext context = new VelocityContext();
            ColumnFullId fullId = usage.getColumnFullId();
            context.put("feId", GlobalStateMgr.getCurrentState().getNodeMgr().getNodeName());
            context.put("dbId", fullId.getDbId());
            context.put("tableId", fullId.getTableId());
            context.put("columnId", fullId.getColumnUniqueId());
            context.put("usage", usage.getUseCaseString());
            context.put("lastUsed", DateUtils.formatDateTimeUnix(usage.getLastUsed()));

            DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", INSERT_VALUE);
            if (!first) {
                insert.append(", ");
            }
            insert.append(sw);
            first = false;
        }

        String sql = insert.toString();
        executor.executeDML(sql);
    }

    /**
     * TODO: splice the result into batch, to reduce memory consumption
     * Restore all states
     */
    public List<ColumnUsage> restore() {
        String selfName = GlobalStateMgr.getCurrentState().getNodeMgr().getNodeName();

        VelocityContext context = new VelocityContext();
        context.put("feId", selfName);

        String template = QUERY + " AND " + WHERE_THIS_FE;
        StringWriter sw = new StringWriter();
        DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", template);
        String sql = sw.toString();

        List<TResultBatch> tResultBatches = executor.executeDQL(sql);
        return resultToColumnUsage(tResultBatches);
    }

    /**
     * Remove all records if the lastUsed < ttlTime
     */
    public void vacuum(LocalDateTime ttlTime) {
        VelocityContext context = new VelocityContext();
        String selfName = GlobalStateMgr.getCurrentState().getNodeMgr().getNodeName();
        context.put("feId", selfName);
        context.put("lastUsed", DateUtils.formatDateTimeUnix(ttlTime));

        StringWriter sw = new StringWriter();
        DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", VACUUM);

        String sql = sw.toString();
        executor.executeDML(sql);
        LOG.info("vacuum column usage from storage before {}", ttlTime);
    }

    public boolean isSystemTableReady() {
        return KEEPER.isReady();
    }

    public boolean isRestored() {
        return lastPersist != LocalDateTime.MIN;
    }

    public void finishRestore() {
        lastPersist = systemStartTime;
        LOG.info("finish restore state, update lastPersist to {}", lastPersist);
    }

    @VisibleForTesting
    protected void finishRestore(LocalDateTime lastPersistTime) {
        lastPersist = lastPersistTime;
    }

    @VisibleForTesting
    static class ColumnUsageJsonRecord {

        List<ColumnUsage> data;

        /**
         * {
         * "data": [field1, field2, field3]
         * }
         */
        public static ColumnUsageJsonRecord fromJson(String json) {
            JsonElement object = JsonParser.parseString(json);
            JsonArray data = object.getAsJsonObject().get("data").getAsJsonArray();
            // String feId = data.get(0).getAsString();
            long dbId = data.get(1).getAsLong();
            long tableId = data.get(2).getAsLong();
            long columnId = data.get(3).getAsLong();
            String useCase = data.get(4).getAsString();
            String lastUsed = data.get(5).getAsString();
            String created = data.get(6).getAsString();
            ColumnFullId fullId = new ColumnFullId(dbId, tableId, columnId);
            Optional<Pair<TableName, ColumnId>> names = fullId.toNames();
            TableName tableName = names.map(x -> x.first).orElse(null);
            EnumSet<ColumnUsage.UseCase> useCases = ColumnUsage.fromUseCaseString(useCase);
            ColumnUsage usage = new ColumnUsage(fullId, tableName, useCases);
            usage.setLastUsed(DateUtils.parseUnixDateTime(lastUsed));
            usage.setCreated(DateUtils.parseUnixDateTime(created));

            ColumnUsageJsonRecord res = new ColumnUsageJsonRecord();
            res.data = List.of(usage);
            return res;
        }

    }

    @VisibleForTesting
    protected static List<ColumnUsage> resultToColumnUsage(List<TResultBatch> batches) {
        List<ColumnUsage> res = new ArrayList<>();
        for (TResultBatch batch : ListUtils.emptyIfNull(batches)) {
            for (ByteBuffer buffer : batch.getRows()) {
                ByteBuf copied = Unpooled.copiedBuffer(buffer);
                String jsonString = copied.toString(Charset.defaultCharset());
                try {
                    List<ColumnUsage> records =
                            ListUtils.emptyIfNull(ColumnUsageJsonRecord.fromJson(jsonString).data);
                    res.addAll(records);
                } catch (Exception e) {
                    throw new RuntimeException("failed to deserialize ColumnUsage record: " + jsonString, e);
                }
            }
        }
        return res;
    }

}
