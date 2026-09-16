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
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.server.GlobalStateMgr;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Storage for {@code _statistics_.external_predicate_columns}, mirroring
 * {@link PredicateColumnsStorage}'s persist/restore/vacuum lifecycle for external (non-native) table
 * columns. See the class doc there for the general persistence/query/vacuum/restore model; the only
 * structural difference is the identity columns: the PK is (fe_id, table_uuid hash, column_name hash)
 * instead of numeric db/table/column ids, since external tables have no metastore-resolvable numeric
 * ids and raw catalog-supplied names are unbounded in byte length. The raw column_name is kept as a
 * plain value column (not part of the PK) so it can still be read back; see
 * {@link ExternalColumnUsage}'s class doc for why both identity fields are hashed.
 */
public class ExternalPredicateColumnsStorage {

    private static final Logger LOG = LogManager.getLogger(ExternalPredicateColumnsStorage.class);

    public static final String DATABASE_NAME = StatsConstants.STATISTICS_DB_NAME;
    public static final String TABLE_NAME = "external_predicate_columns";
    public static final String TABLE_FULL_NAME = DATABASE_NAME + "." + TABLE_NAME;
    private static final String TABLE_DDL = "CREATE TABLE " + TABLE_NAME + "( " +

            // PRIMARY KEYS: column_name_hash (not the raw column_name) keeps the PK's byte size
            // fixed regardless of column name length/encoding; see ExternalColumnUsage's class doc.
            "fe_id STRING NOT NULL," +
            "table_uuid STRING NOT NULL," +
            "column_name_hash STRING NOT NULL," +

            "catalog_name STRING NOT NULL," +
            "db_name STRING NOT NULL," +
            "table_name STRING NOT NULL," +
            "column_name STRING NOT NULL," +
            "usage STRING NOT NULL," +
            "last_used DATETIME NOT NULL," +
            "created DATETIME DEFAULT CURRENT_TIMESTAMP" +
            ") " +
            "PRIMARY KEY(fe_id, table_uuid, column_name_hash) " +
            "DISTRIBUTED BY HASH(fe_id, table_uuid, column_name_hash) BUCKETS 8\n" +
            "PROPERTIES('replication_num'='1')";

    private static final TableKeeper KEEPER = new TableKeeper(DATABASE_NAME, TABLE_NAME, TABLE_DDL, null);

    private static final VelocityEngine DEFAULT_VELOCITY_ENGINE;

    static {
        DEFAULT_VELOCITY_ENGINE = new VelocityEngine();
        DEFAULT_VELOCITY_ENGINE.setProperty(VelocityEngine.RUNTIME_LOG_REFERENCE_LOG_INVALID, false);
    }

    private static final int INSERT_BATCH_SIZE = 1024;
    private static final String SQL_COLUMN_LIST =
            "fe_id, table_uuid, column_name_hash, catalog_name, db_name, table_name, column_name, usage, last_used ";

    private static final String ADD_RECORD = "INSERT INTO " + TABLE_FULL_NAME + "(" + SQL_COLUMN_LIST + ") VALUES ";
    private static final String INSERT_VALUE =
            "('$feId', '$tableUuidHash', '$columnNameHash', '$catalogName', '$dbName', '$tableName', " +
                    "'$columnName', '$usage', '$lastUsed')";

    private static final String QUERY =
            "SELECT fe_id, table_uuid, column_name, catalog_name, db_name, table_name, usage, last_used, created FROM "
                    + TABLE_FULL_NAME + " WHERE true ";
    private static final String WHERE_THIS_FE = "fe_id = '$feId'";

    private static final String VACUUM = "DELETE FROM " + TABLE_FULL_NAME +
            " WHERE fe_id='$feId' AND last_used < '$lastUsed'";

    private static final ExternalPredicateColumnsStorage INSTANCE = new ExternalPredicateColumnsStorage();

    /**
     * MIN: the persisted state has not been restored, we cannot persist
     */
    private final LocalDateTime systemStartTime = TimeUtils.getSystemNow();
    private volatile LocalDateTime lastPersist = LocalDateTime.MIN;

    private final SimpleExecutor executor;

    public static TableKeeper createKeeper() {
        return KEEPER;
    }

    public static ExternalPredicateColumnsStorage getInstance() {
        return INSTANCE;
    }

    public ExternalPredicateColumnsStorage() {
        this.executor = new SimpleExecutor("external_predicate_column", TResultSinkType.HTTP_PROTOCAL);
        // Set the DOP to 1 to reduce impact on normal queries
        this.executor.setDop(1);
    }

    public ExternalPredicateColumnsStorage(SimpleExecutor executor) {
        this.executor = executor;
    }

    /**
     * Query state from all FEs for a single table (identified by its hashed table_uuid)
     */
    public List<ExternalColumnUsage> queryGlobalState(String tableUuidHash, EnumSet<ColumnUsage.UseCase> useCases) {
        StringBuilder sb = new StringBuilder(QUERY);
        sb.append(" AND `table_uuid` = '").append(SqlUtils.escapeSqlString(tableUuidHash)).append("'");

        if (!useCases.isEmpty()) {
            String useCaseRegex = useCases.stream().map(ColumnUsage.UseCase::toString).collect(Collectors.joining("|"));
            sb.append(String.format(" AND regexp(`usage`, '%s')", useCaseRegex));
        }

        String sql = sb.toString();
        List<TResultBatch> tResultBatches = executor.executeDQL(sql);
        List<ExternalColumnUsage> columnUsages = resultToColumnUsage(tResultBatches);
        if (GlobalStateMgr.getCurrentState().getNodeMgr().getFrontends().size() == 1) {
            return columnUsages;
        } else {
            return deduplicateColumnUsages(columnUsages);
        }
    }

    public List<ExternalColumnUsage> deduplicateColumnUsages(List<ExternalColumnUsage> columnUsages) {
        Map<ExternalColumnUsage, ExternalColumnUsage> merged = columnUsages.stream()
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
    public void persist(Collection<ExternalColumnUsage> columnUsageList) {
        if (lastPersist == LocalDateTime.MIN) {
            return;
        }
        LocalDateTime nextPersist = TimeUtils.getSystemNow();
        Stopwatch watch = Stopwatch.createStarted();
        List<ExternalColumnUsage> diff =
                columnUsageList.stream().filter(x -> x.needPersist(lastPersist)).collect(Collectors.toList());
        for (var batch : ListUtils.partition(diff, INSERT_BATCH_SIZE)) {
            persistDiff(batch);
        }
        lastPersist = nextPersist;
        String elapsed = watch.toString();
        LOG.info("persist {} diffed external predicate columns elapsed {}, update lastPersist to {}", diff.size(),
                elapsed, lastPersist);
    }

    private void persistDiff(List<ExternalColumnUsage> diff) {
        StringBuilder insert = new StringBuilder(ADD_RECORD);
        boolean first = true;
        for (ExternalColumnUsage usage : diff) {
            StringWriter sw = new StringWriter();

            // catalog/db/table/column names come from external catalog metadata and are not trusted;
            // escape before embedding them in the single-quoted SQL literals of INSERT_VALUE.
            VelocityContext context = new VelocityContext();
            context.put("feId", GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf().getFid());
            context.put("tableUuidHash", SqlUtils.escapeSqlString(usage.getTableUuidHash()));
            context.put("columnNameHash", SqlUtils.escapeSqlString(usage.getColumnNameHash()));
            context.put("columnName", SqlUtils.escapeSqlString(usage.getColumnName()));
            context.put("catalogName", SqlUtils.escapeSqlString(usage.getCatalogName()));
            context.put("dbName", SqlUtils.escapeSqlString(usage.getDbName()));
            context.put("tableName", SqlUtils.escapeSqlString(usage.getTableName()));
            context.put("usage", SqlUtils.escapeSqlString(usage.getUseCaseString()));
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
     * Restore this FE's own state
     */
    public List<ExternalColumnUsage> restore() {
        String selfName = String.valueOf(GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf().getFid());

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
     * Remove this FE's own records if the lastUsed < ttlTime
     */
    public void vacuum(LocalDateTime ttlTime) {
        String selfName = String.valueOf(GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf().getFid());

        VelocityContext context = new VelocityContext();
        context.put("feId", selfName);
        context.put("lastUsed", DateUtils.formatDateTimeUnix(ttlTime));

        StringWriter sw = new StringWriter();
        DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", VACUUM);

        String sql = sw.toString();
        executor.executeDML(sql);

        LOG.info("vacuum external column usage from storage before {}", ttlTime);
    }

    public boolean isSystemTableReady() {
        return KEEPER.isReady();
    }

    public boolean isRestored() {
        return lastPersist != LocalDateTime.MIN;
    }

    public void finishRestore() {
        lastPersist = systemStartTime;
        LOG.info("finish restore external predicate columns state, update lastPersist to {}", lastPersist);
    }

    @VisibleForTesting
    protected void finishRestore(LocalDateTime lastPersistTime) {
        lastPersist = lastPersistTime;
    }

    @VisibleForTesting
    static class ExternalColumnUsageJsonRecord {

        @SerializedName("data")
        List<ExternalColumnUsage> data;

        public ExternalColumnUsageJsonRecord() {
            this.data = Lists.newArrayList();
        }

        /**
         * {
         * "data": [fe_id, table_uuid, column_name, catalog_name, db_name, table_name, usage, last_used, created]
         * }
         */
        public static ExternalColumnUsageJsonRecord fromJson(String json) {
            JsonElement object = JsonParser.parseString(json);
            JsonArray data = object.getAsJsonObject().get("data").getAsJsonArray();
            // String feId = data.get(0).getAsString();
            String tableUuidHash = data.get(1).getAsString();
            String columnName = data.get(2).getAsString();
            String catalogName = data.get(3).getAsString();
            String dbName = data.get(4).getAsString();
            String tableName = data.get(5).getAsString();
            String useCase = data.get(6).getAsString();
            String lastUsed = data.get(7).getAsString();
            String created = data.get(8).getAsString();

            EnumSet<ColumnUsage.UseCase> useCases = ColumnUsage.fromUseCaseString(useCase);
            ExternalColumnUsage usage =
                    new ExternalColumnUsage(tableUuidHash, catalogName, dbName, tableName, columnName, useCases);
            usage.setLastUsed(DateUtils.parseUnixDateTime(lastUsed));
            usage.setCreated(DateUtils.parseUnixDateTime(created));

            ExternalColumnUsageJsonRecord res = new ExternalColumnUsageJsonRecord();
            res.data = List.of(usage);
            return res;
        }

    }

    @VisibleForTesting
    protected static List<ExternalColumnUsage> resultToColumnUsage(List<TResultBatch> batches) {
        List<ExternalColumnUsage> res = new ArrayList<>();
        for (TResultBatch batch : ListUtils.emptyIfNull(batches)) {
            for (ByteBuffer buffer : batch.getRows()) {
                ByteBuf copied = Unpooled.copiedBuffer(buffer);
                String jsonString = copied.toString(Charset.defaultCharset());
                try {
                    List<ExternalColumnUsage> records =
                            ListUtils.emptyIfNull(ExternalColumnUsageJsonRecord.fromJson(jsonString).data);
                    res.addAll(records);
                } catch (Exception e) {
                    LOG.warn("failed to deserialize ExternalColumnUsage record: {}", jsonString, e);
                }
            }
        }
        return res;
    }

}
