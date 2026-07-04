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

package com.starrocks.statistic;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.iceberg.Snapshot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ExternalSampleStatisticsCollectJob extends ExternalFullStatisticsCollectJob {
    private static final Logger LOG = LogManager.getLogger(ExternalSampleStatisticsCollectJob.class);

    // external_column_statistics is keyed by (table_uuid, partition_name, column_name); a
    // whole-table file sample has no real partition to attach its row to, so it uses this fixed
    // sentinel instead. Must never collide with a real Iceberg partition name. The read path
    // (StatisticSQLBuilder's hll_union_agg(...) GROUP BY table_uuid, column_name) doesn't group by
    // partition_name, so this row is unioned in alongside any real per-partition rows with no
    // read-path change.
    public static final String SAMPLE_PARTITION_NAME_SENTINEL = "$FILE_SAMPLE$";

    // AnalyzeStatus.properties key persisting the round index this table's sampling left off at,
    // so the NEXT scheduled run's internal round loop can start near there instead of always
    // cold-starting at round 0 (whose ratio is superseded within the same run anyway once later,
    // larger rounds overwrite it). Connector-agnostic name: the round-loop driver itself doesn't
    // depend on Iceberg, only the cheap cost-total lookup that feeds it does (see
    // collectIcebergWithFileSampling) -- if another connector eventually plugs into this same
    // driver, its runs should share this warm-start record rather than getting a separate one.
    private static final String SAMPLE_ROUND_PROPERTY = "external_sample_round";

    private static final String SAMPLE_WHOLE_TABLE_TEMPLATE = "SELECT cast($version as INT)" +
            ", '$partitionNameStr'" + // VARCHAR
            ", '$columnNameStr'" + // VARCHAR
            ", cast(COUNT(1) as BIGINT)" + // BIGINT
            ", cast($dataSize as BIGINT)" + // BIGINT
            ", $hllFunction" + // VARBINARY
            ", cast($countNullFunction as BIGINT)" + // BIGINT
            ", $maxFunction" + // VARCHAR
            ", $minFunction " + // VARCHAR
            " FROM `$catalogName`.`$dbName`.`$tableName`";

    private final int allPartitionSize;

    public ExternalSampleStatisticsCollectJob(String catalogName, Database db, Table table, List<String> partitionNames,
                                              List<String> columnNames, List<Type> columnTypes,
                                              StatsConstants.AnalyzeType type, StatsConstants.ScheduleType scheduleType,
                                              Map<String, String> properties, int allPartitionSize) {
        super(catalogName, db, table, partitionNames, columnNames, columnTypes, type, scheduleType, properties);
        this.allPartitionSize = allPartitionSize;
    }

    public Set<Long> getSampledPartitionsHashValue() {
        HashFunction hashFunction = Hashing.murmur3_128();
        return partitionNames.stream().map(s -> hashFunction.hashUnencodedChars(s).asLong()).collect(Collectors.toSet());
    }

    public int getAllPartitionSize() {
        return allPartitionSize;
    }

    @Override
    public String getName() {
        return "ExternalSample";
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        if (table.isIcebergTable()) {
            // Iceberg: cross-partition file-level random sampling, hard IO cap regardless of how
            // large any single partition is. Everything else (Hive/Hudi/Paimon) keeps the existing
            // partition-subset-then-full-scan sampling below.
            collectIcebergWithFileSampling(context, analyzeStatus);
        } else {
            super.collect(context, analyzeStatus);
        }
    }

    private void collectIcebergWithFileSampling(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        long startMs = System.currentTimeMillis();
        long jobId = analyzeStatus.getId();

        IcebergTable icebergTable = (IcebergTable) table;
        Snapshot snapshot = icebergTable.getNativeTable().currentSnapshot();
        if (snapshot == null) {
            LOG.info("[ExternalStats][Sample] jobId={} catalog={} db={} table={} has no snapshot yet, skip",
                    jobId, getCatalogName(), db.getOriginName(), table.getName());
            return;
        }

        // Read file_count/row_count first (manifest-list summary fields, zero data IO) so every
        // round's ratio is derived from the table's actual current size -- never a blind first
        // guess. Row count (not file count) drives the ratio: files can vary wildly in row count,
        // so a file-count-based ratio gives a much less predictable row volume per round.
        // file_count is kept only as informational logging context.
        long fileCount = IcebergMetadata.countIcebergFilesFromManifestList(icebergTable, snapshot.snapshotId());
        long rowCount = IcebergMetadata.countIcebergRowsFromManifestList(icebergTable, snapshot.snapshotId());
        long capPerRound = Math.max(1, Config.statistic_external_sample_max_rows_per_round);
        int maxRounds = Math.max(1, Config.statistic_external_sample_max_rounds);

        // Warm-start near where the last finished run left off, instead of always redoing rounds
        // 0..N-1 that would just get overwritten again within this run anyway.
        int round = Math.min(findPersistedRound(), maxRounds - 1);
        double ratio = 1.0;

        LOG.info("[ExternalStats][Sample] collect start | jobId={} catalog={} db={} table={} fileCount={} " +
                        "rowCount={} startRound={} capPerRound={} maxRounds={} columns={}",
                jobId, getCatalogName(), db.getOriginName(), table.getName(), fileCount, rowCount, round, capPerRound,
                maxRounds, columnNames.size());

        String status = "SUCCESS";
        String failureReason = "";
        try {
            for (; round < maxRounds; round++) {
                long rowsThisRound = rowCount > 0 ? Math.min(rowCount, capPerRound << round) : rowCount;
                ratio = rowCount > 0 ? Math.min(1.0, (double) rowsThisRound / rowCount) : 1.0;

                runOneSampleRound(context, analyzeStatus, ratio);

                LOG.info("[ExternalStats][Sample] round done | jobId={} catalog={} db={} table={} round={} " +
                                "rowsThisRound={} ratio={}",
                        jobId, getCatalogName(), db.getOriginName(), table.getName(), round, rowsThisRound, ratio);

                if (ratio >= 1.0) {
                    // Full coverage reached -- later, smaller-cap rounds would be strictly worse
                    // and would just waste a re-scan, so stop here.
                    break;
                }
            }

            Map<String, String> mergedProperties = new LinkedHashMap<>();
            if (analyzeStatus.getProperties() != null) {
                mergedProperties.putAll(analyzeStatus.getProperties());
            }
            mergedProperties.put("table_format", table.getType().name().toLowerCase(Locale.ROOT));
            mergedProperties.put("column_count", String.valueOf(columnNames.size()));
            mergedProperties.put("file_count", String.valueOf(fileCount));
            mergedProperties.put("row_count", String.valueOf(rowCount));
            mergedProperties.put("sample_ratio", String.valueOf(ratio));
            mergedProperties.put(SAMPLE_ROUND_PROPERTY, String.valueOf(Math.min(round, maxRounds - 1)));
            analyzeStatus.setProperties(mergedProperties);
        } catch (Exception e) {
            status = "FAILED";
            failureReason = e.getMessage();
            throw e;
        } finally {
            LOG.info("[ExternalStats][Sample] collect end | jobId={} catalog={} db={} table={} status={} " +
                            "durationMs={} fileCount={} rowCount={} finalRatio={} reason={}",
                    jobId, getCatalogName(), db.getOriginName(), table.getName(), status,
                    System.currentTimeMillis() - startMs, fileCount, rowCount, ratio, failureReason);
        }
    }

    /**
     * Runs one whole-table sampled collection round at the given ratio and flushes it to
     * external_column_statistics (overwriting the previous round's sentinel row).
     */
    private void runOneSampleRound(ConnectContext context, AnalyzeStatus analyzeStatus, double ratio) throws Exception {
        // seed=0 tells BernoulliFileScanTaskIterator to use a fresh, unseeded Random, so successive
        // rounds don't keep re-sampling the same files.
        context.getSessionVariable().setExternalStatsFileSampleRatio(ratio);
        context.getSessionVariable().setExternalStatsSampleSeed(0);
        try {
            int parallelism = Math.max(1, context.getSessionVariable().getStatisticCollectParallelism());
            List<List<String>> collectSQLList = buildSampleCollectSQLList(parallelism);
            long totalCollectSQL = collectSQLList.size();
            long finishedSQLNum = 0;
            for (List<String> sqlUnion : collectSQLList) {
                if (sqlUnion.size() < parallelism) {
                    context.getSessionVariable().setPipelineDop(parallelism / sqlUnion.size());
                } else {
                    context.getSessionVariable().setPipelineDop(1);
                }
                String sql = Joiner.on(" UNION ALL ").join(sqlUnion);
                collectSampledStatisticSync(sql, context, analyzeStatus, ratio);
                finishedSQLNum++;
                analyzeStatus.setProgress(finishedSQLNum * 100 / totalCollectSQL);
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddAnalyzeStatus(analyzeStatus);
            }
            flushInsertStatisticsData(context, true);
        } finally {
            // Never let sampling leak onto later, unrelated queries sharing this ConnectContext.
            context.getSessionVariable().setExternalStatsFileSampleRatio(1.0);
            context.getSessionVariable().setExternalStatsSampleSeed(0);
        }
    }

    /**
     * Finds the round index persisted by this table's most recently finished sample-ANALYZE run,
     * so this run's internal round loop can warm-start near there instead of always cold-starting
     * at round 0. Defaults to 0 if no prior finished run is found.
     */
    private int findPersistedRound() {
        Map<Long, AnalyzeStatus> statusMap = GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAnalyzeStatusMap();
        int round = 0;
        LocalDateTime latestEnd = null;
        for (AnalyzeStatus otherStatus : statusMap.values()) {
            if (otherStatus.isNative() || otherStatus.getType() != StatsConstants.AnalyzeType.SAMPLE) {
                continue;
            }
            if (otherStatus.getStatus() != StatsConstants.ScheduleStatus.FINISH) {
                continue;
            }
            try {
                if (!getCatalogName().equals(otherStatus.getCatalogName())
                        || !db.getOriginName().equals(otherStatus.getDbName())
                        || !table.getName().equals(otherStatus.getTableName())) {
                    continue;
                }
            } catch (MetaNotFoundException e) {
                continue;
            }
            LocalDateTime end = otherStatus.getEndTime();
            if (end == null) {
                continue;
            }
            if (latestEnd == null || end.isAfter(latestEnd)) {
                latestEnd = end;
                String roundStr = otherStatus.getProperties() == null ? null :
                        otherStatus.getProperties().get(SAMPLE_ROUND_PROPERTY);
                try {
                    round = roundStr != null ? Integer.parseInt(roundStr) : 0;
                } catch (NumberFormatException e) {
                    round = 0;
                }
            }
        }
        return round;
    }

    private List<List<String>> buildSampleCollectSQLList(int parallelism) {
        List<String> totalQuerySQL = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            totalQuerySQL.add(buildSampleCollectSQL(columnNames.get(i), columnTypes.get(i)));
        }
        return Lists.partition(totalQuerySQL, parallelism);
    }

    private String buildSampleCollectSQL(String columnName, Type columnType) {
        VelocityContext context = new VelocityContext();

        String columnNameStr = StringEscapeUtils.escapeSql(columnName);
        String quoteColumnName = StatisticUtils.quoting(table, columnName);

        context.put("version", StatsConstants.STATISTIC_EXTERNAL_VERSION);
        context.put("partitionNameStr", SAMPLE_PARTITION_NAME_SENTINEL);
        context.put("columnNameStr", columnNameStr);
        context.put("dataSize", fullAnalyzeGetDataSize(quoteColumnName, columnType));
        context.put("dbName", db.getOriginName());
        context.put("tableName", table.getName());
        context.put("catalogName", getCatalogName());

        if (!columnType.canStatistic() || columnType.isCollectionType()) {
            context.put("hllFunction", "hex(hll_serialize(hll_empty()))");
            context.put("countNullFunction", "0");
            context.put("maxFunction", "''");
            context.put("minFunction", "''");
        } else {
            context.put("hllFunction", "hex(hll_serialize(IFNULL(hll_raw(" + quoteColumnName + "), hll_empty())))");
            context.put("countNullFunction", "COUNT(1) - COUNT(" + quoteColumnName + ")");
            context.put("maxFunction", getMinMaxFunction(columnType, quoteColumnName, true));
            context.put("minFunction", getMinMaxFunction(columnType, quoteColumnName, false));
        }

        return build(context, SAMPLE_WHOLE_TABLE_TEMPLATE);
    }

    /**
     * Mirrors {@link ExternalFullStatisticsCollectJob#collectStatisticSync}, but scales the sampled
     * row_count back up to a full-table estimate before buffering. NDV (HLL) and min/max are NOT
     * rescaled: HLL directly measures distinct values in the sampled rows (assumed representative
     * of the whole table per random-wide-sampling); min/max are kept as the sampled rows' true
     * values, which is a safe (conservative) direction for range-predicate selectivity.
     */
    private void collectSampledStatisticSync(String sql, ConnectContext context, AnalyzeStatus analyzeStatus, double ratio)
            throws Exception {
        calculateAndSetRemainingTimeout(context, analyzeStatus);

        LOG.debug("statistics collect sql : " + sql);
        StatisticExecutor executor = new StatisticExecutor();
        setDefaultSessionVariable(context);

        List<TStatisticData> dataList = executor.executeStatisticDQL(context, sql);
        for (TStatisticData data : dataList) {
            long scaledRowCount = (ratio > 0 && ratio < 1.0) ? (long) Math.ceil(data.getRowCount() / ratio) : data.getRowCount();

            List<String> params = Lists.newArrayList();
            List<Expr> row = Lists.newArrayList();

            params.add("'" + table.getUUID() + "'");
            params.add("'" + StringEscapeUtils.escapeSql(data.getPartitionName()) + "'");
            params.add("'" + StringEscapeUtils.escapeSql(data.getColumnName()) + "'");
            params.add("'" + getCatalogName() + "'");
            params.add("'" + db.getOriginName() + "'");
            params.add("'" + table.getName() + "'");
            params.add(String.valueOf(scaledRowCount));
            params.add(String.valueOf(data.getDataSize()));
            params.add("hll_deserialize(unhex('mockData'))");
            params.add(String.valueOf(data.getNullCount()));
            params.add("'" + data.getMax() + "'");
            params.add("'" + data.getMin() + "'");
            params.add("now()");

            row.add(new StringLiteral(table.getUUID()));
            row.add(new StringLiteral(data.getPartitionName()));
            row.add(new StringLiteral(data.getColumnName()));
            row.add(new StringLiteral(getCatalogName()));
            row.add(new StringLiteral(db.getOriginName()));
            row.add(new StringLiteral(table.getName()));
            row.add(new IntLiteral(scaledRowCount, IntegerType.BIGINT));
            row.add(new IntLiteral((long) data.getDataSize(), IntegerType.BIGINT));
            row.add(hllDeserialize(data.getHll()));
            row.add(new IntLiteral(data.getNullCount(), IntegerType.BIGINT));
            row.add(new StringLiteral(data.getMax()));
            row.add(new StringLiteral(data.getMin()));
            row.add(nowFn());

            rowsBuffer.add(row);
            sqlBuffer.add("(" + String.join(", ", params) + ")");
        }
        flushInsertStatisticsData(context, false);
    }
}
