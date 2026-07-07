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

import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.gson.JsonArray;
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
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.iceberg.Snapshot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ExternalSampleStatisticsCollectJob extends ExternalFullStatisticsCollectJob {
    private static final Logger LOG = LogManager.getLogger(ExternalSampleStatisticsCollectJob.class);

    // AnalyzeStatus.properties key persisting the round index this table's sampling left off at,
    // so the NEXT scheduled run's internal round loop can start near there instead of always
    // cold-starting at round 0 (whose ratio is superseded within the same run anyway once later,
    // larger rounds overwrite it). Connector-agnostic name: the round-loop driver itself doesn't
    // depend on Iceberg, only the cheap cost-total lookup that feeds it does (see
    // collectIcebergWithFileSampling) -- if another connector eventually plugs into this same
    // driver, its runs should share this warm-start record rather than getting a separate one.
    private static final String SAMPLE_ROUND_PROPERTY = "external_sample_round";

    // A wide-row SQL's SELECT list grows with column count; past this many columns in one batch,
    // split into multiple single-pass SQLs (each still one scan over its own batch of columns)
    // rather than building one unbounded SELECT list.
    private static final int MAX_COLUMNS_PER_SINGLE_PASS_SQL = 200;

    // Per-column aggregate count in buildSinglePassSQL's wide row: data_size, hll, null_count,
    // max, min, ndv cardinality.
    private static final int AGGREGATES_PER_COLUMN = 6;

    // StatisticsUtils#estimateColumnStatistics rescales a SAMPLE job's stored row_count by
    // allPartitionSize / sampledPartitionsHashValue().size() -- a correction for the legacy
    // Hive-style "latest N partitions, full scan" sampling, where the stored row_count only covers
    // the sampled partition subset. Iceberg's whole-table file sampling already rescales row_count
    // (and now data_size/null_count) to a full-table estimate during collection (see
    // collectSinglePassStatisticSync), so that downstream rescale must be a no-op here. Reporting a
    // fixed 1-sampled-out-of-1-total split (rather than the factory's real, and irrelevant,
    // "latest N partitions" pre-selection) keeps the ratio at exactly 1.0x. A single constant
    // element (not a fresh Set per call) keeps StatisticExecutor's cross-run union at size 1 too.
    private static final Set<Long> FILE_SAMPLE_NEUTRAL_PARTITION_HASH = Set.of(0L);
    private static final int FILE_SAMPLE_NEUTRAL_PARTITION_SIZE = 1;

    private final int allPartitionSize;

    // Set once collectIcebergWithFileSampling actually runs (i.e. this job is against an Iceberg
    // table). See FILE_SAMPLE_NEUTRAL_PARTITION_HASH for why this matters downstream. Package-
    // private (not private) so tests can drive the neutral-value branch without needing a live
    // Iceberg snapshot.
    volatile boolean usedFileSampling = false;

    public ExternalSampleStatisticsCollectJob(String catalogName, Database db, Table table, List<String> partitionNames,
                                              List<String> columnNames, List<Type> columnTypes,
                                              StatsConstants.AnalyzeType type, StatsConstants.ScheduleType scheduleType,
                                              Map<String, String> properties, int allPartitionSize) {
        super(catalogName, db, table, partitionNames, columnNames, columnTypes, type, scheduleType, properties);
        this.allPartitionSize = allPartitionSize;
    }

    public Set<Long> getSampledPartitionsHashValue() {
        if (usedFileSampling) {
            return FILE_SAMPLE_NEUTRAL_PARTITION_HASH;
        }
        HashFunction hashFunction = Hashing.murmur3_128();
        return partitionNames.stream().map(s -> hashFunction.hashUnencodedChars(s).asLong()).collect(Collectors.toSet());
    }

    public int getAllPartitionSize() {
        return usedFileSampling ? FILE_SAMPLE_NEUTRAL_PARTITION_SIZE : allPartitionSize;
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
        usedFileSampling = true;
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
            // Compared against each round's cardinality to detect NDV convergence; null on the
            // first round of this job run (warm-started rounds don't carry over the prior job
            // run's per-column values, so convergence is only tracked within one job execution).
            Map<String, Long> prevCardinalityByColumn = null;
            double convergenceThreshold = Config.statistic_external_sample_ndv_convergence_threshold;
            for (; round < maxRounds; round++) {
                long rowsThisRound = rowCount > 0 ? Math.min(rowCount, capPerRound << round) : rowCount;
                ratio = rowCount > 0 ? Math.min(1.0, (double) rowsThisRound / rowCount) : 1.0;

                Map<String, Long> cardinalityByColumn = runOneSampleRound(context, analyzeStatus, ratio);

                // On tables with relatively few splits, Bernoulli sampling at low ratios can drop
                // every split.  With p=(1-ratio)^splitCount, a 15-split table at ratio=0.13 has a
                // ~12 % chance of dropping everything, and the sentinel stats row would be all-zero
                // — the subsequent cleanup would then erase authoritative FULL rows.
                //
                // Retry this round with ratio=1.0 (full scan).  Tables small enough to hit this
                // edge case have negligible full-scan cost; the retry also produces better-quality
                // stats than any partial-keep heuristic would.  The PK-based sentinel overwrite
                // guarantees no zero-row-row survives; if *both* attempts return empty (table truly
                // empty), that is the correct answer.
                if (cardinalityByColumn.isEmpty() && fileCount > 0) {
                    LOG.warn("[ExternalStats][Sample] all splits Bernoulli-dropped at ratio={} " +
                                    "fileCount={} rowCount={}, retrying with full scan",
                            ratio, fileCount, rowCount);
                    cardinalityByColumn = runOneSampleRound(context, analyzeStatus, 1.0);
                }

                LOG.info("[ExternalStats][Sample] round done | jobId={} catalog={} db={} table={} round={} " +
                                "rowsThisRound={} ratio={}",
                        jobId, getCatalogName(), db.getOriginName(), table.getName(), round, rowsThisRound, ratio);

                if (ratio >= 1.0) {
                    // Full coverage reached -- later, smaller-cap rounds would be strictly worse
                    // and would just waste a re-scan, so stop here.
                    break;
                }

                if (prevCardinalityByColumn != null
                        && isNdvConverging(prevCardinalityByColumn, cardinalityByColumn, convergenceThreshold)) {
                    LOG.info("[ExternalStats][Sample] NDV converged, stopping early | jobId={} catalog={} db={} " +
                                    "table={} round={} threshold={}",
                            jobId, getCatalogName(), db.getOriginName(), table.getName(), round, convergenceThreshold);
                    break;
                }
                prevCardinalityByColumn = cardinalityByColumn;
            }

            // All rounds succeeded: this table's sampled stats for these columns are now
            // authoritative in the sentinel row. If a prior FULL collection left real per-partition
            // rows behind for the same columns, the read path's sum(row_count)/sum(data_size)/
            // sum(null_count) (GROUP BY table_uuid, column_name, not partition_name) would double-
            // count them alongside the sentinel row. Clean those up now that we have a fresh,
            // authoritative replacement -- never before, so stats are never momentarily absent.
            cleanupStaleFullCollectionRows(context);

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
     * Deletes any real-partition rows (partition_name != sentinel) for this table's collected
     * columns, left behind by a prior FULL collection. Best-effort: failure here must not fail an
     * otherwise-successful sample collection -- the double-counting it would leave behind already
     * exists today, so a failed cleanup attempt does not make things worse.
     */
    private void cleanupStaleFullCollectionRows(ConnectContext context) {
        try {
            new StatisticExecutor().dropExternalTableStatisticsExcludingPartition(context, table.getUUID(),
                    StatsConstants.EXTERNAL_SAMPLE_PARTITION_NAME_SENTINEL, columnNames);
        } catch (Exception e) {
            LOG.warn("[ExternalStats][Sample] failed to clean up stale FULL rows | catalog={} db={} table={}",
                    getCatalogName(), db.getOriginName(), table.getName(), e);
        }
    }

    /**
     * Runs one whole-table sampled collection round at the given ratio and flushes it to
     * external_column_statistics (overwriting the previous round's sentinel row). Returns each
     * collected column's NDV cardinality for this round, so the caller can track convergence
     * across rounds.
     */
    private Map<String, Long> runOneSampleRound(ConnectContext context, AnalyzeStatus analyzeStatus, double ratio)
            throws Exception {
        // seed=0 tells BernoulliFileScanTaskIterator to use a fresh, unseeded Random, so successive
        // rounds don't keep re-sampling the same files.
        context.getSessionVariable().setExternalStatsFileSampleRatio(ratio);
        context.getSessionVariable().setExternalStatsSampleSeed(0);
        try {
            List<List<Integer>> columnBatches = Lists.partition(
                    IntStream.range(0, columnNames.size()).boxed().collect(Collectors.toList()),
                    MAX_COLUMNS_PER_SINGLE_PASS_SQL);
            long totalBatches = columnBatches.size();
            long finishedBatches = 0;
            Map<String, Long> cardinalityByColumn = new LinkedHashMap<>();
            for (List<Integer> columnBatch : columnBatches) {
                String sql = buildSinglePassSQL(columnBatch);
                cardinalityByColumn.putAll(collectSinglePassStatisticSync(sql, columnBatch, context, analyzeStatus, ratio));
                finishedBatches++;
                analyzeStatus.setProgress(finishedBatches * 100 / totalBatches);
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddAnalyzeStatus(analyzeStatus);
            }
            flushInsertStatisticsData(context, true);
            return cardinalityByColumn;
        } finally {
            // Never let sampling leak onto later, unrelated queries sharing this ConnectContext.
            context.getSessionVariable().setExternalStatsFileSampleRatio(1.0);
            context.getSessionVariable().setExternalStatsSampleSeed(0);
        }
    }

    /**
     * True when every column's NDV cardinality changed by less than {@code threshold} (relative)
     * between two consecutive rounds. A single still-changing column (typically a high-cardinality
     * one) is enough to keep sampling going -- stopping early on ratio alone would compound the
     * NDV underestimation that already applies to high-cardinality columns under sampling.
     */
    static boolean isNdvConverging(Map<String, Long> prev, Map<String, Long> curr, double threshold) {
        for (Map.Entry<String, Long> entry : curr.entrySet()) {
            Long prevValue = prev.get(entry.getKey());
            if (prevValue == null) {
                return false;
            }
            long currValue = entry.getValue();
            double denominator = Math.max(currValue, 1L);
            double relativeChange = Math.abs(currValue - prevValue) / denominator;
            if (relativeChange >= threshold) {
                return false;
            }
        }
        return true;
    }

    /**
     * Finds the round index persisted by this table's most recently finished sample-ANALYZE run,
     * so this run's internal round loop can warm-start near there instead of always cold-starting
     * at round 0. Defaults to 0 if no prior finished run is found.
     */
    int findPersistedRound() {
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

    /**
     * Builds one single-pass SQL covering every column in {@code columnBatch}: a single scan
     * producing one wide row of {@code COUNT(1)} (shared row count) followed by 6 aggregates
     * (data_size, hll, null_count, max, min, ndv cardinality) per column, in column order.
     * Replaces the previous per-column SELECT ... UNION ALL ... approach so a round does exactly
     * one planFiles()/scan (per batch) instead of N, and every column's stats come from the exact
     * same sampled rows. The ndv cardinality aggregate is a plain scalar (not the serialized HLL
     * sketch) so the round loop can compare it across rounds in-memory without needing to
     * deserialize an HLL sketch on the FE side -- see isNdvConverging.
     */
    String buildSinglePassSQL(List<Integer> columnBatch) {
        StringBuilder sql = new StringBuilder("SELECT CAST(COUNT(1) AS BIGINT)");
        for (int idx : columnBatch) {
            String columnName = columnNames.get(idx);
            Type columnType = columnTypes.get(idx);
            String quoteColumnName = StatisticUtils.quoting(table, columnName);

            if (!columnType.canStatistic() || columnType.isCollectionType()) {
                sql.append(", CAST(0 AS BIGINT)");
                sql.append(", hex(hll_serialize(hll_empty()))");
                sql.append(", CAST(0 AS BIGINT)");
                sql.append(", ''");
                sql.append(", ''");
                sql.append(", CAST(0 AS BIGINT)");
            } else {
                sql.append(", CAST(").append(fullAnalyzeGetDataSize(quoteColumnName, columnType)).append(" AS BIGINT)");
                sql.append(", hex(hll_serialize(IFNULL(hll_raw(").append(quoteColumnName).append("), hll_empty())))");
                sql.append(", CAST(COUNT(1) - COUNT(").append(quoteColumnName).append(") AS BIGINT)");
                sql.append(", ").append(getMinMaxFunction(columnType, quoteColumnName, true));
                sql.append(", ").append(getMinMaxFunction(columnType, quoteColumnName, false));
                sql.append(", CAST(hll_cardinality(IFNULL(hll_raw(").append(quoteColumnName)
                        .append("), hll_empty())) AS BIGINT)");
            }
        }
        sql.append(" FROM `").append(getCatalogName()).append("`.`").append(db.getOriginName())
                .append("`.`").append(table.getName()).append("`");
        return sql.toString();
    }

    /**
     * Executes one single-pass wide-row SQL and decodes its one output row (via the HTTP/JSON
     * result protocol, since the fixed 9-field {@code TStatisticData} struct can't hold N columns'
     * worth of aggregates) back into per-column records, scaling the shared sampled row_count back
     * up to a full-table estimate. NDV (HLL) and min/max are NOT rescaled: HLL directly measures
     * distinct values in the sampled rows (assumed representative of the whole table per
     * random-wide-sampling); min/max are kept as the sampled rows' true values, which is a safe
     * (conservative) direction for range-predicate selectivity. Returns each column's NDV
     * cardinality (unscaled, same sample-local estimate as everything else here) so the caller can
     * compare it against the previous round's value to detect convergence.
     */
    Map<String, Long> collectSinglePassStatisticSync(String sql, List<Integer> columnBatch,
                                                      ConnectContext context, AnalyzeStatus analyzeStatus,
                                                      double ratio) throws Exception {
        calculateAndSetRemainingTimeout(context, analyzeStatus);

        LOG.debug("statistics collect sql : " + sql);
        StatisticExecutor executor = new StatisticExecutor();
        setDefaultSessionVariable(context);

        List<JsonArray> rows = executor.executeWideRowDQL(context, sql);
        if (rows.isEmpty()) {
            return Map.of();
        }
        JsonArray data = rows.get(0);
        long sampledRowCount = data.get(0).getAsLong();
        long scaledRowCount = (ratio > 0 && ratio < 1.0) ? (long) Math.ceil(sampledRowCount / ratio) : sampledRowCount;

        // table_uuid is stored hashed (StatisticUtils.hashTableUuidForPkStorage) to stay within
        // BE's primary_key_limit_size -- see ExternalFullStatisticsCollectJob#collectStatisticSync,
        // which does the same for the FULL path. Iceberg's long catalog.db.table.<uuid> identifiers
        // are exactly the case that hashing exists to protect.
        String hashedTableUuid = StatisticUtils.hashTableUuidForPkStorage(table.getUUID());
        Map<String, Long> cardinalityByColumn = new LinkedHashMap<>();
        for (int i = 0; i < columnBatch.size(); i++) {
            String columnName = columnNames.get(columnBatch.get(i));
            int base = 1 + i * AGGREGATES_PER_COLUMN;
            long sampledDataSize = data.get(base).getAsLong();
            String hllHex = data.get(base + 1).getAsString();
            long sampledNullCount = data.get(base + 2).getAsLong();
            String max = data.get(base + 3).getAsString();
            String min = data.get(base + 4).getAsString();
            long cardinality = data.get(base + 5).getAsLong();
            cardinalityByColumn.put(columnName, cardinality);

            // data_size and null_count are sample-local totals just like row_count was before
            // scaling -- readers compute averageRowSize = data_size / row_count and nullsFraction =
            // null_count / row_count, so leaving these two unscaled while row_count is a full-table
            // estimate would make both figures come out ~ratio times too small.
            long scaledDataSize = (ratio > 0 && ratio < 1.0) ? (long) Math.ceil(sampledDataSize / ratio) : sampledDataSize;
            long scaledNullCount = (ratio > 0 && ratio < 1.0) ? (long) Math.ceil(sampledNullCount / ratio) : sampledNullCount;

            List<String> params = Lists.newArrayList();
            List<Expr> row = Lists.newArrayList();

            params.add("'" + hashedTableUuid + "'");
            params.add("'" + StatsConstants.EXTERNAL_SAMPLE_PARTITION_NAME_SENTINEL + "'");
            params.add("'" + StringEscapeUtils.escapeSql(columnName) + "'");
            params.add("'" + getCatalogName() + "'");
            params.add("'" + db.getOriginName() + "'");
            params.add("'" + table.getName() + "'");
            params.add(String.valueOf(scaledRowCount));
            params.add(String.valueOf(scaledDataSize));
            params.add("hll_deserialize(unhex('mockData'))");
            params.add(String.valueOf(scaledNullCount));
            params.add("'" + max + "'");
            params.add("'" + min + "'");
            params.add("now()");

            row.add(new StringLiteral(hashedTableUuid));
            row.add(new StringLiteral(StatsConstants.EXTERNAL_SAMPLE_PARTITION_NAME_SENTINEL));
            row.add(new StringLiteral(columnName));
            row.add(new StringLiteral(getCatalogName()));
            row.add(new StringLiteral(db.getOriginName()));
            row.add(new StringLiteral(table.getName()));
            row.add(new IntLiteral(scaledRowCount, IntegerType.BIGINT));
            row.add(new IntLiteral(scaledDataSize, IntegerType.BIGINT));
            row.add(hllDeserialize(hllHex.getBytes(StandardCharsets.UTF_8)));
            row.add(new IntLiteral(scaledNullCount, IntegerType.BIGINT));
            row.add(new StringLiteral(max));
            row.add(new StringLiteral(min));
            row.add(nowFn());

            rowsBuffer.add(row);
            sqlBuffer.add("(" + String.join(", ", params) + ")");
        }
        flushInsertStatisticsData(context, false);
        return cardinalityByColumn;
    }
}
