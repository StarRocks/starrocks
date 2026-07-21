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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.iceberg.IcebergApiConverter;
import com.starrocks.connector.iceberg.IcebergPartitionTransform;
import com.starrocks.connector.iceberg.IcebergPartitionUtils;
import com.starrocks.connector.iceberg.Partition;
import com.starrocks.connector.paimon.PaimonMetadata;
import com.starrocks.connector.partitiontraits.IcebergPartitionTraits;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.connector.PartitionUtil.ICEBERG_DEFAULT_PARTITION;
import static com.starrocks.statistic.StatsConstants.EXTERNAL_FULL_STATISTICS_TABLE_NAME;

public class ExternalFullStatisticsCollectJob extends StatisticsCollectJob {
    private static final Logger LOG = LogManager.getLogger(ExternalFullStatisticsCollectJob.class);

    // Per-partition CTE wrapper: the partition is read exactly once into base_cte_table and every per-column
    // aggregate branch reads from it, so a partition is scanned once instead of once per column. Requires CTE
    // reuse to be forced on in the collection session (see collect()); otherwise the optimizer may inline the
    // CTE and fall back to one scan per column.
    private static final String BATCH_FULL_STATISTIC_CTE_TEMPLATE =
            "WITH base_cte_table AS (SELECT $projection " +
            "FROM `$catalogName`.`$dbName`.`$tableName` WHERE $partitionPredicate) $unionSelects";

    // One statistics row per column, reading from the shared CTE instead of re-scanning the physical table.
    // The output schema is identical to the previous per-(partition, column) query, so result parsing is
    // unchanged.
    private static final String BATCH_FULL_STATISTIC_SELECT_TEMPLATE = "SELECT cast($version as INT)" +
            ", '$partitionNameStr'" + // VARCHAR
            ", '$columnNameStr'" + // VARCHAR
            ", cast(COUNT(1) as BIGINT)" + // BIGINT
            ", cast($dataSize as BIGINT)" + // BIGINT
            ", $hllFunction" + // VARBINARY
            ", cast($countNullFunction as BIGINT)" + // BIGINT
            ", $maxFunction" + // VARCHAR
            ", $minFunction " + // VARCHAR
            " FROM base_cte_table";

    private final String catalogName;
    protected List<String> partitionNames;
    private final List<String> sqlBuffer = Lists.newArrayList();
    private final List<List<Expr>> rowsBuffer = Lists.newArrayList();
    // Per-partition total row count from connector metadata (Iceberg PARTITIONS table), used to extrapolate a
    // bounded-cost truncated sample back to the full partition. Empty when no scan budget is active or the
    // connector cannot supply per-partition row counts - then statistics are stored as raw sample values.
    private Map<String, Long> partitionTotalRowCounts = Collections.emptyMap();
    // Skip collecting statistics for default partition in iceberg table,
    // because we can not generate partition predicates for it.
    private static final Set<String> DO_NOT_COLLECT_PARTITIONS = Set.of(ICEBERG_DEFAULT_PARTITION);

    // Conservative mirror of BE's primary_key_limit_size default (be/src/common/config.h) and its
    // per-field VARCHAR encoding overhead, used only to decide whether to log a warning below -
    // table_uuid is always hashed (StatisticUtils.hashTableUuidForPkStorage) regardless of this
    // estimate, so an inaccurate guess here never causes a correctness problem.
    private static final int EXTERNAL_STATS_PK_LIMIT_ESTIMATE = 128;
    private static final int PK_FIELD_OVERHEAD_ESTIMATE = 12;

    public ExternalFullStatisticsCollectJob(String catalogName, Database db, Table table, List<String> partitionNames,
                                            List<String> columnNames, List<Type> columnTypes,
                                            StatsConstants.AnalyzeType type, StatsConstants.ScheduleType scheduleType,
                                            Map<String, String> properties) {
        super(db, table, columnNames, columnTypes, type, scheduleType, properties);
        this.catalogName = catalogName;
        this.partitionNames = partitionNames;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    @Override
    public String getName() {
        return "ExternalFull";
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        long startMs = System.currentTimeMillis();
        long jobId = analyzeStatus.getId();
        LOG.info("[ExternalStats] collect start | jobId={} catalog={} db={} table={} partitions={} columns={}",
                jobId, catalogName, db.getOriginName(), table.getName(),
                partitionNames.size(), columnNames.size());
        logIfRawKeyWouldExceedPkLimit(jobId);

        // Bounded-cost scan budgets (design 2.4): resolve per-statement PROPERTIES over global Config, then
        // stash on the session variable so every statistics scan this job triggers inherits them via the
        // normal query path (read back in IcebergScanNode). Restored in the finally block. All three <= 0
        // leaves early stop disabled, i.e. the pre-existing full-scan behavior.
        SessionVariable sessionVariable = context.getSessionVariable();
        long savedScanBytesCap = sessionVariable.getExternalStatsScanBytesCap();
        long savedScanFilesCap = sessionVariable.getExternalStatsScanFilesCap();
        long savedScanRowsCap = sessionVariable.getExternalStatsScanRowsCap();
        // Saved so the forced CTE-reuse settings (set below, restored in finally) do not leak to a reused context.
        boolean savedCboCteReuse = sessionVariable.isCboCteReuse();
        double savedCboCteReuseRatio = sessionVariable.getCboCTERuseRatio();
        long scanBytesCap = resolveScanCap(StatsConstants.EXTERNAL_ANALYZE_SCAN_BYTES_CAP,
                Config.connector_table_analyze_scan_bytes_cap, jobId);
        long scanFilesCap = resolveScanCap(StatsConstants.EXTERNAL_ANALYZE_SCAN_FILES_CAP,
                Config.connector_table_analyze_scan_files_cap, jobId);
        long scanRowsCap = resolveScanCap(StatsConstants.EXTERNAL_ANALYZE_SCAN_ROWS_CAP,
                Config.connector_table_analyze_scan_rows_cap, jobId);
        // Fetch per-partition totals up front so a budget-truncated sample can be extrapolated back to the
        // full partition (see scaleStatisticData). Cheap: one PARTITIONS metadata scan, and only when a
        // budget is actually active.
        partitionTotalRowCounts = buildPartitionTotalRowCounts(scanBytesCap, scanFilesCap, scanRowsCap, jobId);

        Map<String, String> extendedInfo = new LinkedHashMap<>();
        extendedInfo.put("table_format", table.getType().name().toLowerCase(Locale.ROOT));
        extendedInfo.put("partition_count", String.valueOf(partitionNames.size()));
        extendedInfo.put("column_count", String.valueOf(columnNames.size()));
        // Surface the resolved scan budgets so operators can see, in SHOW ANALYZE STATUS, whether a run was
        // bounded-cost and with what caps (the lightweight observability of design 2.7).
        extendedInfo.put(StatsConstants.EXTERNAL_ANALYZE_SCAN_BYTES_CAP, String.valueOf(scanBytesCap));
        extendedInfo.put(StatsConstants.EXTERNAL_ANALYZE_SCAN_FILES_CAP, String.valueOf(scanFilesCap));
        extendedInfo.put(StatsConstants.EXTERNAL_ANALYZE_SCAN_ROWS_CAP, String.valueOf(scanRowsCap));
        extendedInfo.putAll(table.getStatsCollectMetadata());

        // Merge into the existing properties map so it surfaces via the Properties column of
        // SHOW ANALYZE STATUS without introducing new columns.
        Map<String, String> mergedProperties = new LinkedHashMap<>();
        if (analyzeStatus.getProperties() != null) {
            mergedProperties.putAll(analyzeStatus.getProperties());
        }
        mergedProperties.putAll(extendedInfo);
        analyzeStatus.setProperties(mergedProperties);

        LOG.info("[ExternalStats] table info | jobId={} catalog={} db={} table={} {}",
                jobId, catalogName, db.getOriginName(), table.getName(), extendedInfo);

        sessionVariable.setExternalStatsScanBytesCap(scanBytesCap);
        sessionVariable.setExternalStatsScanFilesCap(scanFilesCap);
        sessionVariable.setExternalStatsScanRowsCap(scanRowsCap);
        LOG.info("[ExternalStats] scan budget | jobId={} catalog={} db={} table={} bytesCap={} filesCap={} rowsCap={}",
                jobId, catalogName, db.getOriginName(), table.getName(), scanBytesCap, scanFilesCap, scanRowsCap);

        // Each per-partition query wraps the read in a CTE (base_cte_table) shared by every column's aggregate
        // branch. Force CTE reuse (ratio 0 = reuse regardless of estimated cost) so the optimizer materializes
        // a single scan per partition instead of inlining the CTE back into one scan per column.
        sessionVariable.setCboCteReuse(true);
        sessionVariable.setCboCTERuseRatio(0);

        String status = "SUCCESS";
        String failureReason = "";
        try {
            long finishedSQLNum = 0;
            int parallelism = Math.max(1, context.getSessionVariable().getStatisticCollectParallelism());
            List<List<String>> collectSQLList = buildCollectSQLList(parallelism);
            long totalCollectSQL = collectSQLList.size();

            // Each element is one self-contained CTE query for a (partition, column-group): all columns in the
            // group share a single partition scan. They cannot be merged (only one WITH per statement), so
            // each runs on its own. The collect parallelism now drives per-query pipeline dop: a single-element
            // group is given dop = parallelism to use enough cpu cores.
            for (List<String> sqlUnion : collectSQLList) {
                if (sqlUnion.size() < parallelism) {
                    context.getSessionVariable().setPipelineDop(parallelism / sqlUnion.size());
                } else {
                    context.getSessionVariable().setPipelineDop(1);
                }

                String sql = Joiner.on(" UNION ALL ").join(sqlUnion);

                collectStatisticSync(sql, context, analyzeStatus);
                finishedSQLNum++;
                analyzeStatus.setProgress(finishedSQLNum * 100 / totalCollectSQL);
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddAnalyzeStatus(analyzeStatus);
            }

            flushInsertStatisticsData(context, true);
            cleanupStaleRawKeyedRows(context, jobId);
        } catch (Exception e) {
            status = "FAILED";
            failureReason = e.getMessage();
            throw e;
        } finally {
            sessionVariable.setExternalStatsScanBytesCap(savedScanBytesCap);
            sessionVariable.setExternalStatsScanFilesCap(savedScanFilesCap);
            sessionVariable.setExternalStatsScanRowsCap(savedScanRowsCap);
            sessionVariable.setCboCteReuse(savedCboCteReuse);
            sessionVariable.setCboCTERuseRatio(savedCboCteReuseRatio);
            LOG.info("[ExternalStats] collect end | jobId={} catalog={} db={} table={} status={} " +
                            "durationMs={} partitions={} columns={} reason={}",
                    jobId, catalogName, db.getOriginName(), table.getName(),
                    status, System.currentTimeMillis() - startMs,
                    partitionNames.size(), columnNames.size(), failureReason);
        }
    }

    // Resolves a scan cap: an explicit ANALYZE ... PROPERTIES value wins over the global Config default;
    // an absent or unparseable property falls back to the Config value. <= 0 means the dimension is unlimited.
    private long resolveScanCap(String propertyKey, long configDefault, long jobId) {
        if (properties != null && properties.containsKey(propertyKey)) {
            String raw = properties.get(propertyKey);
            try {
                return Long.parseLong(raw.trim());
            } catch (NumberFormatException e) {
                LOG.warn("[ExternalStats] invalid scan cap property | jobId={} key={} value={} fallbackConfig={}",
                        jobId, propertyKey, raw, configDefault);
            }
        }
        return configDefault;
    }

    // Fetches the total row count of each collected partition from connector metadata, so a budget-truncated
    // sample can be extrapolated back to the full partition. Only meaningful when a budget is active and the
    // connector exposes exact per-partition row counts cheaply - currently Iceberg (PARTITIONS metadata table).
    // Other connectors / no-budget runs return empty, and statistics are then stored as raw sample values.
    private Map<String, Long> buildPartitionTotalRowCounts(long bytesCap, long filesCap, long rowsCap, long jobId) {
        boolean budgetActive = bytesCap > 0 || filesCap > 0 || rowsCap > 0;
        if (!budgetActive) {
            return Collections.emptyMap();
        }
        try {
            // Connector-agnostic: each PartitionTraits decides whether it can supply per-partition totals
            // (Iceberg does via the PARTITIONS metadata table; others return empty -> no extrapolation).
            return ConnectorPartitionTraits.build(table).getPartitionRowCounts(partitionNames);
        } catch (Exception e) {
            // Never fail collection over missing extrapolation input: fall back to raw sample values.
            LOG.warn("[ExternalStats] failed to fetch partition row counts for extrapolation | jobId={} " +
                    "catalog={} db={} table={}", jobId, catalogName, db.getOriginName(), table.getName(), e);
            return Collections.emptyMap();
        }
    }

    // Extrapolates one (partition, column) statistics row from the scanned sample to the full partition when
    // the bounded-cost scan truncated it. row_count is set to the exact metadata total; null_count and
    // data_size scale by (total / sample) so the derived null-fraction and average-row-size (consumed as
    // ratios against row_count) stay correct. NDV (the HLL sketch) is NOT scaled - it cannot be cheaply
    // extrapolated, so it remains a sample undercount (documented degradation). No-op when the partition was
    // not truncated (sample >= total) or its total is unknown.
    private ScaledStats scaleStatisticData(TStatisticData data) {
        return extrapolate(data.getRowCount(), data.getNullCount(), (long) data.getDataSize(),
                partitionTotalRowCounts.get(data.getPartitionName()));
    }

    // Package-private for unit testing. Extrapolates one row's (rowCount, nullCount, dataSize) to the full
    // partition when the sample was truncated (totalRows > sample rowCount); otherwise returns values as-is.
    static ScaledStats extrapolate(long rowCount, long nullCount, long dataSize, Long totalRows) {
        if (totalRows != null && rowCount > 0 && totalRows > rowCount) {
            double scale = (double) totalRows / rowCount;
            nullCount = Math.min(totalRows, Math.round(nullCount * scale));
            dataSize = Math.round(dataSize * scale);
            rowCount = totalRows;
        }
        return new ScaledStats(rowCount, nullCount, dataSize);
    }

    // Holder for the extrapolated (or pass-through) statistics values of one collected row.
    static final class ScaledStats {
        final long rowCount;
        final long nullCount;
        final long dataSize;

        ScaledStats(long rowCount, long nullCount, long dataSize) {
            this.rowCount = rowCount;
            this.nullCount = nullCount;
            this.dataSize = dataSize;
        }
    }

    // table_uuid is always hashed for storage (StatisticUtils.hashTableUuidForPkStorage), so this
    // never affects correctness. It's a diagnostic-only warning to confirm which tables actually
    // would have hit "primary key size exceed the limit" pre-hashing (e.g. long Iceberg
    // catalog/db/table names combined with long partition_name values, see the Demandbase case).
    private void logIfRawKeyWouldExceedPkLimit(long jobId) {
        String rawTableUuid = table.getUUID();
        int maxPartitionNameLen = partitionNames.stream().mapToInt(String::length).max().orElse(0);
        int maxColumnNameLen = columnNames.stream().mapToInt(String::length).max().orElse(0);
        int estimatedRawPkLen = rawTableUuid.length() + maxPartitionNameLen + maxColumnNameLen + PK_FIELD_OVERHEAD_ESTIMATE;
        if (estimatedRawPkLen > EXTERNAL_STATS_PK_LIMIT_ESTIMATE) {
            LOG.warn("[ExternalStats] table_uuid hashed | jobId={} catalog={} db={} table={} rawTableUuidLen={} " +
                            "maxPartitionNameLen={} maxColumnNameLen={} estimatedRawPkLen={} limitEstimate={}",
                    jobId, catalogName, db.getOriginName(), table.getName(), rawTableUuid.length(),
                    maxPartitionNameLen, maxColumnNameLen, estimatedRawPkLen, EXTERNAL_STATS_PK_LIMIT_ESTIMATE);
        }
    }

    // Deletes the stale raw-keyed rows for exactly the (partition, column) pairs this job just
    // (re-)wrote under the hashed table_uuid. Purely storage hygiene: buildQueryExternalFullStatisticsSQL
    // dedups by (partition_name, column_name, latest update_time) internally, so a lingering stale
    // row is never double-counted even if this cleanup fails - it just wastes a bit of space until
    // the next collection retries it. Best-effort: must never fail a job whose actual stats write
    // already succeeded.
    private void cleanupStaleRawKeyedRows(ConnectContext context, long jobId) {
        String rawTableUuid = table.getUUID();
        boolean ok = new StatisticExecutor().dropExternalStatRawPartitions(context, rawTableUuid, partitionNames, columnNames);
        if (!ok) {
            LOG.warn("[ExternalStats] failed to clean up stale raw-keyed rows | jobId={} catalog={} db={} table={}",
                    jobId, catalogName, db.getOriginName(), table.getName());
        }
    }

    protected List<List<String>> buildCollectSQLList(int parallelism) {
        // Collect a partition in a single scan by wrapping the read in a CTE (base_cte_table) shared by every
        // column's aggregate branch. Columns are split into groups so a wide table does not build one CTE
        // multicast to hundreds of consumers (inflating query/plan size and the memory held for the
        // materialized partition); each group scans the partition once, so total scans are
        // partitions x ceil(columns / columnsPerScan). The group size mirrors the internal sample path
        // (ColumnSampleManager.splitPrimitiveTypeStats): max(2, statistic_collect_parallelism), i.e. at least
        // two columns share a scan. Each group is a self-contained CTE query and two CTE queries cannot be
        // UNION ALL'd (one WITH per statement), so the outer group size is fixed to 1; parallelism only sets
        // per-query pipeline dop in the execute loop.
        int columnsPerScan = Math.max(2, parallelism);
        List<String> totalQuerySQL = new ArrayList<>();
        for (String partitionName : partitionNames) {
            if (DO_NOT_COLLECT_PARTITIONS.contains(partitionName)) {
                LOG.info("Skip collect full statistics for partition: {} in table: {}",
                        partitionName, table.getName());
                continue;
            }
            for (int start = 0; start < columnNames.size(); start += columnsPerScan) {
                int end = Math.min(columnNames.size(), start + columnsPerScan);
                totalQuerySQL.add(buildPartitionCTESQL(table, partitionName, start, end));
            }
        }

        return Lists.partition(totalQuerySQL, 1);
    }

    // Builds one CTE query that collects columns [startCol, endCol) of a single partition in a shared scan.
    private String buildPartitionCTESQL(Table table, String partitionName, int startCol, int endCol) {
        Collection<String> nullValues = resolveNullValues(table);
        String partitionPredicate = table.isUnPartitioned() ? "1=1" :
                Joiner.on(" AND ").join(generatePartitionPredicates(table, partitionName, nullValues));

        // CTE projection: only the columns in this group actually read by an aggregate (statable,
        // non-collection). Non-statable columns emit constant sketches and need nothing beyond COUNT(1).
        List<String> projection = new ArrayList<>();
        List<String> unionSelects = new ArrayList<>();
        for (int i = startCol; i < endCol; i++) {
            Type columnType = columnTypes.get(i);
            if (columnType.canStatistic() && !columnType.isCollectionType()) {
                projection.add(StatisticUtils.quoting(table, columnNames.get(i)));
            }
            unionSelects.add(buildColumnSelectFromCTE(table, partitionName, columnNames.get(i), columnType));
        }

        VelocityContext context = new VelocityContext();
        context.put("catalogName", this.catalogName);
        context.put("dbName", db.getOriginName());
        context.put("tableName", table.getName());
        context.put("projection", projection.isEmpty() ? "1" : Joiner.on(", ").join(projection));
        context.put("partitionPredicate", partitionPredicate);
        context.put("unionSelects", Joiner.on(" UNION ALL ").join(unionSelects));
        return build(context, BATCH_FULL_STATISTIC_CTE_TEMPLATE);
    }

    // Builds one statistics row (COUNT/dataSize/HLL/null-count/max/min) for a single column, reading from the
    // shared base_cte_table. The output columns match the legacy per-(partition, column) query verbatim.
    private String buildColumnSelectFromCTE(Table table, String partitionName, String columnName, Type columnType) {
        VelocityContext context = new VelocityContext();

        String columnNameStr = StringEscapeUtils.escapeSql(columnName);
        String quoteColumnName = StatisticUtils.quoting(table, columnName);
        Collection<String> nullValues = resolveNullValues(table);

        context.put("version", StatsConstants.STATISTIC_EXTERNAL_VERSION);
        context.put("partitionNameStr", table.isIcebergTable() ? partitionName :
                PartitionUtil.normalizePartitionName(partitionName, table.getPartitionColumnNames(), nullValues));
        context.put("columnNameStr", columnNameStr);
        context.put("dataSize", fullAnalyzeGetDataSize(quoteColumnName, columnType));

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

        return build(context, BATCH_FULL_STATISTIC_SELECT_TEMPLATE);
    }

    private Collection<String> resolveNullValues(Table table) {
        if (table.isIcebergTable()) {
            return Collections.singleton(IcebergApiConverter.PARTITION_NULL_VALUE);
        } else if (table.isPaimonTable()) {
            return PaimonMetadata.PARTITION_NULL_VALUES;
        } else {
            return Collections.singleton(HiveMetaClient.PARTITION_NULL_VALUE);
        }
    }

    private List<String> generatePartitionPredicates(Table table, String partitionName, Collection<String> nullValues) {
        if (table.isIcebergTable()) {
            return generatePartitionPredicatesForIcebergTable((IcebergTable) table, partitionName);
        }
        List<String> partitionColumnNames = table.getPartitionColumnNames();
        List<String> partitionValues = PartitionUtil.toPartitionValues(partitionName);
        List<String> partitionPredicate = Lists.newArrayList();
        for (int i = 0; i < partitionColumnNames.size(); i++) {
            String partitionColumnName = partitionColumnNames.get(i);
            String partitionValue = partitionValues.get(i);
            if (nullValues.contains(partitionValue)) {
                partitionPredicate.add(StatisticUtils.quoting(partitionColumnName) + " IS NULL");
            } else {
                partitionPredicate.add(StatisticUtils.quoting(partitionColumnName) + " = '" + partitionValue + "'");
            }
        }
        return partitionPredicate;
    }

    private List<String> generatePartitionPredicatesForIcebergTable(IcebergTable table, String partitionName) {
        List<PartitionInfo> partitionInfos = IcebergPartitionTraits.build(table).
                getPartitions(Lists.newArrayList(partitionName));
        Preconditions.checkArgument(partitionInfos.size() == 1,
                "Partition %s not found in table %s", partitionName, table.getName());
        Partition partition = (Partition) partitionInfos.get(0);
        PartitionSpec spec = table.getNativeTable().specs().get(partition.getSpecId());
        if (spec == null) {
            // Fallback to current spec when the provided specId is not found
            spec = table.getNativeTable().spec();
        }

        Schema schema = spec.schema();
        List<PartitionField> partitionFields = spec.fields();
        List<String> partitionValues = PartitionUtil.toPartitionValues(partitionName);
        if (partitionValues.size() != partitionFields.size()) {
            throw new StarRocksConnectorException("Partition values size %s not match spec fields size %s in %s",
                    partitionValues.size(), partitionFields.size(), partitionName);
        }

        List<String> partitionPredicate = Lists.newArrayList();
        for (int i = 0; i < partitionFields.size(); i++) {
            PartitionField field = partitionFields.get(i);
            String partitionColumnName = schema.findColumnName(field.sourceId());
            String partitionValue = partitionValues.get(i);
            if (partitionValue.equals(IcebergApiConverter.PARTITION_NULL_VALUE)) {
                partitionPredicate.add(StatisticUtils.quoting(partitionColumnName) + " IS NULL");
            } else if (isSupportedPartitionTransform(field)) {
                partitionPredicate.add(IcebergPartitionUtils.convertPartitionTransformToPredicate(table, field,
                        partitionColumnName, partitionValue));
            } else {
                partitionPredicate.add(StatisticUtils.quoting(partitionColumnName) + " = '" + partitionValue + "'");
            }
        }
        return partitionPredicate;
    }

    // only iceberg table support partition transform
    // now only support identity/year/month/day/hour transform
    boolean isSupportedPartitionTransform(PartitionField partitionField) {
        // only iceberg table support partition transform
        if (!table.isIcebergTable()) {
            return false;
        }

        IcebergPartitionTransform transform = IcebergPartitionTransform.fromString(partitionField.transform().toString());
        if (!IcebergPartitionUtils.isSupportedConvertPartitionTransform(transform)) {
            LOG.warn("Partition transform {} not supported to analyze, table: {}", transform, table.getName());
            throw new StarRocksConnectorException("Partition transform " + transform + " not supported to analyze, " +
                    "table: " + table.getName());
        }

        return true;
    }

    @Override
    public void collectStatisticSync(String sql, ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        // Calculate and set remaining timeout for this SQL task
        calculateAndSetRemainingTimeout(context, analyzeStatus);

        LOG.debug("statistics collect sql : " + sql);
        StatisticExecutor executor = new StatisticExecutor();

        // set default session variables for stats context
        setDefaultSessionVariable(context);

        List<TStatisticData> dataList = executor.executeStatisticDQL(context, sql);

        String hashedTableUuid = StatisticUtils.hashTableUuidForPkStorage(table.getUUID());
        for (TStatisticData data : dataList) {
            // Extrapolate a bounded-cost truncated sample back to the full partition. No-op (scale=1) when the
            // partition was not truncated or its total is unknown.
            ScaledStats scaled = scaleStatisticData(data);

            List<String> params = Lists.newArrayList();
            List<Expr> row = Lists.newArrayList();

            params.add("'" + hashedTableUuid + "'");
            params.add("'" + StringEscapeUtils.escapeSql(data.getPartitionName()) + "'");
            params.add("'" + StringEscapeUtils.escapeSql(data.getColumnName()) + "'");
            params.add("'" + catalogName + "'");
            params.add("'" + db.getOriginName() + "'");
            params.add("'" + table.getName() + "'");
            params.add(String.valueOf(scaled.rowCount));
            params.add(String.valueOf(scaled.dataSize));
            params.add("hll_deserialize(unhex('mockData'))");
            params.add(String.valueOf(scaled.nullCount));
            params.add("'" + data.getMax() + "'");
            params.add("'" + data.getMin() + "'");
            params.add("now()");
            // int
            row.add(new StringLiteral(hashedTableUuid)); // table id, wait to byte
            row.add(new StringLiteral(data.getPartitionName()));
            row.add(new StringLiteral(data.getColumnName())); // column name, 20 byte
            row.add(new StringLiteral(catalogName));
            row.add(new StringLiteral(db.getOriginName()));
            row.add(new StringLiteral(table.getName()));
            row.add(new IntLiteral(scaled.rowCount, IntegerType.BIGINT)); // row count, 8 byte
            row.add(new IntLiteral(scaled.dataSize, IntegerType.BIGINT)); // data size, 8 byte
            row.add(hllDeserialize(data.getHll())); // hll, 32 kB
            row.add(new IntLiteral(scaled.nullCount, IntegerType.BIGINT)); // null count, 8 byte
            row.add(new StringLiteral(data.getMax())); // max, 200 byte
            row.add(new StringLiteral(data.getMin())); // min, 200 byte
            row.add(nowFn()); // update time, 8 byte

            rowsBuffer.add(row);
            sqlBuffer.add("(" + String.join(", ", params) + ")");
        }
        flushInsertStatisticsData(context, false);
    }

    private void flushInsertStatisticsData(ConnectContext context, boolean force) throws Exception {
        // hll serialize to hex, about 32kb
        long bufferSize = 33L * 1024 * rowsBuffer.size();
        if (bufferSize < Config.statistic_full_collect_buffer && !force) {
            return;
        }
        if (rowsBuffer.isEmpty()) {
            return;
        }

        int count = 0;
        int maxRetryTimes = 5;
        StatementBase insertStmt = createInsertStmt();
        do {
            LOG.debug("statistics insert sql size:" + rowsBuffer.size());
            StmtExecutor executor = StmtExecutor.newInternalExecutor(context, insertStmt);
            context.setExecutor(executor);
            context.setQueryId(UUIDUtil.genUUID());
            context.setStartTime();
            executor.execute();

            if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                LOG.warn("Statistics collect fail | {} | Error Message [{}]", DebugUtil.printId(context.getQueryId()),
                        context.getState().getErrorMessage());
                if (StringUtils.contains(context.getState().getErrorMessage(), "Too many versions")) {
                    Thread.sleep(Config.statistic_collect_too_many_version_sleep);
                    count++;
                } else {
                    throw new DdlException(context.getState().getErrorMessage());
                }
            } else {
                sqlBuffer.clear();
                rowsBuffer.clear();
                return;
            }
        } while (count < maxRetryTimes);

        throw new DdlException(context.getState().getErrorMessage());
    }

    private StatementBase createInsertStmt() {
        List<String> targetColumnNames = StatisticUtils.buildStatsColumnDef(EXTERNAL_FULL_STATISTICS_TABLE_NAME).stream()
                .map(ColumnDef::getName)
                .collect(Collectors.toList());

        String sql = "INSERT INTO external_column_statistics(" + String.join(", ", targetColumnNames) +
                ") values " + String.join(", ", sqlBuffer) + ";";
        QueryStatement qs = new QueryStatement(new ValuesRelation(rowsBuffer, targetColumnNames));
        TableRef tableRef = new TableRef(QualifiedName.of(Lists.newArrayList("_statistics_", "external_column_statistics")),
                null, NodePosition.ZERO);
        InsertStmt insert = new InsertStmt(tableRef, qs);
        insert.setTargetColumnNames(targetColumnNames);
        insert.setOrigStmt(new OriginStatement(sql, 0));
        return insert;
    }
}
