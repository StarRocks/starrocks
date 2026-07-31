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

package com.starrocks.connector.starrocks;

import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.StarRocksExternalTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

public class StarRocksMetadata implements ConnectorMetadata {
    // Internal source-cluster databases that should not be projected through an external
    // catalog. _statistics_ holds the source cluster's stats meta and is meaningless on
    // the local side; sys and information_schema expose source-private/system metadata.
    private static final Set<String> INTERNAL_DBS = ImmutableSet.of("_statistics_", "sys", "information_schema");

    // Past this many selected partitions, per-partition refinement adds little
    // over table-level stats (mirrors the native partition-stats guard).
    private static final int PARTITION_REFINE_LIMIT = 128;

    private final String catalogName;
    private final StarRocksFeClient feClient;
    // null when starrocks.cache.enable=false: every metadata / statistics
    // access then goes straight to the remote FE through feClient.
    private final StarRocksMetadataCache metadataCache;

    public StarRocksMetadata(String catalogName, StarRocksFeClient feClient) {
        this(catalogName, feClient, new StarRocksMetadataCache(feClient));
    }

    StarRocksMetadata(String catalogName, StarRocksFeClient feClient, StarRocksMetadataCache metadataCache) {
        this.catalogName = catalogName;
        this.feClient = feClient;
        this.metadataCache = metadataCache;
    }

    private boolean cacheEnabled() {
        return metadataCache != null;
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.STARROCKS;
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        List<String> dbNames = cacheEnabled() ? metadataCache.getDbNames() : feClient.listDbNames();
        return dbNames.stream()
                .filter(name -> !INTERNAL_DBS.contains(name))
                .collect(Collectors.toList());
    }

    @Override
    public boolean hasSelfInfoSchema() {
        // Do not let CatalogConnectorMetadata synthesize a local information_schema
        // database for StarRocks catalog. The source cluster's information_schema is
        // filtered above, and the external catalog should expose only user databases.
        return true;
    }

    @Override
    public Database getDb(ConnectContext context, String dbName) {
        if (!listDbNames(context).contains(dbName)) {
            return null;
        }
        Database db = new Database(CONNECTOR_ID_GENERATOR.getNextId().asLong(), dbName);
        db.setCatalogName(catalogName);
        return db;
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        return cacheEnabled() ? metadataCache.getTableNames(dbName) : feClient.listTableNames(dbName);
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        StarRocksRemoteScanWire.Table tableInfo =
                cacheEnabled() ? metadataCache.getTable(dbName, tblName) : feClient.getTable(dbName, tblName);
        if (tableInfo == null) {
            return null;
        }
        // The memoizing supplier pins one snapshot generation on the table
        // object, which lives for the resolving query: partition pruning and
        // statistics derivation see the same snapshot, and background cache
        // refreshes only affect later queries. With the cache disabled the
        // supplier fetches the snapshot straight from the remote FE (no cached
        // epochs, so the remote ships all fields) — still once per query, the
        // memoization is what pins it.
        return new StarRocksExternalTable(CONNECTOR_ID_GENERATOR.getNextId().asLong(), catalogName, dbName, tblName,
                StarRocksFeClient.toColumns(tableInfo), tableInfo.schemaVersion, tableInfo.partitionColumns,
                tableInfo.rowCount,
                cacheEnabled() ? () -> metadataCache.getTableStatsSnapshot(dbName, tblName)
                        : () -> feClient.fetchTableStatsSnapshot(dbName, tblName, null));
    }

    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
        if (table == null || !cacheEnabled()) {
            // Nothing is cached without the cache; every access is already fresh.
            return;
        }
        String dbName = table.getCatalogDBName();
        String tableName = table.getCatalogTableName();
        metadataCache.refreshTable(dbName == null ? srDbName : dbName, tableName == null ? table.getName() : tableName);
    }

    /**
     * Base statistics for the (possibly pruned) partition set, following the
     * Hive contract: the predicate is NOT applied here — the optimizer's
     * visitOperator applies selectivity afterwards. Everything is served from
     * the snapshot pinned on the table object plus the partition-level cache;
     * the only remote calls are cache loaders.
     */
    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate,
                                         long limit,
                                         TvrVersionRange tableVersionRange) {
        if (!(table instanceof StarRocksExternalTable)) {
            return ConnectorMetadata.super.getTableStatistics(
                    session, table, columns, partitionKeys, predicate, limit, tableVersionRange);
        }
        StarRocksExternalTable srTable = (StarRocksExternalTable) table;
        StarRocksRemoteTableStats.Snapshot snapshot = srTable.getStatsSnapshot();
        Statistics.Builder builder = Statistics.builder();
        if (snapshot == null) {
            // Degradation: physical whole-table row count from getTable metadata.
            long rowCount = srTable.getTableRowCount();
            builder.setOutputRowCount(Math.max(rowCount, 1));
            columns.keySet().forEach(columnRef -> builder.addColumnStatistic(columnRef, ColumnStatistic.unknown()));
            return builder.build();
        }

        // ---- 1) Partition selection: map the locally pruned PartitionKeys back to the
        // snapshot's partition metas (selectedIds == null means "not pruned").
        List<StarRocksRemoteTableStats.PartitionMeta> partitions =
                snapshot.partitions == null ? List.of() : snapshot.partitions;
        Set<Long> selectedIds = resolveSelectedPartitionIds(snapshot, srTable, partitionKeys);
        boolean pruned = selectedIds != null;
        List<StarRocksRemoteTableStats.PartitionMeta> selectedPartitions = pruned
                ? partitions.stream().filter(p -> selectedIds.contains(p.id)).collect(Collectors.toList())
                : partitions;

        // ---- 2) Table-level collected stats (ANALYZE), indexed by column.
        Map<String, StarRocksRemoteTableStats.ColumnStats> tableLevelStats = new HashMap<>();
        long collectedRowCount = 0;
        if (snapshot.columnStats != null) {
            for (StarRocksRemoteTableStats.ColumnStats stats : snapshot.columnStats) {
                if (stats == null || stats.column == null) {
                    continue;
                }
                tableLevelStats.put(stats.column.toLowerCase(Locale.ROOT), stats);
                collectedRowCount = Math.max(collectedRowCount, stats.rowCount);
            }
        }

        // ---- 3) Per-partition collected stats: the input of the column refinement in
        // step 5 (refineWithPartitionStats). Only worth the batch call for a small pruned
        // subset with FULL collected stats; cache misses load in one batch. Step 4 also
        // reuses the result opportunistically as a row-count fallback.
        Map<Long, Map<String, StarRocksRemoteTableStats.PartitionColumnStats>> partitionStats = Map.of();
        if (pruned && !selectedPartitions.isEmpty() && selectedPartitions.size() < partitions.size()
                && selectedPartitions.size() <= PARTITION_REFINE_LIMIT
                && "FULL".equals(snapshot.analyzeType) && !tableLevelStats.isEmpty()) {
            List<String> columnNames = columns.values().stream()
                    .map(Column::getName).distinct().collect(Collectors.toList());
            List<Long> sortedIds = selectedIds.stream().sorted().collect(Collectors.toList());
            partitionStats = cacheEnabled()
                    ? metadataCache.getPartitionColumnStats(srTable.getCatalogDBName(),
                            srTable.getCatalogTableName(), sortedIds, columnNames)
                    : fetchPartitionColumnStatsDirect(srTable.getCatalogDBName(),
                            srTable.getCatalogTableName(), sortedIds, columnNames);
        }

        // ---- 4) Row count of the selected partition set (single resolution ladder).
        long rowCount = resolveOutputRowCount(srTable, snapshot, selectedPartitions, pruned,
                collectedRowCount, partitionStats);

        // ---- 5) Column statistics: table-level base stats with the caliber fixed up for
        // the selected subset (narrow partition columns, refine the other columns).
        List<String> partitionColumnNames = snapshot.partitionColumns == null ?
                List.of() : snapshot.partitionColumns;
        for (Map.Entry<ColumnRefOperator, Column> entry : columns.entrySet()) {
            Column column = entry.getValue();
            StarRocksRemoteTableStats.ColumnStats baseStats =
                    tableLevelStats.get(column.getName().toLowerCase(Locale.ROOT));
            if (baseStats == null) {
                builder.addColumnStatistic(entry.getKey(), ColumnStatistic.unknown());
                continue;
            }
            ColumnStatistic statistic = StarRocksStatsUtils.toColumnStatistic(
                    baseStats, srTable.getCatalogDBName(), srTable.getCatalogTableName(), column.getType());
            int partitionColumnIndex = indexOfIgnoreCase(partitionColumnNames, column.getName());
            if (pruned && partitionColumnIndex >= 0 && selectedPartitions.size() < partitions.size()) {
                // Narrow partition-column stats to the selected partitions so the
                // partition predicate re-applied by visitOperator does not
                // double-discount (its selectivity over the narrowed range ≈ 1).
                statistic = narrowPartitionColumnStatistic(statistic, snapshot, selectedIds,
                        partitionColumnIndex, column, rowCount, selectedPartitions.size(), partitions.size());
            } else if (!partitionStats.isEmpty()) {
                statistic = refineWithPartitionStats(statistic, partitionStats, column.getName(), rowCount);
            }
            builder.addColumnStatistic(entry.getKey(), statistic);
        }

        builder.setOutputRowCount(Math.max(rowCount, 1));
        return builder.build();
    }

    /**
     * Resolves the row-count estimate of the selected partition set from its sources in
     * freshness order. PHYSICAL counts (TabletStatMgr-fed, refreshed every reporting
     * cycle) are authoritative; ANALYZE-COLLECTED counts (frozen at the last collection)
     * only backfill the reporting-lag window where physical counts are still 0 — a
     * freshly loaded partition reports 0 ("not reported yet", never "empty") until the
     * next TabletStatMgr sweep, while the load-triggered auto-analyze already knows the
     * rows. Collected counts never OVERRIDE a live physical count: they can be stale in
     * either direction (older loads or deletes since the last ANALYZE).
     *
     * <p>The ladder:
     * <ol>
     * <li>Sum of the selected partitions' physical row counts. Unpruned is additionally
     *     guarded by max(..., snapshot.tableRowCount) — the remote computes both from
     *     the same aggregation today, so the max is pure protection against caliber
     *     drift, and it must NOT apply to pruned subsets (it would erase the pruning).</li>
     * <li>Physical absent, unpruned: the table-level collected row count, then the
     *     getTable metadata row count.</li>
     * <li>Physical absent, pruned: sum of the selected partitions' collected row counts
     *     when the step-3 refinement data is at hand (per partition: max across its
     *     columns — the columns should agree, max guards missing entries).</li>
     * </ol>
     * The caller floors the final estimate to 1: 0 always means "unknown", never "empty".
     */
    private static long resolveOutputRowCount(
            StarRocksExternalTable srTable,
            StarRocksRemoteTableStats.Snapshot snapshot,
            List<StarRocksRemoteTableStats.PartitionMeta> selectedPartitions,
            boolean pruned,
            long collectedRowCount,
            Map<Long, Map<String, StarRocksRemoteTableStats.PartitionColumnStats>> partitionStats) {
        long physicalRowCount = selectedPartitions.stream()
                .mapToLong(partition -> partition.rowCount)
                .sum();
        if (!pruned) {
            physicalRowCount = Math.max(physicalRowCount, snapshot.tableRowCount);
        }
        if (physicalRowCount > 0) {
            return physicalRowCount;
        }
        if (!pruned) {
            return collectedRowCount > 0 ? collectedRowCount : srTable.getTableRowCount();
        }
        return partitionStats.values().stream()
                .mapToLong(stats -> stats.values().stream()
                        .mapToLong(s -> s.rowCount).max().orElse(0))
                .sum();
    }

    /**
     * Cache-disabled counterpart of {@link StarRocksMetadataCache#getPartitionColumnStats}:
     * one batch fetch for the selected partitions, grouped into the same result shape.
     */
    private Map<Long, Map<String, StarRocksRemoteTableStats.PartitionColumnStats>> fetchPartitionColumnStatsDirect(
            String dbName, String tableName, List<Long> partitionIds, List<String> columnNames) {
        StarRocksRemoteTableStats.PartitionStatsResponse response =
                feClient.fetchPartitionColumnStats(dbName, tableName, partitionIds, columnNames);
        if (response == null || response.partitionStats == null) {
            return Map.of();
        }
        return StarRocksMetadataCache.groupPartitionStats(response.partitionStats);
    }

    /**
     * Maps the selected PartitionKeys back to logical partition ids via the
     * same canonical-key construction the pruner used. Returns null when the
     * scan is unpruned (all partitions) or any key cannot be mapped.
     */
    private Set<Long> resolveSelectedPartitionIds(StarRocksRemoteTableStats.Snapshot snapshot,
                                                  StarRocksExternalTable table, List<PartitionKey> partitionKeys) {
        if (partitionKeys == null || partitionKeys.isEmpty() || !StarRocksStatsUtils.isPruneSupported(snapshot)) {
            return null;
        }
        List<Column> partitionColumns = table.getPartitionColumns();
        if (partitionColumns.isEmpty()) {
            return null;
        }
        Map<Long, PartitionKey> canonicalKeys = StarRocksStatsUtils.buildCanonicalKeys(snapshot, partitionColumns);
        Map<PartitionKey, Long> keyToId = new HashMap<>();
        canonicalKeys.forEach((id, key) -> keyToId.put(key, id));
        Set<Long> selected = new HashSet<>();
        for (PartitionKey key : partitionKeys) {
            Long id = keyToId.get(key);
            if (id == null) {
                return null;
            }
            selected.add(id);
        }
        return selected;
    }

    private static int indexOfIgnoreCase(List<String> names, String name) {
        for (int i = 0; i < names.size(); i++) {
            if (names.get(i).equalsIgnoreCase(name)) {
                return i;
            }
        }
        return -1;
    }

    private static ColumnStatistic narrowPartitionColumnStatistic(
            ColumnStatistic base, StarRocksRemoteTableStats.Snapshot snapshot, Set<Long> selectedIds,
            int columnIndex, Column column, long selectedRowCount, int selectedCount, int totalCount) {
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        Set<String> distinctValues = new HashSet<>();
        boolean isRange = StarRocksRemoteTableStats.PARTITION_TYPE_RANGE.equals(snapshot.partitionType);
        for (StarRocksRemoteTableStats.PartitionMeta partition : snapshot.partitions) {
            if (!selectedIds.contains(partition.id)) {
                continue;
            }
            if (isRange) {
                if (partition.rangeLower != null && partition.rangeLower.values != null
                        && columnIndex < partition.rangeLower.values.size()) {
                    OptionalDouble value = StarRocksStatsUtils.statisticDomainValue(
                            partition.rangeLower.values.get(columnIndex), column.getType());
                    if (value.isPresent()) {
                        min = Math.min(min, value.getAsDouble());
                    }
                }
                if (partition.rangeUpper != null && partition.rangeUpper.values != null
                        && columnIndex < partition.rangeUpper.values.size()) {
                    OptionalDouble value = StarRocksStatsUtils.statisticDomainValue(
                            partition.rangeUpper.values.get(columnIndex), column.getType());
                    if (value.isPresent()) {
                        max = Math.max(max, value.getAsDouble());
                    }
                }
            } else if (partition.listValues != null) {
                for (List<String> tuple : partition.listValues) {
                    if (columnIndex >= tuple.size()) {
                        continue;
                    }
                    String value = tuple.get(columnIndex);
                    if (value == null) {
                        continue;
                    }
                    distinctValues.add(value);
                    OptionalDouble converted = StarRocksStatsUtils.statisticDomainValue(value, column.getType());
                    if (converted.isPresent()) {
                        min = Math.min(min, converted.getAsDouble());
                        max = Math.max(max, converted.getAsDouble());
                    }
                }
            }
        }
        ColumnStatistic.Builder builder = ColumnStatistic.buildFrom(base);
        if (!Double.isInfinite(min) && min <= max) {
            builder.setMinValue(min);
            builder.setMaxValue(max);
        }
        double narrowedNdv;
        if (!isRange && !distinctValues.isEmpty()) {
            narrowedNdv = distinctValues.size();
        } else {
            // Proportional scaling, the same approximation the native range
            // partition tightening uses.
            narrowedNdv = Math.max(1, base.getDistinctValuesCount() * selectedCount / Math.max(1, totalCount));
        }
        builder.setDistinctValuesCount(Math.max(1, Math.min(narrowedNdv, Math.max(selectedRowCount, 1))));
        return builder.build();
    }

    private static ColumnStatistic refineWithPartitionStats(
            ColumnStatistic base,
            Map<Long, Map<String, StarRocksRemoteTableStats.PartitionColumnStats>> partitionStats,
            String columnName, long selectedRowCount) {
        long ndvSum = 0;
        long nullSum = 0;
        long rowSum = 0;
        boolean any = false;
        for (Map<String, StarRocksRemoteTableStats.PartitionColumnStats> columnStats : partitionStats.values()) {
            // groupPartitionStats keys the maps by lowercase column name.
            StarRocksRemoteTableStats.PartitionColumnStats stats =
                    columnStats.get(columnName.toLowerCase(Locale.ROOT));
            if (stats == null) {
                return base;
            }
            ndvSum += stats.ndv;
            nullSum += stats.nullCount;
            rowSum += stats.rowCount;
            any = true;
        }
        if (!any) {
            return base;
        }
        ColumnStatistic.Builder builder = ColumnStatistic.buildFrom(base);
        double ndv = Math.min(ndvSum, base.getDistinctValuesCount() > 0 ?
                Math.min(base.getDistinctValuesCount(), Math.max(selectedRowCount, 1)) :
                Math.max(selectedRowCount, 1));
        builder.setDistinctValuesCount(Math.max(1, ndv));
        if (rowSum > 0) {
            builder.setNullsFraction(Math.min(1.0, nullSum * 1.0 / rowSum));
        }
        return builder.build();
    }

}
