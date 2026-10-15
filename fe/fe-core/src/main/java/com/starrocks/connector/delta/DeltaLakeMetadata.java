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

package com.starrocks.connector.delta;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorTableVersion;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetaPreparationItem;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.PartitionCastPredicatePruner;
import com.starrocks.connector.PredicateSearchKey;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoDefaultSource;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.statistics.StatisticsUtils;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.ScanBuilderImpl;
import io.delta.kernel.internal.ScanImpl;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

public class DeltaLakeMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeMetadata.class);
    private final String catalogName;
    private final DeltaMetastoreOperations deltaOps;
    private final HdfsEnvironment hdfsEnvironment;
    private final DeltaLakeCacheUpdateProcessor cacheUpdateProcessor;
    private final Map<PredicateSearchKey, List<FileScanTask>> splitTasks = new ConcurrentHashMap<>();
    private final Set<PredicateSearchKey> scannedTables = ConcurrentHashMap.newKeySet();
    // A query-level Delta metadata instance is shared by the optimizer's preparation tasks and the coordinator.
    // One future per key prevents duplicate log replay when a table appears in multiple logical scan operators.
    private final Map<PredicateSearchKey, CompletableFuture<Void>> scanTaskFutures = new ConcurrentHashMap<>();
    private final DeltaStatisticProvider statisticProvider = new DeltaStatisticProvider();
    private final ConnectorProperties properties;

    public DeltaLakeMetadata(HdfsEnvironment hdfsEnvironment, String catalogName, DeltaMetastoreOperations deltaOps,
                             DeltaLakeCacheUpdateProcessor cacheUpdateProcessor, ConnectorProperties properties) {
        this.hdfsEnvironment = hdfsEnvironment;
        this.catalogName = catalogName;
        this.deltaOps = deltaOps;
        this.cacheUpdateProcessor = cacheUpdateProcessor;
        this.properties = properties;
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.DELTALAKE;
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        return deltaOps.getAllDatabaseNames();
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        return deltaOps.getAllTableNames(dbName);
    }

    @Override
    public Database getDb(ConnectContext context, String dbName) {
        return deltaOps.getDb(dbName);
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName,
                                           ConnectorMetadataRequestContext requestContext) {
        return deltaOps.getPartitionKeys(databaseName, tableName);
    }

    @Override
    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        DeltaLakeTable deltaLakeTable = (DeltaLakeTable) table;
        String dbName = deltaLakeTable.getCatalogDBName();
        String tableName = deltaLakeTable.getCatalogTableName();
        PredicateSearchKey key =
                PredicateSearchKey.of(dbName, tableName, params);

        triggerDeltaLakePlanFilesIfNeeded(key, table, params.getPredicate(), params.getFieldNames());

        List<FileScanTask> scanTasks = splitTasks.get(key);
        if (scanTasks == null) {
            throw new StarRocksConnectorException("Missing deltalake split task for table:[{}.{}]. predicate:[{}]",
                    dbName, tableName, params.getPredicate());
        }

        return scanTasks.stream().map(DeltaRemoteFileInfo::new).collect(Collectors.toList());
    }

    @Override
    public RemoteFileInfoSource getRemoteFilesAsync(Table table, GetRemoteFilesParams params) {
        DeltaLakeTable deltaLakeTable = (DeltaLakeTable) table;
        PredicateSearchKey key = PredicateSearchKey.of(deltaLakeTable.getCatalogDBName(),
                deltaLakeTable.getCatalogTableName(), params);
        List<FileScanTask> preparedTasks = getPreparedScanTasks(key);
        if (preparedTasks != null) {
            return new RemoteFileInfoDefaultSource(preparedTasks.stream()
                    .map(DeltaRemoteFileInfo::new).collect(Collectors.toList()));
        }
        return buildRemoteInfoSource(table, params);
    }

    @Override
    public TvrVersionRange getTableVersionRange(String dbName, Table table,
                                                Optional<ConnectorTableVersion> startVersion,
                                                Optional<ConnectorTableVersion> endVersion) {
        // Delta has no time-travel resolution yet (unlike Iceberg), so an ordinary scan always arrives here
        // with both versions empty. Resolve to the table's pinned snapshot, matching the version key that
        // getTableStatistics()/DeltaLakeScanNode build from deltaSnapshot.getVersion() - otherwise the scan
        // operator keeps the TvrVersionRange.empty() default, prepareMetadata() treats that as "nothing to
        // do" (item.getVersion().end().isEmpty()), and pre-collection becomes a no-op for ordinary scans.
        if (startVersion.isPresent() || endVersion.isPresent()) {
            return TvrTableSnapshot.empty();
        }
        SnapshotImpl snapshot = (SnapshotImpl) ((DeltaLakeTable) table).getDeltaSnapshot();
        if (snapshot == null) {
            return TvrTableSnapshot.empty();
        }
        return TvrTableSnapshot.of(snapshot.getVersion());
    }

    @Override
    public boolean prepareMetadata(MetaPreparationItem item, Tracers tracers, ConnectContext connectContext) {
        if (!(item.getTable() instanceof DeltaLakeTable) || item.getVersion() == null || item.getVersion().end().isEmpty()) {
            return true;
        }

        DeltaLakeTable deltaLakeTable = (DeltaLakeTable) item.getTable();
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setTableVersionRange(item.getVersion())
                .setPredicate(item.getPredicate())
                .setLimit(item.getLimit())
                .build();
        PredicateSearchKey key = PredicateSearchKey.of(deltaLakeTable.getCatalogDBName(),
                deltaLakeTable.getCatalogTableName(), params);
        List<String> fieldNames = deltaLakeTable.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        tracers = tracers == null ? Tracers.get() : tracers;
        connectContext = connectContext == null ? ConnectContext.get() : connectContext;
        try (Timer ignored = Tracers.watchScope(tracers, EXTERNAL, "DELTA_LAKE.prepareMetadata")) {
            triggerDeltaLakePlanFilesIfNeeded(key, deltaLakeTable, item.getPredicate(), connectContext, fieldNames,
                    tracers);
        }
        return true;
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session, Table table, Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys, ScalarOperator predicate, long limit,
                                         TvrVersionRange versionRange) {
        if (!properties.enableGetTableStatsFromExternalMetadata()) {
            return StatisticsUtils.buildDefaultStatistics(columns.keySet());
        }

        DeltaLakeTable deltaLakeTable = (DeltaLakeTable) table;
        SnapshotImpl snapshot = (SnapshotImpl) deltaLakeTable.getDeltaSnapshot();
        String dbName = deltaLakeTable.getCatalogDBName();
        String tableName = deltaLakeTable.getCatalogTableName();
        StructType schema = deltaLakeTable.getDeltaMetadata().getSchema();

        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setTableVersionRange(TvrTableSnapshot.of(snapshot.getVersion()))
                .setPredicate(predicate)
                .setLimit(limit)
                .build();
        PredicateSearchKey key = PredicateSearchKey.of(dbName, tableName, params);

        DeltaUtils.checkProtocolAndMetadata(snapshot.getProtocol(), snapshot.getMetadata());

        triggerDeltaLakePlanFilesIfNeeded(key, deltaLakeTable, predicate, columns);

        List<FileScanTask> deltaLakeScanTasks = splitTasks.get(key);
        if (deltaLakeScanTasks == null) {
            throw new StarRocksConnectorException("Missing delta split task for table:[{}.{}]. predicate:[{}]",
                    dbName, table, predicate);
        }

        String traceLabel = session.getSessionVariable().enableDeltaLakeColumnStatistics() ?
                "DELTA_LAKE.getTableStatistics" + key : "DELTA_LAKE.getCardinalityStats" + key;
        try (Timer ignored = Tracers.watchScope(EXTERNAL, traceLabel)) {
            if (session.getSessionVariable().enableDeltaLakeColumnStatistics()) {
                return statisticProvider.getTableStatistics(schema, key, columns);
            } else {
                return statisticProvider.getCardinalityStats(schema, key, columns);
            }
        }
    }

    private void triggerDeltaLakePlanFilesIfNeeded(PredicateSearchKey key, Table table,
                                                   ScalarOperator operator, Map<ColumnRefOperator, Column> columns) {
        List<String> fieldNames = columns.keySet().stream()
                .map(ColumnRefOperator::getName).collect(Collectors.toList());
        triggerDeltaLakePlanFilesIfNeeded(key, table, operator, ConnectContext.get(), fieldNames, Tracers.get());
    }

    private void triggerDeltaLakePlanFilesIfNeeded(PredicateSearchKey key, Table table,
                                                   ScalarOperator operator, List<String> fieldNames) {
        triggerDeltaLakePlanFilesIfNeeded(key, table, operator, ConnectContext.get(), fieldNames, Tracers.get());
    }

    private void triggerDeltaLakePlanFilesIfNeeded(PredicateSearchKey key, Table table,
                                                   ScalarOperator operator, ConnectContext connectContext,
                                                   List<String> fieldNames, Tracers tracers) {
        if (scannedTables.contains(key)) {
            return;
        }

        CompletableFuture<Void> newFuture = new CompletableFuture<>();
        CompletableFuture<Void> existingFuture = scanTaskFutures.putIfAbsent(key, newFuture);
        if (existingFuture != null) {
            try {
                existingFuture.join();
            } catch (CompletionException e) {
                // The other caller's collection attempt failed; the caller here (getRemoteFiles /
                // getTableStatistics) will see splitTasks.get(key) == null and surface the usual
                // "Missing deltalake split task" error instead of a raw CompletionException.
                LOG.warn("Delta Lake metadata collection failed for {} on a concurrent caller", key, e);
            }
            return;
        }

        // The previous collector may have finished after the check above but before it removed its
        // completed future. Do not replay the log (and merge its file statistics a second time) when
        // that happens. Complete our future so callers that raced with this check can proceed too.
        if (scannedTables.contains(key)) {
            newFuture.complete(null);
            scanTaskFutures.remove(key, newFuture);
            return;
        }

        ConnectContext.ScopeGuard contextGuard = connectContext != null ? connectContext.bindScope() : null;
        Tracers prevTracers = Tracers.get();
        Tracers.set(tracers);
        try {
            try (Timer ignored = Tracers.watchScope(tracers, EXTERNAL, "DELTA_LAKE.processSplit." + key)) {
                collectDeltaLakePlanFiles(key, table, operator, connectContext, fieldNames, tracers);
            }
            newFuture.complete(null);
        } catch (RuntimeException | Error e) {
            statisticProvider.discardFileStats(key);
            newFuture.completeExceptionally(e);
            throw e;
        } finally {
            // Drop the entry once the future is resolved (success or failure) so it doesn't linger
            // for the rest of the query; getPreparedScanTasks falls back to splitTasks/resync below.
            scanTaskFutures.remove(key, newFuture);
            Tracers.set(prevTracers);
            if (contextGuard != null) {
                contextGuard.close();
            }
        }
    }

    private List<FileScanTask> getPreparedScanTasks(PredicateSearchKey key) {
        CompletableFuture<Void> future = scanTaskFutures.get(key);
        if (future != null) {
            try {
                future.join();
            } catch (CompletionException e) {
                // The pre-collection attempt for this key failed (already logged by the caller that
                // ran it); treat it as not-prepared so getRemoteFilesAsync falls back to collecting
                // synchronously instead of propagating a wrapped CompletionException.
                LOG.warn("Delta Lake metadata pre-collection failed for {}, falling back to synchronous collection",
                        key, e);
                return null;
            }
        }
        return splitTasks.get(key);
    }

    private CloseableIterator<Pair<FileScanTask, DeltaLakeAddFileStatsSerDe>> buildFileScanTaskIterator(
            Table table, ScalarOperator operator, boolean enableCollectColumnStats) {
        DeltaLakeTable deltaLakeTable = (DeltaLakeTable) table;
        Metadata metadata = deltaLakeTable.getDeltaMetadata();
        Engine engine = deltaLakeTable.getDeltaEngine();
        SnapshotImpl snapshot = (SnapshotImpl) deltaLakeTable.getDeltaSnapshot();

        StructType schema = metadata.getSchema();
        Set<String> partitionColumns = metadata.getPartitionColNames();

        List<ScalarOperator> scalarOperators = Utils.extractConjuncts(operator);
        // Cast-on-string-partition conjuncts (e.g. CAST(c AS DATETIME) = <ts>) are unwrapped by
        // ScalarOperationToDeltaLakeExpr into a string-domain comparison, which never equals a 'yyyy-MM-dd'
        // partition value and would prune every file (wrong empty result / under-estimated cardinality). Keep them
        // out of the pushed predicate and evaluate them StarRocks-side against each file's partition values below.
        PartitionCastPredicatePruner.PartitionResidual residual = PartitionCastPredicatePruner.split(
                scalarOperators, identityStringPartitionColumns(deltaLakeTable, partitionColumns));
        ScalarOperationToDeltaLakeExpr.DeltaLakeContext deltaLakeContext =
                new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(schema, partitionColumns);
        Predicate deltaLakePredicate = new ScalarOperationToDeltaLakeExpr().convert(residual.pushable, deltaLakeContext);

        // Delta kernel's SnapshotImpl/LogReplay lazily initialize internal state (e.g. the log segment) via
        // unsynchronized check-then-act Lazy fields. Pre-collection can run two scans that reference the same
        // underlying table (and therefore the same cached SnapshotImpl, e.g. a self-join) on different threads;
        // synchronize all kernel access on the shared snapshot so first-access initialization isn't racy.
        ScanBuilderImpl scanBuilder;
        ScanImpl scan;
        synchronized (snapshot) {
            scanBuilder = (ScanBuilderImpl) snapshot.getScanBuilder();
            scan = (ScanImpl) scanBuilder.withFilter(deltaLakePredicate).build();
        }
        long estimateRowSize = table.getColumns().stream().mapToInt(column -> column.getType().getTypeSize()).sum();
        CloseableIterator<Pair<FileScanTask, DeltaLakeAddFileStatsSerDe>> baseIterator = new CloseableIterator<>() {
            CloseableIterator<FilteredColumnarBatch> scanFilesAsBatches;
            CloseableIterator<Row> scanFileRows;
            boolean hasMore = true;

            @Override
            public boolean hasNext() {
                if (hasMore) {
                    ensureOpen();
                }
                return hasMore;
            }

            @Override
            public Pair<FileScanTask, DeltaLakeAddFileStatsSerDe> next() {
                ensureOpen();
                Row scanFileRow = scanFileRows.next();

                DeletionVectorDescriptor dv = InternalScanFileUtils.getDeletionVectorDescriptorFromRow(scanFileRow);
                return ScanFileUtils.convertFromRowToFileScanTask(enableCollectColumnStats, scanFileRow, metadata,
                        estimateRowSize, dv);
            }

            private void ensureOpen() {
                synchronized (snapshot) {
                    try {
                        if (scanFilesAsBatches == null) {
                            scanFilesAsBatches = scan.getScanFiles(engine, true);
                        }
                        while (scanFileRows == null || !scanFileRows.hasNext()) {
                            if (!scanFilesAsBatches.hasNext()) {
                                scanFilesAsBatches.close();
                                hasMore = false;
                                break;
                            }
                            if (scanFileRows != null) {
                                scanFileRows.close();
                            }
                            FilteredColumnarBatch scanFileBatch = scanFilesAsBatches.next();
                            scanFileRows = scanFileBatch.getRows();
                        }
                    } catch (IOException e) {
                        LOG.error("Failed to get delta lake scan files", e);
                        throw new StarRocksConnectorException("Failed to get delta lake scan files", e);
                    }
                }
            }

            @Override
            public void close() {
                try {
                    if (scanFileRows != null) {
                        scanFileRows.close();
                    }
                } catch (Exception ignored) {
                }
                try {
                    if (scanFilesAsBatches != null) {
                        scanFilesAsBatches.close();
                    }
                } catch (Exception ignored) {
                } finally {
                    scanFileRows = null;
                    scanFilesAsBatches = null;
                    hasMore = false;
                }
            }
        };

        if (residual.residual.isEmpty()) {
            return baseIterator;
        }
        // Evaluate the cast-on-string-partition residual StarRocks-side: keep a file unless its partition value
        // provably cannot satisfy the residual (partitionMayMatch drops only on a definitive false), consistent
        // with the backend filter and never dropping a possibly-matching file.
        return new CloseableIterator<>() {
            Pair<FileScanTask, DeltaLakeAddFileStatsSerDe> nextMatch = null;
            boolean finished = false;

            @Override
            public boolean hasNext() {
                advance();
                return nextMatch != null;
            }

            @Override
            public Pair<FileScanTask, DeltaLakeAddFileStatsSerDe> next() {
                advance();
                if (nextMatch == null) {
                    throw new java.util.NoSuchElementException();
                }
                Pair<FileScanTask, DeltaLakeAddFileStatsSerDe> result = nextMatch;
                nextMatch = null;
                return result;
            }

            private void advance() {
                if (nextMatch != null || finished) {
                    return;
                }
                while (baseIterator.hasNext()) {
                    Pair<FileScanTask, DeltaLakeAddFileStatsSerDe> candidate = baseIterator.next();
                    if (PartitionCastPredicatePruner.partitionMayMatch(
                            residual.residual, candidate.first.getPartitionValues())) {
                        nextMatch = candidate;
                        return;
                    }
                }
                finished = true;
            }

            @Override
            public void close() {
                try {
                    baseIterator.close();
                } catch (IOException e) {
                    LOG.error("Failed to close delta lake scan file iterator", e);
                }
            }
        };
    }

    // Partition columns of string type. Delta partitioning is always identity, so every string partition column is
    // an identity string partition column for PartitionCastPredicatePruner.split. Package-private for testing.
    static Set<String> identityStringPartitionColumns(DeltaLakeTable table, Set<String> partitionColumns) {
        Set<String> lowerPartitionColumns = new HashSet<>();
        for (String name : partitionColumns) {
            lowerPartitionColumns.add(name.toLowerCase(Locale.ROOT));
        }
        Set<String> result = new HashSet<>();
        for (Column column : table.getColumns()) {
            if (lowerPartitionColumns.contains(column.getName().toLowerCase(Locale.ROOT))
                    && column.getType().isStringType()) {
                result.add(column.getName().toLowerCase(Locale.ROOT));
            }
        }
        return result;
    }

    private RemoteFileInfoSource buildRemoteInfoSource(Table table, GetRemoteFilesParams params) {
        CloseableIterator<Pair<FileScanTask, DeltaLakeAddFileStatsSerDe>> iterator =
                buildFileScanTaskIterator(table, params.getPredicate(), params.isEnableColumnStats());
        return new RemoteFileInfoSource() {
            @Override
            public RemoteFileInfo getOutput() {
                Pair<FileScanTask, DeltaLakeAddFileStatsSerDe> pair = iterator.next();
                return new DeltaRemoteFileInfo(pair.first);
            }

            @Override
            public boolean hasMoreOutput() {
                return iterator.hasNext();
            }

            @Override
            public void close() {
                try {
                    iterator.close();
                } catch (Exception ignored) {
                }
            }
        };
    }

    private void collectDeltaLakePlanFiles(PredicateSearchKey key, Table table, ScalarOperator operator,
                                           ConnectContext connectContext, List<String> fieldNames, Tracers tracers) {
        DeltaLakeTable deltaLakeTable = (DeltaLakeTable) table;
        Metadata metadata = deltaLakeTable.getDeltaMetadata();
        StructType schema = metadata.getSchema();
        Set<String> partitionColumns = metadata.getPartitionColNames();

        Set<String> nonPartitionPrimitiveColumns;
        Set<String> partitionPrimitiveColumns;
        if (fieldNames != null) {
            nonPartitionPrimitiveColumns = fieldNames.stream()
                    .filter(column -> DeltaDataType.canUseStatsType(schema.get(column).getDataType())
                            && !partitionColumns.contains(column))
                    .collect(Collectors.toSet());
            partitionPrimitiveColumns = fieldNames.stream()
                    .filter(column -> DeltaDataType.canUseStatsType(schema.get(column).getDataType())
                            && partitionColumns.contains(column))
                    .collect(Collectors.toSet());
        } else {
            nonPartitionPrimitiveColumns = schema.fieldNames().stream()
                    .filter(column -> DeltaDataType.canUseStatsType(schema.get(column).getDataType())
                            && !partitionColumns.contains(column)).collect(Collectors.toSet());
            partitionPrimitiveColumns = partitionColumns.stream()
                    .filter(column -> DeltaDataType.canUseStatsType(schema.get(column).getDataType()))
                    .collect(Collectors.toSet());
        }

        List<FileScanTask> files = Lists.newArrayList();
        boolean enableCollectColumnStats = enableCollectColumnStatistics(connectContext);
        String traceLabel = enableCollectColumnStats ? "DELTA_LAKE.updateDeltaLakeFileStats" :
                "DELTA_LAKE.updateDeltaLakeCardinality";

        try (CloseableIterator<Pair<FileScanTask, DeltaLakeAddFileStatsSerDe>> iterator =
                buildFileScanTaskIterator(table, operator, enableCollectColumnStats)) {
            while (iterator.hasNext()) {
                Pair<FileScanTask, DeltaLakeAddFileStatsSerDe> pair = iterator.next();
                files.add(pair.first);
                try (Timer ignored = Tracers.watchScope(tracers, EXTERNAL, traceLabel)) {
                    statisticProvider.updateFileStats(deltaLakeTable, key, pair.first, pair.second,
                            nonPartitionPrimitiveColumns, partitionPrimitiveColumns);
                }
            }
        } catch (IOException e) {
            throw new StarRocksConnectorException("Failed to iter deltalake file scan iterator", e);
        }
        splitTasks.put(key, files);
        scannedTables.add(key);
    }

    public boolean enableCollectColumnStatistics(ConnectContext context) {
        if (context == null) {
            return false;
        }

        if (context.getSessionVariable() == null) {
            return false;
        }

        return context.getSessionVariable().enableDeltaLakeColumnStatistics();
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        try {
            return deltaOps.getTable(dbName, tblName);
        } catch (Exception e) {
            LOG.error("Failed to get deltalake table {}.{}.{}", catalogName, dbName, tblName, e);
            throw new StarRocksConnectorException(
                    String.format("Failed to get Delta Lake table %s.%s.%s", catalogName, dbName, tblName), e);
        }
    }

    @Override
    public boolean tableExists(ConnectContext context, String dbName, String tblName) {
        return deltaOps.tableExists(dbName, tblName);
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        return hdfsEnvironment.getCloudConfiguration();
    }

    public MetastoreType getMetastoreType() {
        return deltaOps.getMetastoreType();
    }

    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
        if (cacheUpdateProcessor != null) {
            cacheUpdateProcessor.refreshTable(srDbName, table, onlyCachedPartitions);
        }
    }

    @Override
    public void clear() {
        deltaOps.invalidateAll();
    }
}
