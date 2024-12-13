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
<<<<<<< HEAD
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.Pair;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.PredicateSearchKey;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.common.ErrorType;
=======
import com.starrocks.common.Pair;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.PredicateSearchKey;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.statistics.StatisticsUtils;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.ConnectContext;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
=======
import java.util.Iterator;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private final Set<PredicateSearchKey> scannedTables = new HashSet<>();
    private final DeltaStatisticProvider statisticProvider = new DeltaStatisticProvider();
<<<<<<< HEAD

    public DeltaLakeMetadata(HdfsEnvironment hdfsEnvironment, String catalogName, DeltaMetastoreOperations deltaOps,
                             DeltaLakeCacheUpdateProcessor cacheUpdateProcessor) {
=======
    private final ConnectorProperties properties;

    public DeltaLakeMetadata(HdfsEnvironment hdfsEnvironment, String catalogName, DeltaMetastoreOperations deltaOps,
                             DeltaLakeCacheUpdateProcessor cacheUpdateProcessor, ConnectorProperties properties) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        this.hdfsEnvironment = hdfsEnvironment;
        this.catalogName = catalogName;
        this.deltaOps = deltaOps;
        this.cacheUpdateProcessor = cacheUpdateProcessor;
<<<<<<< HEAD
=======
        this.properties = properties;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.DELTALAKE;
    }

    @Override
    public List<String> listDbNames() {
        return deltaOps.getAllDatabaseNames();
    }

    @Override
    public List<String> listTableNames(String dbName) {
        return deltaOps.getAllTableNames(dbName);
    }

    @Override
    public Database getDb(String dbName) {
        return deltaOps.getDb(dbName);
    }

    @Override
<<<<<<< HEAD
    public List<String> listPartitionNames(String databaseName, String tableName, long snapshotId) {
=======
    public List<String> listPartitionNames(String databaseName, String tableName, ConnectorMetadatRequestContext requestContext) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return deltaOps.getPartitionKeys(databaseName, tableName);
    }

    @Override
<<<<<<< HEAD
    public List<RemoteFileInfo> getRemoteFileInfos(Table table, List<PartitionKey> partitionKeys,
                                                   long snapshotId, ScalarOperator operator,
                                                   List<String> fieldNames, long limit) {
        DeltaLakeTable deltaLakeTable = (DeltaLakeTable) table;
        RemoteFileInfo remoteFileInfo = new RemoteFileInfo();
        String dbName = deltaLakeTable.getDbName();
        String tableName = deltaLakeTable.getTableName();
        PredicateSearchKey key = PredicateSearchKey.of(dbName, tableName, snapshotId, operator);

        triggerDeltaLakePlanFilesIfNeeded(key, table, operator, fieldNames);

        List<FileScanTask> scanTasks = splitTasks.get(key);
        if (scanTasks == null) {
            throw new StarRocksConnectorException("Missing iceberg split task for table:[{}.{}]. predicate:[{}]",
                    dbName, tableName, operator);
        }

        List<RemoteFileDesc> remoteFileDescs = Lists.newArrayList(
                DeltaLakeRemoteFileDesc.createDeltaLakeRemoteFileDesc(scanTasks));
        remoteFileInfo.setFiles(remoteFileDescs);
        return Lists.newArrayList(remoteFileInfo);
=======
    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        DeltaLakeTable deltaLakeTable = (DeltaLakeTable) table;
        String dbName = deltaLakeTable.getCatalogDBName();
        String tableName = deltaLakeTable.getCatalogTableName();
        PredicateSearchKey key =
                PredicateSearchKey.of(dbName, tableName, params.getTableVersionRange().end().get(), params.getPredicate());

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
        return buildRemoteInfoSource(table, params.getPredicate(), false);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session, Table table, Map<ColumnRefOperator, Column> columns,
<<<<<<< HEAD
                                         List<PartitionKey> partitionKeys, ScalarOperator predicate, long limit) {
        DeltaLakeTable deltaLakeTable = (DeltaLakeTable) table;
        SnapshotImpl snapshot = (SnapshotImpl) deltaLakeTable.getDeltaSnapshot();
        String dbName = deltaLakeTable.getDbName();
        String tableName = deltaLakeTable.getTableName();
=======
                                         List<PartitionKey> partitionKeys, ScalarOperator predicate, long limit,
                                         TableVersionRange versionRange) {
        if (!properties.enableGetTableStatsFromExternalMetadata()) {
            return StatisticsUtils.buildDefaultStatistics(columns.keySet());
        }

        DeltaLakeTable deltaLakeTable = (DeltaLakeTable) table;
        SnapshotImpl snapshot = (SnapshotImpl) deltaLakeTable.getDeltaSnapshot();
        String dbName = deltaLakeTable.getCatalogDBName();
        String tableName = deltaLakeTable.getCatalogTableName();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        Engine engine = deltaLakeTable.getDeltaEngine();
        StructType schema = deltaLakeTable.getDeltaMetadata().getSchema();
        PredicateSearchKey key = PredicateSearchKey.of(dbName, tableName, snapshot.getVersion(engine), predicate);

        DeltaUtils.checkProtocolAndMetadata(snapshot.getProtocol(), snapshot.getMetadata());

        triggerDeltaLakePlanFilesIfNeeded(key, deltaLakeTable, predicate, columns);

        List<FileScanTask> deltaLakeScanTasks = splitTasks.get(key);
        if (deltaLakeScanTasks == null) {
            throw new StarRocksConnectorException("Missing delta split task for table:[{}.{}]. predicate:[{}]",
                    dbName, table, predicate);
        }

<<<<<<< HEAD
        if (session.getSessionVariable().enableDeltaLakeColumnStatistics()) {
            try (Timer ignored = Tracers.watchScope(EXTERNAL, "DELTA_LAKE.getTableStatistics" + key)) {
                return statisticProvider.getTableStatistics(schema, key, columns);
            }
        } else {
            try (Timer ignored = Tracers.watchScope(EXTERNAL, "DELTA_LAKE.getCardinalityStats" + key)) {
=======
        String traceLabel = session.getSessionVariable().enableDeltaLakeColumnStatistics() ?
                "DELTA_LAKE.getTableStatistics" + key : "DELTA_LAKE.getCardinalityStats" + key;
        try (Timer ignored = Tracers.watchScope(EXTERNAL, traceLabel)) {
            if (session.getSessionVariable().enableDeltaLakeColumnStatistics()) {
                return statisticProvider.getTableStatistics(schema, key, columns);
            } else {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                return statisticProvider.getCardinalityStats(schema, key, columns);
            }
        }
    }

    private void triggerDeltaLakePlanFilesIfNeeded(PredicateSearchKey key, Table table,
                                                   ScalarOperator operator, Map<ColumnRefOperator, Column> columns) {
        if (!scannedTables.contains(key)) {
            try (Timer ignored = Tracers.watchScope(Tracers.get(), EXTERNAL, "DELTA_LAKE.processSplit." + key)) {
                List<String> fieldNames = columns.keySet().stream()
                        .map(ColumnRefOperator::getName).collect(Collectors.toList());
                collectDeltaLakePlanFiles(key, table, operator, ConnectContext.get(), fieldNames);
            }
        }
    }

    private void triggerDeltaLakePlanFilesIfNeeded(PredicateSearchKey key, Table table,
                                                   ScalarOperator operator, List<String> fieldNames) {
        if (!scannedTables.contains(key)) {
            try (Timer ignored = Tracers.watchScope(Tracers.get(), EXTERNAL, "DELTA_LAKE.processSplit." + key)) {
                collectDeltaLakePlanFiles(key, table, operator, ConnectContext.get(), fieldNames);
            }
        }
    }

<<<<<<< HEAD
    private void collectDeltaLakePlanFiles(PredicateSearchKey key, Table table, ScalarOperator operator,
                                           ConnectContext connectContext, List<String> fieldNames) {
=======
    private Iterator<Pair<FileScanTask, DeltaLakeAddFileStatsSerDe>> buildFileScanTaskIterator(
            Table table, ScalarOperator operator, boolean enableCollectColumnStats) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        DeltaLakeTable deltaLakeTable = (DeltaLakeTable) table;
        Metadata metadata = deltaLakeTable.getDeltaMetadata();
        Engine engine = deltaLakeTable.getDeltaEngine();
        SnapshotImpl snapshot = (SnapshotImpl) deltaLakeTable.getDeltaSnapshot();

        StructType schema = metadata.getSchema();
        Set<String> partitionColumns = metadata.getPartitionColNames();

        List<ScalarOperator> scalarOperators = Utils.extractConjuncts(operator);
        ScalarOperationToDeltaLakeExpr.DeltaLakeContext deltaLakeContext =
                new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(schema, partitionColumns);
        Predicate deltaLakePredicate = new ScalarOperationToDeltaLakeExpr().convert(scalarOperators, deltaLakeContext);

        ScanBuilderImpl scanBuilder = (ScanBuilderImpl) snapshot.getScanBuilder(engine);
        ScanImpl scan = (ScanImpl) scanBuilder.withFilter(engine, deltaLakePredicate).build();
<<<<<<< HEAD
=======
        long estimateRowSize = table.getColumns().stream().mapToInt(column -> column.getType().getTypeSize()).sum();
        return new Iterator<>() {
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
                return ScanFileUtils.convertFromRowToFileScanTask(enableCollectColumnStats, scanFileRow,
                                estimateRowSize, dv);
            }

            private void ensureOpen() {
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
        };
    }

    private RemoteFileInfoSource buildRemoteInfoSource(Table table, ScalarOperator operator, boolean enableCollectColumnStats) {
        Iterator<Pair<FileScanTask, DeltaLakeAddFileStatsSerDe>> iterator =
                buildFileScanTaskIterator(table, operator, enableCollectColumnStats);
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
        };
    }

    private void collectDeltaLakePlanFiles(PredicateSearchKey key, Table table, ScalarOperator operator,
                                           ConnectContext connectContext, List<String> fieldNames) {
        DeltaLakeTable deltaLakeTable = (DeltaLakeTable) table;
        Metadata metadata = deltaLakeTable.getDeltaMetadata();
        StructType schema = metadata.getSchema();
        Set<String> partitionColumns = metadata.getPartitionColNames();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

        Set<String> nonPartitionPrimitiveColumns;
        Set<String> partitionPrimitiveColumns;
        if (fieldNames != null) {
            nonPartitionPrimitiveColumns = fieldNames.stream()
                    .filter(column -> DeltaDataType.canUseStatsType(schema.get(column).getDataType())
<<<<<<< HEAD
                    && !partitionColumns.contains(column))
                    .collect(Collectors.toSet());
            partitionPrimitiveColumns = fieldNames.stream()
                    .filter(column -> DeltaDataType.canUseStatsType(schema.get(column).getDataType())
                    && partitionColumns.contains(column))
=======
                            && !partitionColumns.contains(column))
                    .collect(Collectors.toSet());
            partitionPrimitiveColumns = fieldNames.stream()
                    .filter(column -> DeltaDataType.canUseStatsType(schema.get(column).getDataType())
                            && partitionColumns.contains(column))
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD

        long estimateRowSize = table.getColumns().stream().mapToInt(column -> column.getType().getTypeSize()).sum();

        try (CloseableIterator<FilteredColumnarBatch> scanFilesAsBatches = scan.getScanFiles(engine, true)) {
            while (scanFilesAsBatches.hasNext()) {
                FilteredColumnarBatch scanFileBatch = scanFilesAsBatches.next();
                try (CloseableIterator<Row> scanFileRows = scanFileBatch.getRows()) {
                    while (scanFileRows.hasNext()) {
                        Row scanFileRow = scanFileRows.next();
                        DeletionVectorDescriptor dv = InternalScanFileUtils.getDeletionVectorDescriptorFromRow(scanFileRow);
                        if (dv != null) {
                            ErrorReport.reportValidateException(ErrorCode.ERR_BAD_TABLE_ERROR, ErrorType.UNSUPPORTED,
                                    "Delta table feature [deletion vectors] is not supported");
                        }

                        if (enableCollectColumnStatistics(connectContext)) {
                            Pair<FileScanTask, DeltaLakeAddFileStatsSerDe> pair =
                                    ScanFileUtils.convertFromRowToFileScanTask(true, scanFileRow, estimateRowSize);
                            files.add(pair.first);

                            try (Timer ignored = Tracers.watchScope(EXTERNAL, "DELTA_LAKE.updateDeltaLakeFileStats")) {
                                statisticProvider.updateFileStats(deltaLakeTable, key, pair.first, pair.second,
                                        nonPartitionPrimitiveColumns, partitionPrimitiveColumns);
                            }
                        } else {
                            Pair<FileScanTask, DeltaLakeAddFileStatsSerDe> pair =
                                    ScanFileUtils.convertFromRowToFileScanTask(false, scanFileRow, estimateRowSize);
                            files.add(pair.first);

                            try (Timer ignored = Tracers.watchScope(EXTERNAL, "DELTA_LAKE.updateDeltaLakeCardinality")) {
                                statisticProvider.updateFileStats(deltaLakeTable, key, pair.first, pair.second,
                                        nonPartitionPrimitiveColumns, partitionPrimitiveColumns);
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            LOG.error("Failed to get delta lake scan files", e);
            throw new StarRocksConnectorException("Failed to get delta lake scan files", e);
        }

=======
        boolean enableCollectColumnStats = enableCollectColumnStatistics(connectContext);
        String traceLabel = enableCollectColumnStats ? "DELTA_LAKE.updateDeltaLakeFileStats" :
                "DELTA_LAKE.updateDeltaLakeCardinality";

        Iterator<Pair<FileScanTask, DeltaLakeAddFileStatsSerDe>> iterator =
                buildFileScanTaskIterator(table, operator, enableCollectColumnStats);
        while (iterator.hasNext()) {
            Pair<FileScanTask, DeltaLakeAddFileStatsSerDe> pair = iterator.next();
            files.add(pair.first);
            try (Timer ignored = Tracers.watchScope(EXTERNAL, traceLabel)) {
                statisticProvider.updateFileStats(deltaLakeTable, key, pair.first, pair.second,
                        nonPartitionPrimitiveColumns, partitionPrimitiveColumns);
            }
        }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
    public Table getTable(String dbName, String tblName) {
        try {
            return deltaOps.getTable(dbName, tblName);
        } catch (Exception e) {
            LOG.error("Failed to get table {}.{}", dbName, tblName, e);
            return null;
        }
    }

    @Override
    public boolean tableExists(String dbName, String tblName) {
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
