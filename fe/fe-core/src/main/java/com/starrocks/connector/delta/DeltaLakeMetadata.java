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
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
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
import io.delta.kernel.types.BasePrimitiveType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.ImmutableList.toImmutableList;
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

    public DeltaLakeMetadata(HdfsEnvironment hdfsEnvironment, String catalogName, DeltaMetastoreOperations deltaOps,
                             DeltaLakeCacheUpdateProcessor cacheUpdateProcessor) {
        this.hdfsEnvironment = hdfsEnvironment;
        this.catalogName = catalogName;
        this.deltaOps = deltaOps;
        this.cacheUpdateProcessor = cacheUpdateProcessor;
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
    public List<String> listPartitionNames(String databaseName, String tableName, long snapshotId) {
        return deltaOps.getPartitionKeys(databaseName, tableName);
    }

    @Override
    public List<RemoteFileInfo> getRemoteFileInfos(Table table, List<PartitionKey> partitionKeys,
                                                   long snapshotId, ScalarOperator operator,
                                                   List<String> fieldNames, long limit) {
        DeltaLakeTable deltaLakeTable = (DeltaLakeTable) table;
        RemoteFileInfo remoteFileInfo = new RemoteFileInfo();
        String dbName = deltaLakeTable.getDbName();
        String tableName = deltaLakeTable.getTableName();
        PredicateSearchKey key = PredicateSearchKey.of(dbName, tableName, snapshotId, operator);

        triggerDeltaLakePlanFilesIfNeeded(key, table, operator);

        List<FileScanTask> scanTasks = splitTasks.get(key);
        if (scanTasks == null) {
            throw new StarRocksConnectorException("Missing iceberg split task for table:[{}.{}]. predicate:[{}]",
                    dbName, tableName, operator);
        }

        List<RemoteFileDesc> remoteFileDescs = Lists.newArrayList(
                DeltaLakeRemoteFileDesc.createDeltaLakeRemoteFileDesc(scanTasks));
        remoteFileInfo.setFiles(remoteFileDescs);
        return Lists.newArrayList(remoteFileInfo);
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session, Table table, Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys, ScalarOperator predicate, long limit) {
        DeltaLakeTable deltaLakeTable = (DeltaLakeTable) table;
        SnapshotImpl snapshot = (SnapshotImpl) deltaLakeTable.getDeltaSnapshot();
        String dbName = deltaLakeTable.getDbName();
        String tableName = deltaLakeTable.getTableName();
        Engine engine = deltaLakeTable.getDeltaEngine();
        PredicateSearchKey key = PredicateSearchKey.of(dbName, tableName, snapshot.getVersion(engine), predicate);

        DeltaUtils.checkTableFeatureSupported(snapshot.getProtocol(), snapshot.getMetadata());

        triggerDeltaLakePlanFilesIfNeeded(key, deltaLakeTable, predicate);

        List<FileScanTask> deltaLakeScanTasks = splitTasks.get(key);
        if (deltaLakeScanTasks == null) {
            throw new StarRocksConnectorException("Missing delta split task for table:[{}.{}]. predicate:[{}]",
                    dbName, table, predicate);
        }

        if (session.getSessionVariable().enableDeltaLakeColumnStatistics()) {
            return statisticProvider.getTableStatistics(deltaLakeTable, columns, predicate);
        } else {
            try (Timer ignored = Tracers.watchScope(EXTERNAL, "DELTA_LAKE.calculateCardinality" + key)) {
                return statisticProvider.getCardinalityStats(columns, deltaLakeScanTasks);
            }
        }
    }

    private void triggerDeltaLakePlanFilesIfNeeded(PredicateSearchKey key, Table table, ScalarOperator operator) {
        if (!scannedTables.contains(key)) {
            try (Timer ignored = Tracers.watchScope(Tracers.get(), EXTERNAL, "DELTA_LAKE.processSplit." + key)) {
                collectDeltaLakePlanFiles(key, table, operator, ConnectContext.get());
            }
        }
    }

    private void collectDeltaLakePlanFiles(PredicateSearchKey key, Table table, ScalarOperator operator,
                                           ConnectContext connectContext) {
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

        List<String> nonPartitionPrimitiveColumns = schema.fieldNames().stream()
                .filter(column -> BasePrimitiveType.isPrimitiveType(
                        schema.get(column).getDataType().toString())
                        && !partitionColumns.contains(column)).collect(toImmutableList());

        List<FileScanTask> files = Lists.newArrayList();

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
                            FileScanTask fileScanTask = ScanFileUtils.convertFromRowToFileScanTask(
                                    true, scanFileRow);
                            files.add(fileScanTask);

                            try (Timer ignored = Tracers.watchScope(EXTERNAL, "DELTA_LAKE.updateDeltaLakeFileStats")) {
                                statisticProvider.updateFileStats(deltaLakeTable, key, fileScanTask,
                                        nonPartitionPrimitiveColumns);
                            }
                        } else {
                            FileScanTask fileScanTask = ScanFileUtils.convertFromRowToFileScanTask(false, scanFileRow);
                            files.add(fileScanTask);
                        }
                    }
                }
            }
        } catch (IOException e) {
            LOG.error("Failed to get delta lake scan files", e);
            throw new StarRocksConnectorException("Failed to get delta lake scan files", e);
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
