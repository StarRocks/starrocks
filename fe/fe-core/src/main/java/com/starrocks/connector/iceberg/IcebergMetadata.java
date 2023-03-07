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


package com.starrocks.connector.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.cost.IcebergStatisticProvider;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.starrocks.connector.PartitionUtil.convertIcebergPartitionToPartitionName;
import static com.starrocks.connector.iceberg.IcebergCatalogType.GLUE_CATALOG;
import static com.starrocks.connector.iceberg.IcebergCatalogType.HIVE_CATALOG;
import static com.starrocks.connector.iceberg.IcebergCatalogType.REST_CATALOG;

public class IcebergMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(IcebergMetadata.class);
    private final String catalogName;
    private final IcebergCatalog icebergCatalog;
    private final IcebergStatisticProvider statisticProvider = new IcebergStatisticProvider();

    private final Map<TableIdentifier, Table> tables = new ConcurrentHashMap<>();
    private final Map<IcebergFilter, List<FileScanTask>> tasks = new ConcurrentHashMap<>();

    public IcebergMetadata(String catalogName, IcebergCatalog icebergCatalog) {
        this.catalogName = catalogName;
        this.icebergCatalog = icebergCatalog;
    }

    @Override
    public List<String> listDbNames() {
        return icebergCatalog.listAllDatabases();
    }

    @Override
    public Database getDb(String dbName) {
        try {
            return icebergCatalog.getDB(dbName);
        } catch (InterruptedException | TException e) {
            LOG.error("Failed to get iceberg database " + dbName, e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }

    @Override
    public List<String> listTableNames(String dbName) {
        List<TableIdentifier> tableIdentifiers = icebergCatalog.listTables(Namespace.of(dbName));
        return tableIdentifiers.stream().map(TableIdentifier::name).collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        TableIdentifier identifier = TableIdentifier.of(dbName, tblName);
        if (tables.containsKey(identifier)) {
            return tables.get(identifier);
        }

        try {
            org.apache.iceberg.Table icebergTable = icebergCatalog.loadTable(identifier);
            Table table = IcebergApiConverter.toIcebergTable(
                    icebergTable, catalogName, dbName, tblName, icebergCatalog.getIcebergCatalogType().name());
            tables.put(identifier, table);
            return table;
        } catch (StarRocksConnectorException e) {
            LOG.error("Failed to get iceberg table {}", identifier, e);
            return null;
        }
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName) {
        org.apache.iceberg.Table icebergTable
                = icebergCatalog.loadTable(TableIdentifier.of(dbName, tblName));
        IcebergCatalogType nativeType = icebergCatalog.getIcebergCatalogType();

        if (nativeType != HIVE_CATALOG && nativeType != REST_CATALOG && nativeType != GLUE_CATALOG) {
            throw new StarRocksConnectorException(
                    "Do not support get partitions from catalog type: " + nativeType);
        }

        if (icebergTable.spec().fields().stream()
                .anyMatch(partitionField -> !partitionField.transform().isIdentity())) {
            throw new StarRocksConnectorException(
                    "Do not support get partitions from No-Identity partition transform now");
        }

        List<String> partitionNames = Lists.newArrayList();
        TableScan tableScan = icebergTable.newScan();
        List<FileScanTask> tasks = Lists.newArrayList(tableScan.planFiles());
        if (icebergTable.spec().isUnpartitioned()) {
            return partitionNames;
        }

        if (icebergTable.spec().fields().stream()
                .anyMatch(partitionField -> !partitionField.transform().isIdentity())) {
            return partitionNames;
        }

        for (FileScanTask fileScanTask : tasks) {
            StructLike partition = fileScanTask.file().partition();
            partitionNames.add(convertIcebergPartitionToPartitionName(icebergTable.spec(), partition));
        }
        return partitionNames;
    }

    @Override
    public List<RemoteFileInfo> getRemoteFileInfos(Table table, List<PartitionKey> partitionKeys,
                                                   long snapshotId, ScalarOperator predicate) {
        return getRemoteFileInfos((IcebergTable) table, snapshotId, predicate);
    }

    private List<RemoteFileInfo> getRemoteFileInfos(IcebergTable table, long snapshotId, ScalarOperator predicate) {
        RemoteFileInfo remoteFileInfo = new RemoteFileInfo();
        IcebergFilter key = IcebergFilter.of(table.getRemoteDbName(), table.getRemoteTableName(), snapshotId, predicate);

        if (!tasks.containsKey(key)) {
            List<ScalarOperator> scalarOperators = Utils.extractConjuncts(predicate);
            org.apache.iceberg.Table nativeTbl = table.getNativeTable();
            Types.StructType schema = nativeTbl.schema().asStruct();
            ScalarOperatorToIcebergExpr.IcebergContext icebergContext = new ScalarOperatorToIcebergExpr.IcebergContext(schema);
            Expression icebergPredicate = new ScalarOperatorToIcebergExpr().convert(scalarOperators, icebergContext);

            ImmutableList.Builder<FileScanTask> builder = ImmutableList.builder();
            org.apache.iceberg.Table nativeTable = table.getNativeTable();
            TableScan scan = nativeTable.newScan().useSnapshot(snapshotId);
            if (icebergPredicate.op() != Expression.Operation.TRUE) {
                scan = scan.filter(icebergPredicate);
            }

            for (CombinedScanTask combinedScanTask : scan.planTasks()) {
                for (FileScanTask fileScanTask : combinedScanTask.files()) {
                    builder.add(fileScanTask);
                }
            }
            tasks.put(key, builder.build());
        }

        List<RemoteFileDesc> remoteFileDescs = ImmutableList.of(new RemoteFileDesc(tasks.get(key)));
        remoteFileInfo.setFiles(remoteFileDescs);

        return Lists.newArrayList(remoteFileInfo);
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate) {
        return statisticProvider.getTableStatistics((IcebergTable) table, predicate, columns);
    }

    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
        org.apache.iceberg.Table nativeTable = ((IcebergTable) table).getNativeTable();
        try {
            if (nativeTable instanceof BaseTable) {
                BaseTable baseTable = (BaseTable) nativeTable;
                if (baseTable.operations().refresh() == null) {
                    // If table is loaded successfully, current table metadata will never be null.
                    // So when we get a null metadata after refresh, it indicates the table has been dropped.
                    // See: https://github.com/StarRocks/starrocks/issues/3076
                    throw new NoSuchTableException("No such table: %s", nativeTable.name());
                }
            } else {
                // table loaded by GlobalStateMgr should be a base table
                throw new StarRocksConnectorException("Invalid table type of %s, it should be a BaseTable!", nativeTable.name());
            }
        } catch (NoSuchTableException e) {
            throw new StarRocksConnectorException("No such table  %s", nativeTable.name());
        } catch (IllegalStateException ei) {
            throw new StarRocksConnectorException("Refresh table %s with failure, the table under hood" +
                    " may have been dropped. You should re-create the external table. cause %s",
                    nativeTable.name(), ei.getMessage());
        }
    }

    @Override
    public void clear() {
        tasks.clear();
        tables.clear();
    }
}
