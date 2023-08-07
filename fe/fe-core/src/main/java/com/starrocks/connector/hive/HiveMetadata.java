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


package com.starrocks.connector.hive;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.connector.PartitionUtil.toHivePartitionName;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;

public class HiveMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(HiveMetadata.class);
    private final String catalogName;
    private final HiveMetastoreOperations hmsOps;
    private final RemoteFileOperations fileOps;
    private final HiveStatisticsProvider statisticsProvider;
    private final Optional<CacheUpdateProcessor> cacheUpdateProcessor;

    public HiveMetadata(String catalogName,
                        HiveMetastoreOperations hmsOps,
                        RemoteFileOperations fileOperations,
                        HiveStatisticsProvider statisticsProvider,
                        Optional<CacheUpdateProcessor> cacheUpdateProcessor) {
        this.catalogName = catalogName;
        this.hmsOps = hmsOps;
        this.fileOps = fileOperations;
        this.statisticsProvider = statisticsProvider;
        this.cacheUpdateProcessor = cacheUpdateProcessor;
    }

    @Override
    public List<String> listDbNames() {
        return hmsOps.getAllDatabaseNames();
    }

    @Override
    public void createDb(String dbName, Map<String, String> properties) throws AlreadyExistsException {
        if (dbExists(dbName)) {
            throw new AlreadyExistsException("Database Already Exists");
        }
        hmsOps.createDb(dbName, properties);
    }

    @Override
    public void dropDb(String dbName, boolean isForceDrop) throws MetaNotFoundException {
        if (listTableNames(dbName).size() != 0) {
            throw new StarRocksConnectorException("Database %s not empty", dbName);
        }

        hmsOps.dropDb(dbName, isForceDrop);
    }

    @Override
    public Database getDb(String dbName) {
        Database database;
        try {
            database = hmsOps.getDb(dbName);
        } catch (Exception e) {
            LOG.error("Failed to get hive database [{}.{}]", catalogName, dbName, e);
            return null;
        }

        return database;
    }

    @Override
    public List<String> listTableNames(String dbName) {
        return hmsOps.getAllTableNames(dbName);
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName) {
        return hmsOps.getPartitionKeys(dbName, tblName);
    }

    @Override
    public List<String> listPartitionNamesByValue(String dbName, String tblName,
                                                  List<Optional<String>> partitionValues) {
        return hmsOps.getPartitionKeysByValue(dbName, tblName, partitionValues);
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        Table table;
        try {
            table = hmsOps.getTable(dbName, tblName);
        } catch (Exception e) {
            LOG.error("Failed to get hive table [{}.{}.{}]", catalogName, dbName, tblName, e);
            return null;
        }

        return table;
    }

    @Override
    public List<RemoteFileInfo> getRemoteFileInfos(Table table, List<PartitionKey> partitionKeys,
                                                   long snapshotId, ScalarOperator predicate, List<String> fieldNames) {
        ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
        HiveMetaStoreTable hmsTbl = (HiveMetaStoreTable) table;

        if (((HiveMetaStoreTable) table).isUnPartitioned()) {
            partitions.add(hmsOps.getPartition(hmsTbl.getDbName(), hmsTbl.getTableName(), Lists.newArrayList()));
        } else {
            Map<String, Partition> existingPartitions = hmsOps.getPartitionByPartitionKeys(table, partitionKeys);
            for (PartitionKey partitionKey : partitionKeys) {
                String hivePartitionName = toHivePartitionName(hmsTbl.getPartitionColumnNames(), partitionKey);
                Partition partition = existingPartitions.get(hivePartitionName);
                if (partition != null) {
                    partitions.add(partition);
                } else {
                    LOG.error("Partition {} doesn't exist", hivePartitionName);
                    throw new StarRocksConnectorException("Partition %s doesn't exist", hivePartitionName);
                }
            }
        }

        boolean useRemoteFileCache = true;
        if (table instanceof HiveTable) {
            useRemoteFileCache = ((HiveTable) table).isUseMetadataCache();
        }

        return fileOps.getRemoteFiles(partitions.build(), useRemoteFileCache);
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        HiveMetaStoreTable hmsTbl = (HiveMetaStoreTable) table;
        if (hmsTbl.isUnPartitioned()) {
            return Lists.newArrayList(hmsOps.getPartition(hmsTbl.getDbName(), hmsTbl.getTableName(),
                    Lists.newArrayList()));
        } else {
            ImmutableList.Builder<PartitionInfo> partitions = ImmutableList.builder();
            Map<String, Partition> partitionMap = hmsOps.getPartitionByNames(table, partitionNames);
            partitionNames.forEach(partitionName -> partitions.add(partitionMap.get(partitionName)));
            return partitions.build();
        }
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate) {
        Statistics statistics = null;
        List<ColumnRefOperator> columnRefOperators = Lists.newArrayList(columns.keySet());
        try {
            if (session.getSessionVariable().enableHiveColumnStats()) {
                statistics = statisticsProvider.getTableStatistics(session, table, columnRefOperators, partitionKeys);
            } else {
                statistics = Statistics.builder().build();
                LOG.warn("Session variable {} is false when getting table statistics on table {}",
                        SessionVariable.ENABLE_HIVE_COLUMN_STATS, table);
            }
        } catch (Exception e) {
            LOG.warn("Failed to get table column statistics on [{}]. error : {}", table, e);
        } finally {
            statistics = statistics == null ? Statistics.builder().build() : statistics;
            Map<ColumnRefOperator, ColumnStatistic> columnStatistics = statistics.getColumnStatistics();
            if (columnStatistics.isEmpty()) {
                double outputRowNums = statistics.getOutputRowCount();
                statistics = statisticsProvider.createUnknownStatistics(table, columnRefOperators, partitionKeys, outputRowNums);
            }
        }

        Preconditions.checkState(columnRefOperators.size() == statistics.getColumnStatistics().size());
        if (session.getDumpInfo() != null) {
            for (ColumnRefOperator column : columnRefOperators) {
                session.getDumpInfo().addTableStatistics(table, column.getName(), statistics.getColumnStatistic(column));
            }

            HiveMetaStoreTable hmsTable = (HiveMetaStoreTable) table;
            session.getDumpInfo().getHMSTable(hmsTable.getResourceName(), hmsTable.getDbName(), table.getName())
                    .setScanRowCount(statistics.getOutputRowCount());
        }

        return statistics;
    }

    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
        if (partitionNames != null && partitionNames.size() > 0) {
            cacheUpdateProcessor.ifPresent(processor -> processor.refreshPartition(table, partitionNames));
        } else {
            cacheUpdateProcessor.ifPresent(processor -> processor.refreshTable(srDbName, table, onlyCachedPartitions));
        }
    }

    @Override
    public void dropTable(DropTableStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();
        if (isResourceMappingCatalog(catalogName)) {
            HiveMetaStoreTable hmsTable = (HiveMetaStoreTable) GlobalStateMgr.getCurrentState()
                    .getMetadata().getTable(dbName, tableName);
            cacheUpdateProcessor.ifPresent(processor -> processor.invalidateTable(
                    hmsTable.getDbName(), hmsTable.getTableName(), hmsTable.getTableLocation()));
        } else {
            if (!stmt.isForceDrop()) {
                throw new DdlException(String.format("Table location will be cleared." +
                        " 'Force' must be set when dropping a hive table." +
                        " Please execute 'drop table %s.%s.%s force'", stmt.getCatalogName(), dbName, tableName));
            }
            hmsOps.dropTable(dbName, tableName);
        }
    }

    @Override
    public void clear() {
        hmsOps.invalidateAll();
        fileOps.invalidateAll();
    }
}
