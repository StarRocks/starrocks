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

package com.starrocks.connector.hudi;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.HiveCacheUpdateProcessor;
import com.starrocks.connector.hive.HiveMetastoreOperations;
import com.starrocks.connector.hive.HiveStatisticsProvider;
import com.starrocks.connector.hive.Partition;
import com.starrocks.credential.CloudConfiguration;
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
import java.util.stream.Collectors;

import static com.starrocks.connector.PartitionUtil.toHivePartitionName;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;

public class HudiMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(HudiMetadata.class);
    private final String catalogName;
    private final HdfsEnvironment hdfsEnvironment;

    private final HiveMetastoreOperations hmsOps;
    private final RemoteFileOperations fileOps;
    private final HiveStatisticsProvider statisticsProvider;
    private final Optional<HiveCacheUpdateProcessor> cacheUpdateProcessor;

    public HudiMetadata(String catalogName,
                        HdfsEnvironment hdfsEnvironment,
                        HiveMetastoreOperations hmsOps,
                        RemoteFileOperations fileOperations,
                        HiveStatisticsProvider statisticsProvider,
                        Optional<HiveCacheUpdateProcessor> cacheUpdateProcessor) {
        this.catalogName = catalogName;
        this.hdfsEnvironment = hdfsEnvironment;
        this.hmsOps = hmsOps;
        this.fileOps = fileOperations;
        this.statisticsProvider = statisticsProvider;
        this.cacheUpdateProcessor = cacheUpdateProcessor;
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.HUDI;
    }

    @Override
    public List<String> listDbNames() {
        return hmsOps.getAllDatabaseNames();
    }

    @Override
    public List<String> listTableNames(String dbName) {
        return hmsOps.getAllTableNames(dbName);
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName, TableVersionRange version) {
        return hmsOps.getPartitionKeys(dbName, tblName);
    }

    @Override
    public List<String> listPartitionNamesByValue(String databaseName, String tableName,
                                                  List<Optional<String>> partitionValues) {
        return hmsOps.getPartitionKeysByValue(databaseName, tableName, partitionValues);
    }

    @Override
    public Database getDb(String dbName) {
        Database database;
        try {
            database = hmsOps.getDb(dbName);
        } catch (Exception e) {
            LOG.error("Failed to get hudi database [{}.{}]", catalogName, dbName, e);
            return null;
        }

        return database;
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        Table table;
        try {
            table = hmsOps.getTable(dbName, tblName);
        } catch (Exception e) {
            LOG.error("Failed to get hudi table [{}.{}.{}]", catalogName, dbName, tblName, e);
            return null;
        }

        return table;
    }

    @Override
    public boolean tableExists(String dbName, String tblName) {
        return hmsOps.tableExists(dbName, tblName);
    }

    private List<Partition> buildGetRemoteFilesPartitions(Table table, GetRemoteFilesParams params) {
        ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
        HiveMetaStoreTable hmsTbl = (HiveMetaStoreTable) table;

        if (((HiveMetaStoreTable) table).isUnPartitioned()) {
            partitions.add(hmsOps.getPartition(hmsTbl.getDbName(), hmsTbl.getTableName(), Lists.newArrayList()));
        } else {
            // convert partition keys to partition names.
            // and handle partition names in following code.
            // in most cases, we use partition keys. but in some cases,  we use partition names.
            // so partition keys has higher priority than partition names.
            List<String> partitionNames = params.getPartitionNames();
            if (params.getPartitionKeys() != null) {
                partitionNames =
                        params.getPartitionKeys().stream().map(x -> toHivePartitionName(hmsTbl.getPartitionColumnNames(), x))
                                .collect(
                                        Collectors.toList());
            }
            // check existences
            Map<String, Partition> existingPartitions = hmsOps.getPartitionByNames(table, partitionNames);
            for (String hivePartitionName : partitionNames) {
                Partition partition = existingPartitions.get(hivePartitionName);
                if (partition != null) {
                    partitions.add(partition);
                } else if (params.isCheckPartitionExistence()) {
                    LOG.error("Partition {} doesn't exist", hivePartitionName);
                    throw new StarRocksConnectorException("Partition %s doesn't exist", hivePartitionName);
                }
            }
        }
        return partitions.build();
    }

    @Override
    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        List<Partition> partitions = buildGetRemoteFilesPartitions(table, params);
        return fileOps.getRemoteFiles(table, partitions, params);
    }

    @Override
    public RemoteFileInfoSource getRemoteFilesAsync(Table table, GetRemoteFilesParams params) {
        List<Partition> partitions = buildGetRemoteFilesPartitions(table, params);
        return fileOps.getRemoteFilesAsync(table, partitions, params);
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate, long limit, TableVersionRange version) {
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
            Table table = GlobalStateMgr.getCurrentState()
                    .getLocalMetastore().getTable(dbName, tableName);
            HiveMetaStoreTable hmsTable = (HiveMetaStoreTable) table;
            if (hmsTable != null) {
                cacheUpdateProcessor.ifPresent(processor -> processor.invalidateTable(
                        hmsTable.getDbName(), hmsTable.getTableName(), table));
            }
        }
    }

    @Override
    public void clear() {
        hmsOps.invalidateAll();
        fileOps.invalidateAll();
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        return hdfsEnvironment.getCloudConfiguration();
    }
}
