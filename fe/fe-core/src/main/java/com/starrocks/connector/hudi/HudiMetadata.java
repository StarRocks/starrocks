// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hudi;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.CacheUpdateProcessor;
import com.starrocks.connector.hive.HiveMetastoreOperations;
import com.starrocks.connector.hive.HiveStatisticsProvider;
import com.starrocks.connector.hive.Partition;
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

public class HudiMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(HudiMetadata.class);
    private final String catalogName;
    private final HiveMetastoreOperations hmsOps;
    private final RemoteFileOperations fileOps;
    private final HiveStatisticsProvider statisticsProvider;
    private final Optional<CacheUpdateProcessor> cacheUpdateProcessor;

    public HudiMetadata(String catalogName,
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
    public List<String> listTableNames(String dbName) {
        return hmsOps.getAllTableNames(dbName);
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName) {
        return hmsOps.getPartitionKeys(dbName, tblName);
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
    public List<RemoteFileInfo> getRemoteFileInfos(Table table, List<PartitionKey> partitionKeys,
                                                   ScalarOperator predicate, List<String> fieldNames) {
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

        return fileOps.getRemoteFiles(partitions.build(), Optional.of(hmsTbl.getTableLocation()));
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         List<ColumnRefOperator> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate) {
        Statistics statistics = null;
        try {
            if (session.getSessionVariable().enableHiveColumnStats()) {
                statistics = statisticsProvider.getTableStatistics(session, table, columns, partitionKeys);
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
                statistics = statisticsProvider.createUnknownStatistics(table, columns, partitionKeys, outputRowNums);
            }
        }

        Preconditions.checkState(columns.size() == statistics.getColumnStatistics().size());
        for (ColumnRefOperator column : columns) {
            session.getDumpInfo().addTableStatistics(table, column.getName(), statistics.getColumnStatistic(column));
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
            HudiTable hudiTable = (HudiTable) GlobalStateMgr.getCurrentState().getMetadata().getTable(dbName, tableName);
            cacheUpdateProcessor.ifPresent(processor -> processor.invalidateTable(
                    hudiTable.getDbName(), hudiTable.getTableName(), hudiTable.getTableLocation()));
        }
    }

    @Override
    public void clear() {
        hmsOps.invalidateAll();
        fileOps.invalidateAll();
    }
}