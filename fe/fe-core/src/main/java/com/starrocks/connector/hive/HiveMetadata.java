// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.external.RemoteFileInfo;
import com.starrocks.external.RemoteFileOperations;
import com.starrocks.external.hive.HiveMetastoreOperations;
import com.starrocks.external.hive.HiveStatisticsProvider;
import com.starrocks.external.hive.Partition;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;

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
            LOG.error("Failed to get hive database [{}.{}]", catalogName, dbName);
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
            LOG.error("Failed to get hive table [{}.{}.{}]", catalogName, dbName, tblName);
            return null;
        }

        return table;
    }

    public List<RemoteFileInfo> getRemoteFileInfos(Table table, List<PartitionKey> partitionKeys) {
        ImmutableCollection.Builder<Partition> partitions = ImmutableList.builder();
        String dbName = ((HiveMetaStoreTable) table).getDbName();
        String tblName = ((HiveMetaStoreTable) table).getTableName();

        if (((HiveMetaStoreTable) table).isUnPartitioned()) {
            partitions.add(hmsOps.getPartition(dbName, tblName, Lists.newArrayList()));
        } else {
            partitions.addAll(hmsOps.getPartitionByNames(table, partitionKeys).values());
        }

        return fileOps.getRemoteFiles(partitions.build());
    }

    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         List<ColumnRefOperator> columns,
                                         List<PartitionKey> partitionKeys) {
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

    public void refreshTable(String dbName, String tableName, Table table, List<String> partitionNames) {
        if (partitionNames != null && partitionNames.size() > 1) {
            cacheUpdateProcessor.ifPresent(processor -> processor.refreshPartition(table, partitionNames));
        } else {
            cacheUpdateProcessor.ifPresent(processor -> processor.refreshTable(dbName, tableName, table));
        }
    }

    public void clearQueryLevelCache() {
        hmsOps.invalidateAll();
        fileOps.invalidateAll();
    }
}
