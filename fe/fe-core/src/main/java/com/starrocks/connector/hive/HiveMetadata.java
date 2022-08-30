// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.connector.hive;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorMetadata;

import com.starrocks.external.RemoteFileOperations;
import com.starrocks.external.hive.HiveMetastoreOperations;
import com.starrocks.external.hive.HiveStatisticsProvider;
import com.starrocks.external.hive.Partition;
import com.starrocks.external.hive.RemoteFileInfo;
import com.starrocks.external.hive.StarRocksConnectorException;

import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class HiveMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(HiveMetadata.class);

    private final HiveMetastoreOperations hmsOps;
    private final RemoteFileOperations fileOperations;
    private HiveStatisticsProvider statisticsProvider;

    public HiveMetadata(HiveMetastoreOperations hmsOps,
                        RemoteFileOperations fileOperations,
                        HiveStatisticsProvider statisticsProvider) {
        this.hmsOps = hmsOps;
        this.fileOperations = fileOperations;
        this.statisticsProvider = statisticsProvider;
    }

    @Override
    public List<String> listDbNames() throws DdlException {
        return hmsOps.listDbNames();
    }

    @Override
    public Database getDb(String dbName) {
        return hmsOps.getDb(dbName);
    }

    @Override
    public List<String> listTableNames(String dbName) throws DdlException {
        return hmsOps.getAllTableNames(dbName);
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        Table table;
        try {
            table = hmsOps.getTable(dbName, tblName);
        } catch (StarRocksConnectorException e) {
            LOG.error("Failed to get hive meta cache on {}.{}", dbName, tblName);
            return null;
        }

        return table;
    }

    @Override
    public List<String> getPartitionNames(String dbName, String tblName) {
        return hmsOps.getPartitionKeys(dbName, tblName);
    }

    public List<RemoteFileInfo> getRemoteFileInfos(Table table, List<PartitionKey> partitionKeys) {
        ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
        String dbName = ((HiveMetaStoreTable) table).getHiveDb();
        String tblName = ((HiveMetaStoreTable) table).getTableName();

        if (((HiveMetaStoreTable) table).getPartitionColumnNames().size() == 0) {
            partitions.add(hmsOps.getPartition(dbName, tblName, Lists.newArrayList()));
        } else {
            partitions.addAll(hmsOps.getPartitionByNames(table, partitionKeys).values());
        }

        return fileOperations.getRemoteFiles(partitions.build());
    }

    public Statistics getTableStatistics(OptimizerContext session,
                                          Table table,
                                          List<ColumnRefOperator> columns,
                                          List<PartitionKey> partitionKeys) {
        Statistics statistics = null;
        try {
            if (session.getSessionVariable().enableHiveColumnStats()) {
                statistics = statisticsProvider.getTableStatistics(table, columns, partitionKeys, session);
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

}
