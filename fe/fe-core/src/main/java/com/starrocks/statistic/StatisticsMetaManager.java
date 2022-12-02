// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.statistic;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.analysis.HashDistributionDesc;
import com.starrocks.analysis.KeysDesc;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class StatisticsMetaManager extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(StatisticsMetaManager.class);

    // If all replicas are lost more than 3 times in a row, rebuild the statistics table
    private int lossTableCount = 0;

    public StatisticsMetaManager() {
        super("statistics meta manager", 60 * 1000);
    }

    private boolean checkDatabaseExist() {
        return GlobalStateMgr.getCurrentState().getDb(StatsConstants.STATISTICS_DB_NAME) != null;
    }

    private boolean createDatabase() {
        LOG.info("create statistics db start");
        CreateDbStmt dbStmt = new CreateDbStmt(false, StatsConstants.STATISTICS_DB_NAME);
        try {
            GlobalStateMgr.getCurrentState().getMetadata().createDb(dbStmt.getFullDbName());
        } catch (UserException e) {
            LOG.warn("Failed to create database " + e.getMessage());
            return false;
        }
        LOG.info("create statistics db down");
        return checkDatabaseExist();
    }

    private boolean checkTableExist(String tableName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(StatsConstants.STATISTICS_DB_NAME);
        Preconditions.checkState(db != null);
        return db.getTable(tableName) != null;
    }

    private boolean checkReplicateNormal(String tableName) {
        int aliveSize = GlobalStateMgr.getCurrentSystemInfo().getAliveBackendNumber();
        int total = GlobalStateMgr.getCurrentSystemInfo().getTotalBackendNumber();
        // maybe cluster just shutdown, ignore
        if (aliveSize <= total / 2) {
            lossTableCount = 0;
            return true;
        }

        Database db = GlobalStateMgr.getCurrentState().getDb(StatsConstants.STATISTICS_DB_NAME);
        Preconditions.checkState(db != null);
        OlapTable table = (OlapTable) db.getTable(tableName);
        Preconditions.checkState(table != null);

        boolean check = true;
        for (Partition partition : table.getPartitions()) {
            // check replicate miss
            if (partition.getBaseIndex().getTablets().stream()
                    .anyMatch(t -> ((LocalTablet) t).getNormalReplicaBackendIds().isEmpty())) {
                check = false;
                break;
            }
        }

        if (!check) {
            lossTableCount++;
        } else {
            lossTableCount = 0;
        }

        return lossTableCount < 3;
    }

    private static final List<String> keyColumnNames = ImmutableList.of(
            "table_id", "column_name", "db_id"
    );

    private static final List<String> fullStatisticsKeyColumns = ImmutableList.of(
            "table_id", "partition_id", "column_name"
    );

    private static final List<String> histogramKeyColumns = ImmutableList.of(
            "table_id", "column_name"
    );

    private boolean createSampleStatisticsTable(ConnectContext context) {
        LOG.info("create statistics table start");
        TableName tableName = new TableName(StatsConstants.STATISTICS_DB_NAME,
                StatsConstants.SAMPLE_STATISTICS_TABLE_NAME);
        Map<String, String> properties = Maps.newHashMap();
        int defaultReplicationNum = Math.min(3, GlobalStateMgr.getCurrentSystemInfo().getTotalBackendNumber());
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
        CreateTableStmt stmt = new CreateTableStmt(false, false,
                tableName,
                StatisticUtils.buildStatsColumnDef(StatsConstants.SAMPLE_STATISTICS_TABLE_NAME),
                "olap",
                new KeysDesc(KeysType.UNIQUE_KEYS, keyColumnNames),
                null,
                new HashDistributionDesc(10, keyColumnNames),
                properties,
                null,
                "");

        Analyzer.analyze(stmt, context);
        try {
            GlobalStateMgr.getCurrentState().createTable(stmt);
        } catch (DdlException e) {
            LOG.warn("Failed to create table" + e.getMessage());
            return false;
        }
        LOG.info("create statistics table done");
        refreshAnalyzeJob();
        return checkTableExist(StatsConstants.SAMPLE_STATISTICS_TABLE_NAME);
    }

    private boolean createFullStatisticsTable(ConnectContext context) {
        LOG.info("create statistics table v2 start");
        TableName tableName = new TableName(StatsConstants.STATISTICS_DB_NAME,
                StatsConstants.FULL_STATISTICS_TABLE_NAME);
        Map<String, String> properties = Maps.newHashMap();
        int defaultReplicationNum = Math.min(3, GlobalStateMgr.getCurrentSystemInfo().getTotalBackendNumber());
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
        CreateTableStmt stmt = new CreateTableStmt(false, false,
                tableName,
                StatisticUtils.buildStatsColumnDef(StatsConstants.FULL_STATISTICS_TABLE_NAME),
                "olap",
                new KeysDesc(KeysType.PRIMARY_KEYS, fullStatisticsKeyColumns),
                null,
                new HashDistributionDesc(10, fullStatisticsKeyColumns),
                properties,
                null,
                "");
   
        Analyzer.analyze(stmt, context);
        try {
            GlobalStateMgr.getCurrentState().createTable(stmt);
        } catch (DdlException e) {
            LOG.warn("Failed to create table" + e.getMessage());
            return false;
        }
        LOG.info("create statistics table done");
        refreshAnalyzeJob();
        return checkTableExist(StatsConstants.FULL_STATISTICS_TABLE_NAME);
    }

    private boolean createHistogramStatisticsTable(ConnectContext context) {
        LOG.info("create statistics table v2 start");
        TableName tableName = new TableName(StatsConstants.STATISTICS_DB_NAME,
                StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME);
        Map<String, String> properties = Maps.newHashMap();
        int defaultReplicationNum = Math.min(3, GlobalStateMgr.getCurrentSystemInfo().getTotalBackendNumber());
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
        CreateTableStmt stmt = new CreateTableStmt(false, false,
                tableName,
                StatisticUtils.buildStatsColumnDef(StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME),
                "olap",
                new KeysDesc(KeysType.PRIMARY_KEYS, histogramKeyColumns),
                null,
                new HashDistributionDesc(10, histogramKeyColumns),
                properties,
                null,
                "");
        
        Analyzer.analyze(stmt, context);
        try {
            GlobalStateMgr.getCurrentState().createTable(stmt);
        } catch (DdlException e) {
            LOG.warn("Failed to create table" + e.getMessage());
            return false;
        }
        LOG.info("create statistics table done");
        for (Map.Entry<Pair<Long, String>, HistogramStatsMeta> entry :
                GlobalStateMgr.getCurrentAnalyzeMgr().getHistogramStatsMetaMap().entrySet()) {
            HistogramStatsMeta histogramStatsMeta = entry.getValue();
            GlobalStateMgr.getCurrentAnalyzeMgr().addHistogramStatsMeta(new HistogramStatsMeta(
                    histogramStatsMeta.getDbId(), histogramStatsMeta.getTableId(), histogramStatsMeta.getColumn(),
                    histogramStatsMeta.getType(), LocalDateTime.MIN, histogramStatsMeta.getProperties()));
        }
        return checkTableExist(StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME);
    }

    private void refreshAnalyzeJob() {
        for (Map.Entry<Long, BasicStatsMeta> entry :
                GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().entrySet()) {
            BasicStatsMeta basicStatsMeta = entry.getValue();
            GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(new BasicStatsMeta(
                    basicStatsMeta.getDbId(), basicStatsMeta.getTableId(), basicStatsMeta.getColumns(),
                    basicStatsMeta.getType(), LocalDateTime.MIN, basicStatsMeta.getProperties()));
        }

        for (AnalyzeJob analyzeJob : GlobalStateMgr.getCurrentAnalyzeMgr().getAllAnalyzeJobList()) {
            analyzeJob.setWorkTime(LocalDateTime.MIN);
            GlobalStateMgr.getCurrentAnalyzeMgr().updateAnalyzeJobWithLog(analyzeJob);
        }
    }

    private boolean dropTable(String tableName) {
        LOG.info("drop statistics table start");
        DropTableStmt stmt = new DropTableStmt(true,
                new TableName(StatsConstants.STATISTICS_DB_NAME, tableName), true);

        try {
            GlobalStateMgr.getCurrentState().dropTable(stmt);
        } catch (DdlException e) {
            LOG.warn("Failed to drop table" + e.getMessage());
            return false;
        }
        LOG.info("drop statistics table done");
        return !checkTableExist(tableName);
    }

    private void trySleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            LOG.warn(e.getMessage());
        }
    }

    private boolean createTable(String tableName) {
        ConnectContext context = StatisticUtils.buildConnectContext();
        context.setThreadLocalInfo();

        if (tableName.equals(StatsConstants.SAMPLE_STATISTICS_TABLE_NAME)) {
            return createSampleStatisticsTable(context);
        } else if (tableName.equals(StatsConstants.FULL_STATISTICS_TABLE_NAME)) {
            return createFullStatisticsTable(context);
        } else if (tableName.equals(StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME)) {
            return createHistogramStatisticsTable(context);
        } else {
            throw new StarRocksPlannerException("Error table name " + tableName, ErrorType.INTERNAL_ERROR);
        }
    }

    private void refreshStatisticsTable(String tableName) {
        while (checkTableExist(tableName) && !checkReplicateNormal(tableName)) {
            LOG.info("statistics table " + tableName + " replicate is not normal, will drop table and rebuild");
            if (dropTable(tableName)) {
                break;
            }
            LOG.warn("drop statistics table " + tableName + " failed");
            trySleep(10000);
        }

        while (!checkTableExist(tableName)) {
            if (createTable(tableName)) {
                break;
            }
            LOG.warn("create statistics table " + tableName + " failed");
            trySleep(10000);
        }
    }


    @Override
    protected void runAfterCatalogReady() {
        // To make UT pass, some UT will create database and table
        trySleep(Config.statistic_manager_sleep_time_sec * 1000);
        while (!checkDatabaseExist()) {
            if (createDatabase()) {
                break;
            }
            trySleep(10000);
        }

        refreshStatisticsTable(StatsConstants.SAMPLE_STATISTICS_TABLE_NAME);
        refreshStatisticsTable(StatsConstants.FULL_STATISTICS_TABLE_NAME);
        refreshStatisticsTable(StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME);

        GlobalStateMgr.getCurrentAnalyzeMgr().clearStatisticFromDroppedTable();
        GlobalStateMgr.getCurrentAnalyzeMgr().clearExpiredAnalyzeStatus();
    }
}
