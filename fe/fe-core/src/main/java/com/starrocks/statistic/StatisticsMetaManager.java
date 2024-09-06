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

package com.starrocks.statistic;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
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
import com.starrocks.common.util.AutoInferUtil;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.load.pipe.filelist.RepoCreator;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.KeysDesc;
import com.starrocks.sql.common.EngineType;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class StatisticsMetaManager extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(StatisticsMetaManager.class);

    // If all replicas are lost more than 3 times in a row, rebuild the statistics table
    private int lossTableCount = 0;

    public StatisticsMetaManager() {
        super("statistics meta manager", 60L * 1000L);
    }

    private boolean checkDatabaseExist() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(StatsConstants.STATISTICS_DB_NAME) != null;
    }

    private boolean createDatabase() {
        LOG.info("create statistics db start");
        CreateDbStmt dbStmt = new CreateDbStmt(false, StatsConstants.STATISTICS_DB_NAME);
        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(dbStmt.getFullDbName());
        } catch (UserException e) {
            LOG.warn("Failed to create database ", e);
            return false;
        }
        LOG.info("create statistics db down");
        return checkDatabaseExist();
    }

    private boolean checkTableExist(String tableName) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(StatsConstants.STATISTICS_DB_NAME);
        Preconditions.checkState(db != null);
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName) != null;
    }

    private boolean checkReplicateNormal(String tableName) {
        int aliveSize = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAliveBackendNumber();
        int total = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getTotalBackendNumber();
        // maybe cluster just shutdown, ignore
        if (aliveSize <= total / 2) {
            lossTableCount = 0;
            return true;
        }

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(StatsConstants.STATISTICS_DB_NAME);
        Preconditions.checkState(db != null);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        Preconditions.checkState(table != null);
        if (table.isCloudNativeTableOrMaterializedView()) {
            return true;
        }

        boolean check = true;
        for (Partition partition : table.getPartitions()) {
            // check replicate miss
            if (partition.getDefaultPhysicalPartition().getBaseIndex().getTablets().stream()
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

    private static final List<String> KEY_COLUMN_NAMES = ImmutableList.of(
            "table_id", "column_name", "db_id"
    );

    private static final List<String> FULL_STATISTICS_KEY_COLUMNS = ImmutableList.of(
            "table_id", "partition_id", "column_name"
    );

    private static final List<String> HISTOGRAM_KEY_COLUMNS = ImmutableList.of(
            "table_id", "column_name"
    );

    private static final List<String> EXTERNAL_FULL_STATISTICS_KEY_COLUMNS = ImmutableList.of(
            "table_uuid", "partition_name", "column_name"
    );

    private static final List<String> EXTERNAL_HISTOGRAM_KEY_COLUMNS = ImmutableList.of(
            "table_uuid", "column_name"
    );

    private boolean createSampleStatisticsTable(ConnectContext context) {
        LOG.info("create sample statistics table start");
        TableName tableName = new TableName(StatsConstants.STATISTICS_DB_NAME,
                StatsConstants.SAMPLE_STATISTICS_TABLE_NAME);
        Map<String, String> properties = Maps.newHashMap();
        try {
            int defaultReplicationNum = AutoInferUtil.calDefaultReplicationNum();
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
            KeysType keysType = KeysType.UNIQUE_KEYS;
            CreateTableStmt stmt = new CreateTableStmt(false, false,
                    tableName,
                    StatisticUtils.buildStatsColumnDef(StatsConstants.SAMPLE_STATISTICS_TABLE_NAME),
                    EngineType.defaultEngine().name(),
                    new KeysDesc(keysType, KEY_COLUMN_NAMES),
                    null,
                    new HashDistributionDesc(10, KEY_COLUMN_NAMES),
                    properties,
                    null,
                    "");

            Analyzer.analyze(stmt, context);
            GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        } catch (UserException e) {
            LOG.warn("Failed to create sample statistics, ", e);
            return false;
        }
        LOG.info("create sample statistics table done");
        refreshAnalyzeJob();
        return checkTableExist(StatsConstants.SAMPLE_STATISTICS_TABLE_NAME);
    }

    private boolean createFullStatisticsTable(ConnectContext context) {
        LOG.info("create full statistics table start");
        TableName tableName = new TableName(StatsConstants.STATISTICS_DB_NAME,
                StatsConstants.FULL_STATISTICS_TABLE_NAME);
        KeysType keysType = RunMode.isSharedDataMode() ? KeysType.UNIQUE_KEYS : KeysType.PRIMARY_KEYS;
        Map<String, String> properties = Maps.newHashMap();

        try {
            int defaultReplicationNum = AutoInferUtil.calDefaultReplicationNum();
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
            CreateTableStmt stmt = new CreateTableStmt(false, false,
                    tableName,
                    StatisticUtils.buildStatsColumnDef(StatsConstants.FULL_STATISTICS_TABLE_NAME),
                    EngineType.defaultEngine().name(),
                    new KeysDesc(keysType, FULL_STATISTICS_KEY_COLUMNS),
                    null,
                    new HashDistributionDesc(10, FULL_STATISTICS_KEY_COLUMNS),
                    properties,
                    null,
                    "");

            Analyzer.analyze(stmt, context);
            GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        } catch (UserException e) {
            LOG.warn("Failed to create full statistics table", e);
            return false;
        }
        LOG.info("create full statistics table done");
        refreshAnalyzeJob();
        return checkTableExist(StatsConstants.FULL_STATISTICS_TABLE_NAME);
    }

    private boolean createHistogramStatisticsTable(ConnectContext context) {
        LOG.info("create histogram statistics table start");
        TableName tableName = new TableName(StatsConstants.STATISTICS_DB_NAME,
                StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME);
        KeysType keysType = RunMode.isSharedDataMode() ? KeysType.UNIQUE_KEYS : KeysType.PRIMARY_KEYS;
        Map<String, String> properties = Maps.newHashMap();
        try {
            int defaultReplicationNum = AutoInferUtil.calDefaultReplicationNum();
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
            CreateTableStmt stmt = new CreateTableStmt(false, false,
                    tableName,
                    StatisticUtils.buildStatsColumnDef(StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME),
                    EngineType.defaultEngine().name(),
                    new KeysDesc(keysType, HISTOGRAM_KEY_COLUMNS),
                    null,
                    new HashDistributionDesc(10, HISTOGRAM_KEY_COLUMNS),
                    properties,
                    null,
                    "");

            Analyzer.analyze(stmt, context);
            GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        } catch (UserException e) {
            LOG.warn("Failed to create histogram statistics table", e);
            return false;
        }
        LOG.info("create histogram statistics table done");
        for (Map.Entry<Pair<Long, String>, HistogramStatsMeta> entry :
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().getHistogramStatsMetaMap().entrySet()) {
            HistogramStatsMeta histogramStatsMeta = entry.getValue();
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addHistogramStatsMeta(new HistogramStatsMeta(
                    histogramStatsMeta.getDbId(), histogramStatsMeta.getTableId(), histogramStatsMeta.getColumn(),
                    histogramStatsMeta.getType(), LocalDateTime.MIN, histogramStatsMeta.getProperties()));
        }
        return checkTableExist(StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME);
    }

    private boolean createExternalFullStatisticsTable(ConnectContext context) {
        LOG.info("create external full statistics table start");
        TableName tableName = new TableName(StatsConstants.STATISTICS_DB_NAME,
                StatsConstants.EXTERNAL_FULL_STATISTICS_TABLE_NAME);
        KeysType keysType = RunMode.isSharedDataMode() ? KeysType.UNIQUE_KEYS : KeysType.PRIMARY_KEYS;
        Map<String, String> properties = Maps.newHashMap();

        try {
            int defaultReplicationNum = AutoInferUtil.calDefaultReplicationNum();
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
            CreateTableStmt stmt = new CreateTableStmt(false, false,
                    tableName,
                    StatisticUtils.buildStatsColumnDef(StatsConstants.EXTERNAL_FULL_STATISTICS_TABLE_NAME),
                    EngineType.defaultEngine().name(),
                    new KeysDesc(keysType, EXTERNAL_FULL_STATISTICS_KEY_COLUMNS),
                    null,
                    new HashDistributionDesc(10, EXTERNAL_FULL_STATISTICS_KEY_COLUMNS),
                    properties,
                    null,
                    "");

            Analyzer.analyze(stmt, context);
            GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        } catch (UserException e) {
            LOG.warn("Failed to create full statistics table", e);
            return false;
        }
        LOG.info("create external full statistics table done");
        return checkTableExist(StatsConstants.EXTERNAL_FULL_STATISTICS_TABLE_NAME);
    }

    private boolean createExternalHistogramStatisticsTable(ConnectContext context) {
        LOG.info("create external histogram statistics table start");
        TableName tableName = new TableName(StatsConstants.STATISTICS_DB_NAME,
                StatsConstants.EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME);
        KeysType keysType = RunMode.isSharedDataMode() ? KeysType.UNIQUE_KEYS : KeysType.PRIMARY_KEYS;
        Map<String, String> properties = Maps.newHashMap();
        try {
            int defaultReplicationNum = AutoInferUtil.calDefaultReplicationNum();
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
            CreateTableStmt stmt = new CreateTableStmt(false, false,
                    tableName,
                    StatisticUtils.buildStatsColumnDef(StatsConstants.EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME),
                    EngineType.defaultEngine().name(),
                    new KeysDesc(keysType, EXTERNAL_HISTOGRAM_KEY_COLUMNS),
                    null,
                    new HashDistributionDesc(10, EXTERNAL_HISTOGRAM_KEY_COLUMNS),
                    properties,
                    null,
                    "");

            Analyzer.analyze(stmt, context);
            GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        } catch (UserException e) {
            LOG.warn("Failed to create external histogram statistics table", e);
            return false;
        }
        LOG.info("create external histogram statistics table done");
        for (Map.Entry<AnalyzeMgr.StatsMetaColumnKey, ExternalHistogramStatsMeta> entry :
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().getExternalHistogramStatsMetaMap().entrySet()) {
            ExternalHistogramStatsMeta histogramStatsMeta = entry.getValue();
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addExternalHistogramStatsMeta(
                    new ExternalHistogramStatsMeta(histogramStatsMeta.getCatalogName(), histogramStatsMeta.getDbName(),
                            histogramStatsMeta.getTableName(), histogramStatsMeta.getColumn(),
                            histogramStatsMeta.getType(), LocalDateTime.MIN, histogramStatsMeta.getProperties()));
        }
        return checkTableExist(StatsConstants.EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME);
    }

    private void refreshAnalyzeJob() {
        for (Map.Entry<Long, BasicStatsMeta> entry :
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().getBasicStatsMetaMap().entrySet()) {
            BasicStatsMeta basicStatsMeta = entry.getValue();
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addBasicStatsMeta(new BasicStatsMeta(
                    basicStatsMeta.getDbId(), basicStatsMeta.getTableId(), basicStatsMeta.getColumns(),
                    basicStatsMeta.getType(), LocalDateTime.MIN, basicStatsMeta.getProperties(),
                    basicStatsMeta.getUpdateRows()));
        }

        for (AnalyzeJob analyzeJob : GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAllAnalyzeJobList()) {
            analyzeJob.setWorkTime(LocalDateTime.MIN);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().updateAnalyzeJobWithLog(analyzeJob);
        }
    }

    private boolean dropTable(String tableName) {
        LOG.info("drop statistics table start");
        DropTableStmt stmt = new DropTableStmt(true,
                new TableName(StatsConstants.STATISTICS_DB_NAME, tableName), true);

        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(stmt);
        } catch (DdlException e) {
            LOG.warn("Failed to drop table", e);
            return false;
        }
        LOG.info("drop statistics table done");
        return !checkTableExist(tableName);
    }

    private void trySleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            LOG.warn(e.getMessage(), e);
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
        } else if (tableName.equals(StatsConstants.EXTERNAL_FULL_STATISTICS_TABLE_NAME)) {
            return createExternalFullStatisticsTable(context);
        } else if (tableName.equals(StatsConstants.EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME)) {
            return createExternalHistogramStatisticsTable(context);
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
        refreshStatisticsTable(StatsConstants.EXTERNAL_FULL_STATISTICS_TABLE_NAME);
        refreshStatisticsTable(StatsConstants.EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME);

        GlobalStateMgr.getCurrentState().getAnalyzeMgr().clearStatisticFromDroppedPartition();
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().clearStatisticFromDroppedTable();
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().clearExpiredAnalyzeStatus();

        RepoCreator.getInstance().run();
    }

    public void createStatisticsTablesForTest() {
        while (!checkDatabaseExist()) {
            if (createDatabase()) {
                break;
            }
            trySleep(1);
        }

        boolean existsSample = false;
        boolean existsFull = false;
        while (!existsSample || !existsFull) {
            existsSample = checkTableExist(StatsConstants.SAMPLE_STATISTICS_TABLE_NAME);
            existsFull = checkTableExist(StatsConstants.FULL_STATISTICS_TABLE_NAME);
            if (!existsSample) {
                createTable(StatsConstants.SAMPLE_STATISTICS_TABLE_NAME);
            }
            if (!existsFull) {
                createTable(StatsConstants.FULL_STATISTICS_TABLE_NAME);
            }
            trySleep(1);
        }
    }
}
