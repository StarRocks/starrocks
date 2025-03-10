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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.AutoInferUtil;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.load.pipe.filelist.RepoCreator;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.KeysDesc;
import com.starrocks.sql.common.EngineType;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
import static com.starrocks.statistic.StatsConstants.EXTERNAL_FULL_STATISTICS_TABLE_NAME;
import static com.starrocks.statistic.StatsConstants.EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME;
import static com.starrocks.statistic.StatsConstants.FULL_STATISTICS_TABLE_NAME;
import static com.starrocks.statistic.StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME;
import static com.starrocks.statistic.StatsConstants.MULTI_COLUMN_STATISTICS_TABLE_NAME;
import static com.starrocks.statistic.StatsConstants.SAMPLE_STATISTICS_TABLE_NAME;
import static com.starrocks.statistic.StatsConstants.SPM_BASELINE_TABLE_NAME;
import static com.starrocks.statistic.StatsConstants.STATISTICS_DB_NAME;

public class StatisticsMetaManager extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(StatisticsMetaManager.class);

    public StatisticsMetaManager() {
        super("statistics meta manager", 60L * 1000L);
    }

    private boolean checkDatabaseExist() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(STATISTICS_DB_NAME) != null;
    }

    private boolean createDatabase() {
        LOG.info("create statistics db start");
        CreateDbStmt dbStmt = new CreateDbStmt(false, STATISTICS_DB_NAME);
        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(dbStmt.getFullDbName());
        } catch (StarRocksException e) {
            LOG.warn("Failed to create database ", e);
            return false;
        }
        LOG.info("create statistics db down");
        return checkDatabaseExist();
    }

    private boolean checkTableExist(String tableName) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(STATISTICS_DB_NAME);
        Preconditions.checkState(db != null);
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName) != null;
    }

    // Add collection_size field to `column_statistics` table to collect array/map type columns
    public boolean checkTableCompatible(String tableName) {
        if (!tableName.equalsIgnoreCase(FULL_STATISTICS_TABLE_NAME)) {
            return true;
        }
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(STATISTICS_DB_NAME);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        for (String columnName : FULL_STATISTICS_COMPATIBLE_COLUMNS) {
            if (table.getColumn(columnName) == null) {
                return false;
            }
        }
        return true;
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

    private static final List<String> FULL_STATISTICS_COMPATIBLE_COLUMNS = ImmutableList.of(
            "collection_size"
    );

    private static final List<String> MULTI_COLUMN_STATISTICS_KEY_COLUMNS = ImmutableList.of(
            "table_id", "column_ids"
    );

    private boolean createSampleStatisticsTable(ConnectContext context) {
        LOG.info("create sample statistics table start");
        TableName tableName = new TableName(STATISTICS_DB_NAME, SAMPLE_STATISTICS_TABLE_NAME);
        Map<String, String> properties = Maps.newHashMap();
        try {
            int defaultReplicationNum = AutoInferUtil.calDefaultReplicationNum();
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
            KeysType keysType = KeysType.UNIQUE_KEYS;
            CreateTableStmt stmt = new CreateTableStmt(false, false,
                    tableName,
                    StatisticUtils.buildStatsColumnDef(SAMPLE_STATISTICS_TABLE_NAME),
                    EngineType.defaultEngine().name(),
                    new KeysDesc(keysType, KEY_COLUMN_NAMES),
                    null,
                    new HashDistributionDesc(10, KEY_COLUMN_NAMES),
                    properties,
                    null,
                    "");

            Analyzer.analyze(stmt, context);
            GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        } catch (StarRocksException e) {
            LOG.warn("Failed to create sample statistics, ", e);
            return false;
        }
        LOG.info("create sample statistics table done");
        refreshAnalyzeJob();
        return checkTableExist(SAMPLE_STATISTICS_TABLE_NAME);
    }

    private boolean createFullStatisticsTable(ConnectContext context) {
        LOG.info("create full statistics table start");
        TableName tableName = new TableName(STATISTICS_DB_NAME,
                FULL_STATISTICS_TABLE_NAME);
        KeysType keysType = RunMode.isSharedDataMode() ? KeysType.UNIQUE_KEYS : KeysType.PRIMARY_KEYS;
        Map<String, String> properties = Maps.newHashMap();

        try {
            int defaultReplicationNum = AutoInferUtil.calDefaultReplicationNum();
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
            CreateTableStmt stmt = new CreateTableStmt(false, false,
                    tableName,
                    StatisticUtils.buildStatsColumnDef(FULL_STATISTICS_TABLE_NAME),
                    EngineType.defaultEngine().name(),
                    new KeysDesc(keysType, FULL_STATISTICS_KEY_COLUMNS),
                    null,
                    new HashDistributionDesc(10, FULL_STATISTICS_KEY_COLUMNS),
                    properties,
                    null,
                    "");

            Analyzer.analyze(stmt, context);
            GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        } catch (StarRocksException e) {
            LOG.warn("Failed to create full statistics table", e);
            return false;
        }
        LOG.info("create full statistics table done");
        refreshAnalyzeJob();
        return checkTableExist(FULL_STATISTICS_TABLE_NAME);
    }

    private boolean createHistogramStatisticsTable(ConnectContext context) {
        LOG.info("create histogram statistics table start");
        TableName tableName = new TableName(STATISTICS_DB_NAME, HISTOGRAM_STATISTICS_TABLE_NAME);
        KeysType keysType = RunMode.isSharedDataMode() ? KeysType.UNIQUE_KEYS : KeysType.PRIMARY_KEYS;
        Map<String, String> properties = Maps.newHashMap();
        try {
            int defaultReplicationNum = AutoInferUtil.calDefaultReplicationNum();
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
            CreateTableStmt stmt = new CreateTableStmt(false, false,
                    tableName,
                    StatisticUtils.buildStatsColumnDef(HISTOGRAM_STATISTICS_TABLE_NAME),
                    EngineType.defaultEngine().name(),
                    new KeysDesc(keysType, HISTOGRAM_KEY_COLUMNS),
                    null,
                    new HashDistributionDesc(10, HISTOGRAM_KEY_COLUMNS),
                    properties,
                    null,
                    "");

            Analyzer.analyze(stmt, context);
            GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        } catch (StarRocksException e) {
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
        return checkTableExist(HISTOGRAM_STATISTICS_TABLE_NAME);
    }

    private boolean createExternalFullStatisticsTable(ConnectContext context) {
        LOG.info("create external full statistics table start");
        TableName tableName = new TableName(STATISTICS_DB_NAME, EXTERNAL_FULL_STATISTICS_TABLE_NAME);
        KeysType keysType = RunMode.isSharedDataMode() ? KeysType.UNIQUE_KEYS : KeysType.PRIMARY_KEYS;
        Map<String, String> properties = Maps.newHashMap();

        try {
            int defaultReplicationNum = AutoInferUtil.calDefaultReplicationNum();
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
            CreateTableStmt stmt = new CreateTableStmt(false, false,
                    tableName,
                    StatisticUtils.buildStatsColumnDef(EXTERNAL_FULL_STATISTICS_TABLE_NAME),
                    EngineType.defaultEngine().name(),
                    new KeysDesc(keysType, EXTERNAL_FULL_STATISTICS_KEY_COLUMNS),
                    null,
                    new HashDistributionDesc(10, EXTERNAL_FULL_STATISTICS_KEY_COLUMNS),
                    properties,
                    null,
                    "");

            Analyzer.analyze(stmt, context);
            GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        } catch (StarRocksException e) {
            LOG.warn("Failed to create full statistics table", e);
            return false;
        }
        LOG.info("create external full statistics table done");
        return checkTableExist(EXTERNAL_FULL_STATISTICS_TABLE_NAME);
    }

    private boolean createExternalHistogramStatisticsTable(ConnectContext context) {
        LOG.info("create external histogram statistics table start");
        TableName tableName = new TableName(STATISTICS_DB_NAME, EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME);
        KeysType keysType = RunMode.isSharedDataMode() ? KeysType.UNIQUE_KEYS : KeysType.PRIMARY_KEYS;
        Map<String, String> properties = Maps.newHashMap();
        try {
            int defaultReplicationNum = AutoInferUtil.calDefaultReplicationNum();
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
            CreateTableStmt stmt = new CreateTableStmt(false, false,
                    tableName,
                    StatisticUtils.buildStatsColumnDef(EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME),
                    EngineType.defaultEngine().name(),
                    new KeysDesc(keysType, EXTERNAL_HISTOGRAM_KEY_COLUMNS),
                    null,
                    new HashDistributionDesc(10, EXTERNAL_HISTOGRAM_KEY_COLUMNS),
                    properties,
                    null,
                    "");

            Analyzer.analyze(stmt, context);
            GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        } catch (StarRocksException e) {
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
        return checkTableExist(EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME);
    }

    private boolean createMultiColumnStatisticsTable(ConnectContext context) {
        LOG.info("create multi column statistics table start");
        TableName tableName = new TableName(STATISTICS_DB_NAME, MULTI_COLUMN_STATISTICS_TABLE_NAME);
        Map<String, String> properties = Maps.newHashMap();

        try {
            int defaultReplicationNum = AutoInferUtil.calDefaultReplicationNum();
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
            CreateTableStmt stmt = new CreateTableStmt(false, false,
                    tableName,
                    StatisticUtils.buildStatsColumnDef(MULTI_COLUMN_STATISTICS_TABLE_NAME),
                    EngineType.defaultEngine().name(),
                    new KeysDesc(KeysType.PRIMARY_KEYS, MULTI_COLUMN_STATISTICS_KEY_COLUMNS),
                    null,
                    new HashDistributionDesc(10, MULTI_COLUMN_STATISTICS_KEY_COLUMNS),
                    properties,
                    null,
                    "");

            Analyzer.analyze(stmt, context);
            GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        } catch (StarRocksException e) {
            LOG.warn("Failed to create multi column statistics table", e);
            return false;
        }
        LOG.info("create multi column statistics table done");
        return checkTableExist(MULTI_COLUMN_STATISTICS_TABLE_NAME);
    }

    private boolean createSPMBaselinesTable(ConnectContext context) {
        LOG.info("create spm_baselines table start");
        TableName tableName = new TableName(STATISTICS_DB_NAME, SPM_BASELINE_TABLE_NAME);
        KeysType keysType = KeysType.DUP_KEYS;
        Map<String, String> properties = Maps.newHashMap();
        try {
            List<ColumnDef> columns = ImmutableList.of(
                    new ColumnDef("id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("bind_sql", new TypeDef(ScalarType.createVarcharType(65530))),
                    new ColumnDef("bind_sql_digest", new TypeDef(ScalarType.createVarcharType(65530))),
                    new ColumnDef("bind_sql_hash", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("plan_sql", new TypeDef(ScalarType.createVarcharType(65530))),
                    new ColumnDef("costs", new TypeDef(ScalarType.createType(PrimitiveType.DOUBLE))),
                    new ColumnDef("update_time", new TypeDef(ScalarType.createType(PrimitiveType.DATETIME)))
            );

            int defaultReplicationNum = AutoInferUtil.calDefaultReplicationNum();
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
            CreateTableStmt stmt = new CreateTableStmt(false, false,
                    tableName, columns, EngineType.defaultEngine().name(),
                    new KeysDesc(keysType, List.of("id")), null,
                    new HashDistributionDesc(10, List.of("id")),
                    properties, null, "");

            Analyzer.analyze(stmt, context);
            GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        } catch (StarRocksException e) {
            LOG.warn("Failed to create spm_baselines table", e);
            return false;
        }
        LOG.info("create spm_baselines table done");
        return checkTableExist(SPM_BASELINE_TABLE_NAME);
    }

    private void refreshAnalyzeJob() {
        for (Map.Entry<Long, BasicStatsMeta> entry :
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().getBasicStatsMetaMap().entrySet()) {
            BasicStatsMeta basicStatsMeta = entry.getValue().clone();
            basicStatsMeta.setUpdateTime(LocalDateTime.MIN);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addBasicStatsMeta(basicStatsMeta);
        }

        for (AnalyzeJob analyzeJob : GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAllAnalyzeJobList()) {
            analyzeJob.setWorkTime(LocalDateTime.MIN);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().updateAnalyzeJobWithLog(analyzeJob);
        }
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
        try (ConnectContext.ScopeGuard guard = context.bindScope()) {
            if (tableName.equals(SAMPLE_STATISTICS_TABLE_NAME)) {
                return createSampleStatisticsTable(context);
            } else if (tableName.equals(FULL_STATISTICS_TABLE_NAME)) {
                return createFullStatisticsTable(context);
            } else if (tableName.equals(HISTOGRAM_STATISTICS_TABLE_NAME)) {
                return createHistogramStatisticsTable(context);
            } else if (tableName.equals(EXTERNAL_FULL_STATISTICS_TABLE_NAME)) {
                return createExternalFullStatisticsTable(context);
            } else if (tableName.equals(EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME)) {
                return createExternalHistogramStatisticsTable(context);
            } else if (tableName.equals(MULTI_COLUMN_STATISTICS_TABLE_NAME)) {
                return createMultiColumnStatisticsTable(context);
            } else if (SPM_BASELINE_TABLE_NAME.equals(tableName)) {
                return createSPMBaselinesTable(context);
            } else {
                throw new StarRocksPlannerException("Error table name " + tableName, ErrorType.INTERNAL_ERROR);
            }
        }
    }

    public boolean alterTable(String tableName) {
        ConnectContext context = StatisticUtils.buildConnectContext();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(STATISTICS_DB_NAME);
        Table table =  GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        try (ConnectContext.ScopeGuard guard = context.bindScope()) {
            if (tableName.equals(FULL_STATISTICS_TABLE_NAME)) {
                return alterFullStatisticsTable(context, table);
            } else {
                throw new StarRocksPlannerException("Error table name " + tableName, ErrorType.INTERNAL_ERROR);
            }
        }
    }

    public boolean alterFullStatisticsTable(ConnectContext context, Table table) {
        for (String columnName : FULL_STATISTICS_COMPATIBLE_COLUMNS) {
            if (table.getColumn(columnName) == null) {
                if (columnName.equalsIgnoreCase("collection_size")) {
                    ColumnDef.DefaultValueDef defaultValueDef = new ColumnDef.DefaultValueDef(true, new StringLiteral("-1"));
                    ColumnDef columnDef = new ColumnDef(columnName, new TypeDef(ScalarType.createType(PrimitiveType.BIGINT)),
                            false, null, null, true, defaultValueDef, "");
                    AddColumnClause addColumnClause = new AddColumnClause(columnDef, null, null, new HashMap<>());
                    AlterTableStmt alterTableStmt = new AlterTableStmt(
                            new TableName(DEFAULT_INTERNAL_CATALOG_NAME, STATISTICS_DB_NAME, table.getName()),
                            Lists.newArrayList(addColumnClause));

                    try {
                        Analyzer.analyze(alterTableStmt, context);
                        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(context, alterTableStmt);
                    } catch (Exception e) {
                        LOG.warn("Failed to add column {} on full statistics table", columnName, e);
                        return false;
                    }

                    while (table.getColumn(columnName) == null) {
                        // `alter table` may be sync in the shared-nothing cluster. So we need to check if job is done.
                        // TODO(stephen): This check is not robust because we can't get job handle here.
                        List<AlterJobV2> unfinishedAlterJobs = GlobalStateMgr.getCurrentState().getAlterJobMgr()
                                .getSchemaChangeHandler().getUnfinishedAlterJobV2ByTableId(table.getId());
                        if (unfinishedAlterJobs.isEmpty()) {
                            if (table.getColumn(columnName) != null) {
                                break;
                            } else {
                                LOG.warn("Failed to add column {} on full statistics table", columnName);
                                return false;
                            }
                        } else {
                            trySleep(5000);
                        }
                    }

                    LOG.info("Finished to add column {} to table {}", columnName, FULL_STATISTICS_TABLE_NAME);
                } else {
                    throw new StarRocksPlannerException("Error compatible column name " + columnName, ErrorType.INTERNAL_ERROR);
                }
            }
        }

        LOG.info("alter full statistics table done");
        return true;
    }

    private void refreshStatisticsTable(String tableName) {
        while (!checkTableExist(tableName)) {
            if (createTable(tableName)) {
                break;
            }
            LOG.warn("create statistics table " + tableName + " failed");
            trySleep(10000);
        }
        if (checkTableExist(tableName)) {
            StatisticUtils.alterSystemTableReplicationNumIfNecessary(tableName);
        }

        while (!checkTableCompatible(tableName)) {
            if (alterTable(tableName)) {
                break;
            }
            LOG.warn("alter statistics table " + tableName + " failed");
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

        refreshStatisticsTable(SAMPLE_STATISTICS_TABLE_NAME);
        refreshStatisticsTable(FULL_STATISTICS_TABLE_NAME);
        refreshStatisticsTable(HISTOGRAM_STATISTICS_TABLE_NAME);
        refreshStatisticsTable(EXTERNAL_FULL_STATISTICS_TABLE_NAME);
        refreshStatisticsTable(EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME);
        refreshStatisticsTable(MULTI_COLUMN_STATISTICS_TABLE_NAME);
        refreshStatisticsTable(SPM_BASELINE_TABLE_NAME);

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
            existsSample = checkTableExist(SAMPLE_STATISTICS_TABLE_NAME);
            existsFull = checkTableExist(FULL_STATISTICS_TABLE_NAME);
            if (!existsSample) {
                createTable(SAMPLE_STATISTICS_TABLE_NAME);
            }
            if (!existsFull) {
                createTable(FULL_STATISTICS_TABLE_NAME);
            }
            trySleep(1);
        }
    }

}
