// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.statistic;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.analysis.HashDistributionDesc;
import com.starrocks.analysis.KeysDesc;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.MasterDaemon;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

import static com.starrocks.common.Config.statistics_manager_sleep_time_sec;

public class StatisticsMetaManager extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(StatisticsMetaManager.class);

    static {
        ScalarType columnNameType = ScalarType.createVarcharType(65530);
        ScalarType tableNameType = ScalarType.createVarcharType(65530);
        ScalarType partitionNameType = ScalarType.createVarcharType(65530);
        ScalarType dbNameType = ScalarType.createVarcharType(65530);
        ScalarType maxType = ScalarType.createVarcharType(65530);
        ScalarType minType = ScalarType.createVarcharType(65530);
        ScalarType histogramType = ScalarType.createVarcharType(65530);

        // varchar type column need call setAssignedStrLenInColDefinition here,
        // otherwise it will be set length to 1 at analyze
        columnNameType.setAssignedStrLenInColDefinition();
        tableNameType.setAssignedStrLenInColDefinition();
        partitionNameType.setAssignedStrLenInColDefinition();
        dbNameType.setAssignedStrLenInColDefinition();
        maxType.setAssignedStrLenInColDefinition();
        minType.setAssignedStrLenInColDefinition();
        histogramType.setAssignedStrLenInColDefinition();

        SAMPLE_STATISTICS_COLUMNS = ImmutableList.of(
                new ColumnDef("table_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                new ColumnDef("column_name", new TypeDef(columnNameType)),
                new ColumnDef("db_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                new ColumnDef("table_name", new TypeDef(tableNameType)),
                new ColumnDef("db_name", new TypeDef(dbNameType)),
                new ColumnDef("row_count", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                new ColumnDef("data_size", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                new ColumnDef("distinct_count", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                new ColumnDef("null_count", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                new ColumnDef("max", new TypeDef(maxType)),
                new ColumnDef("min", new TypeDef(minType)),
                new ColumnDef("update_time", new TypeDef(ScalarType.createType(PrimitiveType.DATETIME)))
        );

        FULL_STATISTICS_COLUMNS = ImmutableList.of(
                new ColumnDef("table_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                new ColumnDef("partition_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                new ColumnDef("column_name", new TypeDef(columnNameType)),
                new ColumnDef("db_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                new ColumnDef("table_name", new TypeDef(tableNameType)),
                new ColumnDef("partition_name", new TypeDef(partitionNameType)),
                new ColumnDef("row_count", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                new ColumnDef("data_size", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                new ColumnDef("ndv", new TypeDef(ScalarType.createType(PrimitiveType.HLL))),
                new ColumnDef("null_count", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                new ColumnDef("max", new TypeDef(maxType)),
                new ColumnDef("min", new TypeDef(minType)),
                new ColumnDef("update_time", new TypeDef(ScalarType.createType(PrimitiveType.DATETIME)))
        );

        HISTOGRAM_STATISTICS_COLUMNS = ImmutableList.of(
                new ColumnDef("table_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                new ColumnDef("column_name", new TypeDef(columnNameType)),
                new ColumnDef("table_name", new TypeDef(tableNameType)),
                new ColumnDef("histogram", new TypeDef(histogramType))
        );
    }

    private static final List<ColumnDef> SAMPLE_STATISTICS_COLUMNS;

    private static final List<ColumnDef> FULL_STATISTICS_COLUMNS;

    private static final List<ColumnDef> HISTOGRAM_STATISTICS_COLUMNS;

    // If all replicas are lost more than 3 times in a row, rebuild the statistics table
    private int lossTableCount = 0;

    public StatisticsMetaManager() {
        super("statistics meta manager", 60 * 1000);
    }

    private boolean checkDatabaseExist() {
        return GlobalStateMgr.getCurrentState().getDb(Constants.StatisticsDBName) != null;
    }

    private boolean createDatabase() {
        LOG.info("create statistics db start");
        CreateDbStmt dbStmt = new CreateDbStmt(false, Constants.StatisticsDBName);
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
        Database db = GlobalStateMgr.getCurrentState().getDb(Constants.StatisticsDBName);
        Preconditions.checkState(db != null);
        return db.getTable(tableName) != null;
    }

    private boolean checkReplicateNormal(String tableName) {
        int aliveSize = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true).size();
        int total = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(false).size();
        // maybe cluster just shutdown, ignore
        if (aliveSize < total / 2) {
            lossTableCount = 0;
            return true;
        }

        Database db = GlobalStateMgr.getCurrentState().getDb(Constants.StatisticsDBName);
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

    private boolean createSampleStatisticsTable() {
        LOG.info("create statistics table start");
        TableName tableName = new TableName(Constants.StatisticsDBName,
                Constants.SampleStatisticsTableName);
        Map<String, String> properties = Maps.newHashMap();
        int defaultReplicationNum = Math.min(3,
                GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true).size());
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
        CreateTableStmt stmt = new CreateTableStmt(false, false,
                tableName, SAMPLE_STATISTICS_COLUMNS, "olap",
                new KeysDesc(KeysType.UNIQUE_KEYS, keyColumnNames),
                null,
                new HashDistributionDesc(10, keyColumnNames),
                properties,
                null,
                "");
        Analyzer analyzer = new Analyzer(GlobalStateMgr.getCurrentState(),
                StatisticUtils.buildConnectContext());
        try {
            stmt.analyze(analyzer);
        } catch (UserException e) {
            LOG.warn("Failed to create table when analyze " + e.getMessage());
            return false;
        }
        try {
            GlobalStateMgr.getCurrentState().createTable(stmt);
        } catch (DdlException e) {
            LOG.warn("Failed to create table" + e.getMessage());
            return false;
        }
        LOG.info("create statistics table done");
        return checkTableExist(Constants.SampleStatisticsTableName);
    }

    private boolean createFullStatisticsTable() {
        LOG.info("create statistics table v2 start");
        TableName tableName = new TableName(Constants.StatisticsDBName,
                Constants.FullStatisticsTableName);
        Map<String, String> properties = Maps.newHashMap();
        int defaultReplicationNum = Math.min(3,
                GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true).size());
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
        CreateTableStmt stmt = new CreateTableStmt(false, false,
                tableName, FULL_STATISTICS_COLUMNS, "olap",
                new KeysDesc(KeysType.PRIMARY_KEYS, fullStatisticsKeyColumns),
                null,
                new HashDistributionDesc(10, fullStatisticsKeyColumns),
                properties,
                null,
                "");
        Analyzer analyzer = new Analyzer(GlobalStateMgr.getCurrentState(),
                StatisticUtils.buildConnectContext());
        try {
            stmt.analyze(analyzer);
        } catch (UserException e) {
            LOG.warn("Failed to create table when analyze " + e.getMessage());
            return false;
        }
        try {
            GlobalStateMgr.getCurrentState().createTable(stmt);
        } catch (DdlException e) {
            LOG.warn("Failed to create table" + e.getMessage());
            return false;
        }
        LOG.info("create statistics table done");
        return checkTableExist(Constants.FullStatisticsTableName);
    }

    private boolean createHistogramStatisticsTable() {
        LOG.info("create statistics table v2 start");
        TableName tableName = new TableName(Constants.StatisticsDBName,
                Constants.HistogramStatisticsTableName);
        Map<String, String> properties = Maps.newHashMap();
        int defaultReplicationNum = Math.min(3,
                GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true).size());
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
        CreateTableStmt stmt = new CreateTableStmt(false, false,
                tableName, HISTOGRAM_STATISTICS_COLUMNS, "olap",
                new KeysDesc(KeysType.PRIMARY_KEYS, histogramKeyColumns),
                null,
                new HashDistributionDesc(10, histogramKeyColumns),
                properties,
                null,
                "");
        Analyzer analyzer = new Analyzer(GlobalStateMgr.getCurrentState(),
                StatisticUtils.buildConnectContext());
        try {
            stmt.analyze(analyzer);
        } catch (UserException e) {
            LOG.warn("Failed to create table when analyze " + e.getMessage());
            return false;
        }
        try {
            GlobalStateMgr.getCurrentState().createTable(stmt);
        } catch (DdlException e) {
            LOG.warn("Failed to create table" + e.getMessage());
            return false;
        }
        LOG.info("create statistics table done");
        return checkTableExist(Constants.HistogramStatisticsTableName);
    }

    private boolean dropTable(String tableName) {
        LOG.info("drop statistics table start");
        DropTableStmt stmt = new DropTableStmt(true,
                new TableName(Constants.StatisticsDBName, tableName), true);

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
        if (tableName.equals(Constants.SampleStatisticsTableName)) {
            return createSampleStatisticsTable();
        } else if (tableName.equals(Constants.FullStatisticsTableName)) {
            return createFullStatisticsTable();
        } else if (tableName.equals(Constants.HistogramStatisticsTableName)) {
            return createHistogramStatisticsTable();
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
        trySleep(statistics_manager_sleep_time_sec * 1000);
        while (!checkDatabaseExist()) {
            if (createDatabase()) {
                break;
            }
            trySleep(10000);
        }

        refreshStatisticsTable(Constants.SampleStatisticsTableName);
        if (Config.enable_collect_full_statistics) {
            refreshStatisticsTable(Constants.FullStatisticsTableName);
            refreshStatisticsTable(Constants.HistogramStatisticsTableName);
        }
    }
}
