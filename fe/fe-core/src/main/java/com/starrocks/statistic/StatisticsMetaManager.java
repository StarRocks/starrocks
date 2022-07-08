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
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.MasterDaemon;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.SystemInfoService;
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
        ScalarType dbNameType = ScalarType.createVarcharType(65530);
        ScalarType maxType = ScalarType.createVarcharType(65530);
        ScalarType minType = ScalarType.createVarcharType(65530);

        // varchar type column need call setAssignedStrLenInColDefinition here,
        // otherwise it will be set length to 1 at analyze
        columnNameType.setAssignedStrLenInColDefinition();
        tableNameType.setAssignedStrLenInColDefinition();
        dbNameType.setAssignedStrLenInColDefinition();
        maxType.setAssignedStrLenInColDefinition();
        minType.setAssignedStrLenInColDefinition();

        COLUMNS = ImmutableList.of(
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
    }

    private static final List<ColumnDef> COLUMNS;

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
        dbStmt.setClusterName(SystemInfoService.DEFAULT_CLUSTER);
        try {
            GlobalStateMgr.getCurrentState().createDb(dbStmt);
        } catch (DdlException e) {
            LOG.warn("Failed to create database " + e.getMessage());
            return false;
        }
        LOG.info("create statistics db down");
        return checkDatabaseExist();
    }

    private boolean checkTableExist() {
        Database db = GlobalStateMgr.getCurrentState().getDb(Constants.StatisticsDBName);
        Preconditions.checkState(db != null);
        return db.getTable(Constants.StatisticsTableName) != null;
    }

    private boolean checkReplicateNormal() {
        int aliveSize = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true).size();
        int total = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(false).size();
        // maybe cluster just shutdown, ignore
        if (aliveSize <= total / 2) {
            lossTableCount = 0;
            return true;
        }

        Database db = GlobalStateMgr.getCurrentState().getDb(Constants.StatisticsDBName);
        Preconditions.checkState(db != null);
        OlapTable table = (OlapTable) db.getTable(Constants.StatisticsTableName);
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

    private boolean createTable() {
        LOG.info("create statistics table start");
        TableName tableName = new TableName(Constants.StatisticsDBName,
                Constants.StatisticsTableName);
        Map<String, String> properties = Maps.newHashMap();
        int defaultReplicationNum = Math.min(3,
                GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true).size());
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
        CreateTableStmt stmt = new CreateTableStmt(false, false,
                tableName, COLUMNS, "olap",
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
        return checkTableExist();
    }

    private boolean dropTable() {
        LOG.info("drop statistics table start");
        TableName tableName = new TableName(Constants.StatisticsDBName, Constants.StatisticsTableName);
        DropTableStmt stmt = new DropTableStmt(true, tableName, true);

        try {
            GlobalStateMgr.getCurrentState().dropTable(stmt);
        } catch (DdlException e) {
            LOG.warn("Failed to drop table" + e.getMessage());
            return false;
        }
        LOG.info("drop statistics table done");
        return !checkTableExist();
    }

    private void trySleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            LOG.warn(e.getMessage());
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

        while (checkTableExist() && !checkReplicateNormal()) {
            if (dropTable()) {
                break;
            }
            trySleep(10000);
        }

        while (!checkTableExist()) {
            if (createTable()) {
                break;
            }
            trySleep(10000);
        }
    }
}
