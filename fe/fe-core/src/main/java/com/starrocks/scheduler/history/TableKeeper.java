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

package com.starrocks.scheduler.history;

import com.google.common.collect.Lists;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.load.pipe.filelist.RepoExecutor;
import com.starrocks.server.GlobalStateMgr;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Keeper:
 * 1. Create the table for the history storage
 * 2. Cleanup stale history
 */
public class TableKeeper {

    private static final Logger LOG = LogManager.getLogger(TableKeeper.class);

    private final String databaseName;
    private final String tableName;
    private final String createTableSql;

    private boolean databaseExisted = false;
    private boolean tableExisted = false;
    private boolean tableCorrected = false;
    private Supplier<Integer> ttlSupplier;

    public TableKeeper(String database,
                       String table,
                       String createTable,
                       Supplier<Integer> ttlSupplier) {
        this.databaseName = database;
        this.tableName = table;
        this.createTableSql = createTable;
        this.ttlSupplier = ttlSupplier;
    }

    public synchronized void run() {
        try {
            if (!databaseExisted) {
                databaseExisted = checkDatabaseExists();
                if (!databaseExisted) {
                    LOG.warn("database not exists: {}", databaseName);
                    return;
                }
            }
            if (!tableExisted) {
                createTable();
                LOG.info("table created: {}", tableName);
                tableExisted = true;
            }
            correctTable();
            if (tableExisted) {
                changeTTL();
            }
        } catch (Exception e) {
            LOG.error("error happens in Keeper: {}", e.getMessage(), e);
        }
    }

    /**
     * Is the table ready for insert
     */
    public synchronized boolean isReady() {
        return databaseExisted && tableExisted;
    }

    public boolean checkDatabaseExists() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(databaseName) != null;
    }

    public void createTable() throws UserException {
        RepoExecutor.getInstance().executeDDL(createTableSql);
    }

    public void correctTable() {
        int expectedReplicationNum =
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getSystemTableExpectedReplicationNum();
        int replica = GlobalStateMgr.getCurrentState()
                .getLocalMetastore().mayGetTable(databaseName, tableName)
                .map(tbl -> ((OlapTable) tbl).getPartitionInfo().getMinReplicationNum())
                .orElse((short) 1);

        if (replica != expectedReplicationNum) {
            String sql = alterTableReplicas(expectedReplicationNum);
            if (StringUtils.isNotEmpty(sql)) {
                RepoExecutor.getInstance().executeDDL(sql);
            }
            LOG.info("changed replication_number of table {} from {} to {}",
                    tableName, replica, expectedReplicationNum);
        }
    }

    public void changeTTL() {
        Optional<OlapTable> table = mayGetTable();
        if (table.isEmpty()) {
            return;
        }
        OlapTable olapTable = table.get();
        int currentTTLNumber = olapTable.getTableProperty().getPartitionTTLNumber();
        int expectedTTLDays = ttlSupplier.get();
        if (currentTTLNumber == expectedTTLDays || expectedTTLDays == 0) {
            return;
        }
        String sql = alterTableTTL(expectedTTLDays);
        try {
            RepoExecutor.getInstance().executeDDL(sql);
            LOG.info("change table {}.{} TTL from {} to {}",
                    databaseName, tableName, currentTTLNumber, expectedTTLDays);
        } catch (Throwable e) {
            LOG.warn("change table TTL failed", e);
        }
    }

    private Optional<OlapTable> mayGetTable() {
        return GlobalStateMgr.getCurrentState()
                .getLocalMetastore().mayGetTable(databaseName, tableName)
                .flatMap(x -> Optional.of((OlapTable) x));
    }

    private String alterTableReplicas(int replicationNum) {
        Optional<OlapTable> table = mayGetTable();
        if (table.isEmpty()) {
            return "";
        }
        PartitionInfo partitionInfo = table.get().getPartitionInfo();
        if (partitionInfo.isRangePartition()) {
            String sql1 = String.format("ALTER TABLE %s.%s MODIFY PARTITION(*) SET ('replication_num'='%d');",
                    databaseName, tableName, replicationNum);
            String sql2 = String.format("ALTER TABLE %s.%s SET ('default.replication_num'='%d');",
                    databaseName, tableName, replicationNum);
            return sql1 + sql2;
        } else {
            return String.format("ALTER TABLE %s.%s SET ('replication_num'='%d')",
                    databaseName, tableName, replicationNum);
        }
    }

    private String alterTableTTL(int days) {
        return String.format("ALTER TABLE %s.%s SET ('partition_live_number' = '%d') ", databaseName, tableName, days);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getCreateTableSql() {
        return createTableSql;
    }

    public boolean isDatabaseExisted() {
        return databaseExisted;
    }

    public boolean isTableExisted() {
        return tableExisted;
    }

    public boolean isTableCorrected() {
        return tableCorrected;
    }

    public void setDatabaseExisted(boolean databaseExisted) {
        this.databaseExisted = databaseExisted;
    }

    public static TableKeeperDaemon startDaemon() {
        TableKeeperDaemon daemon = TableKeeperDaemon.getInstance();
        daemon.start();
        return daemon;
    }

    /**
     * The daemon thread running the TableKeeper
     */
    public static class TableKeeperDaemon extends FrontendDaemon {

        private static final TableKeeperDaemon INSTANCE = new TableKeeperDaemon();
        private final List<TableKeeper> keeperList = Lists.newArrayList();

        TableKeeperDaemon() {
            super("TableKeeper", Config.table_keeper_interval_second * 1000L);

            keeperList.add(TaskRunHistoryTable.createKeeper());
            // TODO: add FileListPipeRepo
            // TODO: add statistic table
        }

        public static TableKeeperDaemon getInstance() {
            return INSTANCE;
        }

        @Override
        protected void runAfterCatalogReady() {
            if (!GlobalStateMgr.getCurrentState().isLeader()) {
                return;
            }
            setInterval(Config.table_keeper_interval_second * 1000L);

            for (TableKeeper keeper : keeperList) {
                try {
                    keeper.run();
                } catch (Exception e) {
                    LOG.warn("error in TableKeeper: ", e);
                }
            }
        }
    }
}
