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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.load.loadv2.LoadsHistorySyncer;
import com.starrocks.load.rejected.RejectedRecordsTable;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.columns.ExternalPredicateColumnsStorage;
import com.starrocks.statistic.columns.PredicateColumnsStorage;
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

    private final boolean tableCorrected = false;
    private final Supplier<Integer> ttlSupplier;
    // Every column the table is expected to have, as (name, type definition) pairs.
    // Columns an existing table lacks (created by an older version) get added.
    private final List<Pair<String, String>> expectedColumns;
    // Set once we logged that a schema change holds back the column migration.
    private boolean schemaChangePendingLogged = false;

    public TableKeeper(String database,
                       String table,
                       String createTable,
                       Supplier<Integer> ttlSupplier) {
        this(database, table, createTable, ttlSupplier, Lists.newArrayList());
    }

    public TableKeeper(String database,
                       String table,
                       String createTable,
                       Supplier<Integer> ttlSupplier,
                       List<Pair<String, String>> expectedColumns) {
        this.databaseName = database;
        this.tableName = table;
        this.createTableSql = createTable;
        this.ttlSupplier = ttlSupplier;
        this.expectedColumns = expectedColumns;
    }

    public synchronized void run() {
        try {
            if (!checkDatabaseExists()) {
                LOG.warn("database not exists: {}", databaseName);
                return;
            }
            if (!checkTableExists()) {
                createTable();
                LOG.info("table created: {}", tableName);
            }
            if (checkTableExists()) {
                correctTable();
                addMissingColumns();
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
        return checkDatabaseExists() && checkTableExists();
    }

    public boolean checkDatabaseExists() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(databaseName) != null;
    }

    public boolean checkTableExists() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().mayGetTable(databaseName, tableName).isPresent();
    }

    public void createTable() {
        SimpleExecutor.getRepoExecutor().executeDDL(createTableSql);
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
                SimpleExecutor.getRepoExecutor().executeDDL(sql);
            }
            LOG.info("changed replication_number of table {} from {} to {}",
                    tableName, replica, expectedReplicationNum);
        }
    }

    /**
     * Append the columns a newer version added to the schema when the existing table
     * was created without them. Columns are added in declaration order so that a
     * positional INSERT ... SELECT * from the matching system view keeps lining up.
     */
    public void addMissingColumns() {
        if (expectedColumns == null || expectedColumns.isEmpty()) {
            return;
        }
        // The lock is released before the ALTER below, which takes the table lock itself.
        Optional<SchemaSnapshot> schema = readSchema();
        if (schema.isEmpty()) {
            return;
        }
        List<String> missing = schema.get().missingColumns;
        if (missing.isEmpty()) {
            schemaChangePendingLogged = false;
            return;
        }
        // ADD COLUMN is asynchronous; while a schema change is running the table is not NORMAL
        // and a second ALTER would be rejected, so wait for it instead of re-issuing every cycle.
        if (schema.get().state != OlapTable.OlapTableState.NORMAL) {
            if (!schemaChangePendingLogged) {
                LOG.info("table {}.{} is in state {}, waiting to add columns {}", databaseName, tableName,
                        schema.get().state, missing);
                schemaChangePendingLogged = true;
            }
            return;
        }
        String sql = alterTableAddColumns(databaseName, tableName, missing);
        try {
            SimpleExecutor.getRepoExecutor().executeDDL(sql);
            LOG.info("adding missing columns to table {}.{}: {}", databaseName, tableName, missing);
        } catch (Throwable e) {
            LOG.warn("add missing columns to table {}.{} failed", databaseName, tableName, e);
        }
    }

    /**
     * Whether the table exists, already has every expected column and is not being altered,
     * so that statements naming those columns explicitly can run against it. A table under
     * schema change already lists the columns it is adding, although they only exist in the
     * shadow index until the job finishes, hence the state check.
     */
    public boolean isSchemaReady() {
        if (expectedColumns == null || expectedColumns.isEmpty()) {
            return mayGetTable().isPresent();
        }
        Optional<SchemaSnapshot> schema = readSchema();
        return schema.isPresent() && schema.get().state == OlapTable.OlapTableState.NORMAL
                && schema.get().missingColumns.isEmpty();
    }

    // The state of the table and the expected columns it lacks, as of one moment.
    private static class SchemaSnapshot {
        private final OlapTable.OlapTableState state;
        private final List<String> missingColumns;

        private SchemaSnapshot(OlapTable.OlapTableState state, List<String> missingColumns) {
            this.state = state;
            this.missingColumns = missingColumns;
        }
    }

    /**
     * Reads the table state and its columns under the table read lock. A schema change job
     * flips both under the write lock, so reading them apart could pair the NORMAL state from
     * before the job with the columns it has just added to the shadow index.
     */
    private Optional<SchemaSnapshot> readSchema() {
        Optional<OlapTable> table = mayGetTable();
        if (table.isEmpty()) {
            return Optional.empty();
        }
        OlapTable olapTable = table.get();
        return Optional.of(withTableReadLock(olapTable, () -> new SchemaSnapshot(olapTable.getState(),
                missingColumnDefinitions(olapTable, expectedColumns))));
    }

    @VisibleForTesting
    protected <T> T withTableReadLock(OlapTable table, Supplier<T> reader) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(databaseName);
        if (db == null) {
            // dropped since the table was looked up: nothing left to be consistent with
            return reader.get();
        }
        try (AutoCloseableLock ignored = new AutoCloseableLock(db.getId(), table.getId(), LockType.READ)) {
            return reader.get();
        }
    }

    public static List<String> missingColumnDefinitions(OlapTable table, List<Pair<String, String>> expectedColumns) {
        List<String> missing = Lists.newArrayList();
        for (Pair<String, String> column : expectedColumns) {
            if (table.getColumn(column.first) == null) {
                missing.add("`" + column.first + "` " + column.second);
            }
        }
        return missing;
    }

    public static String alterTableAddColumns(String databaseName, String tableName, List<String> columnDefinitions) {
        return String.format("ALTER TABLE %s.%s ADD COLUMN (%s)", databaseName, tableName,
                String.join(", ", columnDefinitions));
    }

    public void changeTTL() {
        if (ttlSupplier == null) {
            return;
        }
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
            SimpleExecutor.getRepoExecutor().executeDDL(sql);
            LOG.info("change table {}.{} TTL from {} to {}",
                    databaseName, tableName, currentTTLNumber, expectedTTLDays);
        } catch (Throwable e) {
            LOG.warn("change table TTL failed", e);
        }
    }

    @VisibleForTesting
    protected Optional<OlapTable> mayGetTable() {
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

    public boolean isTableCorrected() {
        return tableCorrected;
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
            super("table-keeper", Config.table_keeper_interval_second * 1000L);

            keeperList.add(TaskRunHistoryTable.createKeeper());
            keeperList.add(LoadsHistorySyncer.createKeeper());
            keeperList.add(com.starrocks.lake.TabletWriteLogHistorySyncer.createKeeper());
            keeperList.add(PredicateColumnsStorage.createKeeper());
            keeperList.add(ExternalPredicateColumnsStorage.createKeeper());
            keeperList.add(RejectedRecordsTable.createKeeper());
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
