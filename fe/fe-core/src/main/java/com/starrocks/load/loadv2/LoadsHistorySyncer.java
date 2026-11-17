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

package com.starrocks.load.loadv2;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.information.LoadsSystemTable;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Archives finished rows of information_schema.loads into _statistics_.loads_history.
 * The archive table's columns are derived from the view's schema, so both the table
 * definition and the INSERT ... SELECT name every column explicitly.
 */
public class LoadsHistorySyncer extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(LoadsHistorySyncer.class);

    public static final String LOADS_HISTORY_DB_NAME = "_statistics_";
    public static final String LOADS_HISTORY_TABLE_NAME = "loads_history";

    // Partition column; the only NOT NULL column of the archive.
    private static final String LOAD_FINISH_TIME = "load_finish_time";

    // (column name, column type DDL) for every column of the archive, in view order.
    // TableKeeper adds whichever of these an existing table lacks, and the sync only
    // runs once none is missing.
    //
    // rejected_record_path is kept although BE no longer populates it; rejected rows
    // live in `_statistics_.rejected_records`, joinable to this table via load label
    // or txn_id.
    static final List<Pair<String, String>> HISTORY_COLUMNS = LoadsSystemTable.columns().stream()
            .map(column -> Pair.create(column.getName().toLowerCase(), historyColumnType(column)))
            .collect(ImmutableList.toImmutableList());

    private static String historyColumnType(Column column) {
        Type type = column.getType();
        if (type.isBigint()) {
            return "bigint";
        }
        if (type.isVarchar()) {
            return "varchar(2048)";
        }
        if (type.isJsonType()) {
            return "json";
        }
        if (type.isDatetime()) {
            return "datetime";
        }
        throw new IllegalStateException("unsupported loads column type " + type + " for " + column.getName());
    }

    private static String columnDefinitions() {
        return HISTORY_COLUMNS.stream()
                .map(column -> "`" + column.first + "` " + column.second
                        + (LOAD_FINISH_TIME.equals(column.first) ? " not null" : ""))
                .collect(Collectors.joining(", "));
    }

    private static String columnList() {
        return HISTORY_COLUMNS.stream()
                .map(column -> "`" + column.first + "`")
                .collect(Collectors.joining(", "));
    }

    private static final String LOADS_HISTORY_TABLE_CREATE =
            String.format("CREATE TABLE IF NOT EXISTS %s (%s) " +
            "PARTITION BY date_trunc('DAY', load_finish_time) " +
            "DISTRIBUTED BY HASH(label) BUCKETS 3 " +
            "PROPERTIES( " +
            "'replication_num' = '1', " +
            "'partition_live_number' = '" + Config.loads_history_retained_days + "'" +
            ")",
            LOADS_HISTORY_TABLE_NAME, columnDefinitions());

    private static final String LOADS_HISTORY_SYNC =
            "INSERT INTO %s (%s) " +
            "SELECT %s FROM information_schema.loads " +
            "WHERE load_finish_time IS NOT NULL " +
            "AND load_finish_time < NOW() - INTERVAL 1 MINUTE " +
            "AND load_finish_time > ( " +
            "SELECT COALESCE(MAX(load_finish_time), '0001-01-01 00:00:00') " +
            "FROM %s);";

    private boolean firstSync = true;

    private static final TableKeeper KEEPER =
            new TableKeeper(LOADS_HISTORY_DB_NAME, LOADS_HISTORY_TABLE_NAME, LOADS_HISTORY_TABLE_CREATE,
                    () -> Math.max(1, Config.loads_history_retained_days), HISTORY_COLUMNS);

    private long syncedLoadFinishTime = -1L;
    private boolean schemaNotReadyLogged = false;

    public static TableKeeper createKeeper() {
        return KEEPER;
    }

    public LoadsHistorySyncer() {
        super("load-history-syncer", Config.loads_history_sync_interval_second * 1000L);
    }

    public void syncData() {
        if (getInterval() != Config.loads_history_sync_interval_second * 1000L) {
            setInterval(Config.loads_history_sync_interval_second * 1000L);
        }

        try {
            SimpleExecutor.getRepoExecutor().executeDML(SQLBuilder.buildSyncSql());
        } catch (Exception e) {
            LOG.error("Failed to sync loads history", e);
        }
    }

    @Override
    protected void runAfterLeaseValid() {
        if (FeConstants.runningUnitTest) {
            return;
        }
        try {
            // wait table keeper to create table
            if (firstSync) {
                firstSync = false;
                return;
            }
            // The keeper appends columns added by newer versions with an asynchronous
            // schema change. Wait until every column is there and the table is back to
            // NORMAL: before the ALTER the explicit column list would not resolve, and
            // while it runs the new columns exist only in the shadow index.
            if (!KEEPER.isSchemaReady()) {
                if (!schemaNotReadyLogged) {
                    LOG.info("loads history table is missing columns or being altered, "
                            + "waiting for the table keeper");
                    schemaNotReadyLogged = true;
                }
                return;
            }
            schemaNotReadyLogged = false;

            long latestFinishTime = getLatestFinishTime();
            if (syncedLoadFinishTime < latestFinishTime) {
                // refer to SQL:LOADS_HISTORY_SYNC. Only sync loads that completed more than 1 minute ago
                long oneMinAgo = System.currentTimeMillis() - 60000;
                syncData();
                // use (oneMinAgo - 10000) to cover the clock skew between FE and BE
                syncedLoadFinishTime = Math.min(latestFinishTime, oneMinAgo - 10000);
            }
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of LoadJobScheduler with error message {}", e.getMessage(), e);
        }
    }

    @Override
    protected void onStopped() {
        // Reset leader-session bookkeeping: firstSync re-arms the "wait one cycle for table
        // keeper" warm-up, and syncedLoadFinishTime resets so the next leader does not skip
        // syncing rows whose finish times pre-date its activation.
        firstSync = true;
        syncedLoadFinishTime = -1L;
        schemaNotReadyLogged = false;
    }

    /**
     * Generate SQL for operations
     */
    static class SQLBuilder {
        public static String buildSyncSql() {
            return String.format(LOADS_HISTORY_SYNC,
                    CatalogUtils.normalizeTableName(LOADS_HISTORY_DB_NAME, LOADS_HISTORY_TABLE_NAME),
                    columnList(),
                    columnList(),
                    CatalogUtils.normalizeTableName(LOADS_HISTORY_DB_NAME, LOADS_HISTORY_TABLE_NAME));
        }
    }

    private Pair<Long, Long> getTargetDbTableId() {
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(LOADS_HISTORY_DB_NAME);
        if (database == null) {
            return null;
        }
        Table table = database.getTable(LOADS_HISTORY_TABLE_NAME);
        if (table == null) {
            return null;
        }

        return Pair.create(database.getId(), table.getId());
    }

    private long getLatestFinishTime() {
        Pair<Long, Long> dbTableId = getTargetDbTableId();
        if (dbTableId == null) {
            LOG.warn("failed to get db: {}, table: {}", LOADS_HISTORY_DB_NAME, LOADS_HISTORY_TABLE_NAME);
            return -1L;
        }
        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        return Math.max(state.getLoadMgr().getLatestFinishTimeExcludeTable(dbTableId.first, dbTableId.second),
                state.getStreamLoadMgr().getLatestFinishTime());
    }
}
