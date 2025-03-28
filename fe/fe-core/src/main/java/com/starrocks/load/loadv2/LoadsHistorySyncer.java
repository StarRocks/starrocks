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

import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 */
public class LoadsHistorySyncer extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(LoadsHistorySyncer.class);

    public static final String LOADS_HISTORY_DB_NAME = "_statistics_";
    public static final String LOADS_HISTORY_TABLE_NAME = "loads_history";

    private static final String LOADS_HISTORY_TABLE_CREATE =
            String.format("CREATE TABLE IF NOT EXISTS %s (" +
            "id bigint, " +
            "label varchar(2048), " +
            "profile_id varchar(2048), " +
            "db_name varchar(2048), " +
            "table_name varchar(2048), " +
            "user varchar(2048), " +
            "warehouse varchar(2048), " +
            "state varchar(2048), " +
            "progress varchar(2048), " +
            "type varchar(2048), " +
            "priority varchar(2048), " +
            "scan_rows bigint, " +
            "scan_bytes bigint, " +
            "filtered_rows bigint, " +
            "unselected_rows bigint, " +
            "sink_rows bigint, " +
            "runtime_details json, " +
            "create_time datetime, " +
            "load_start_time datetime, " +
            "load_commit_time datetime, " +
            "load_finish_time datetime not null, " +
            "properties json, " +
            "error_msg varchar(2048), " +
            "tracking_sql varchar(2048), " +
            "rejected_record_path varchar(2048), " +
            "job_id bigint " +
            ") " +
            "PARTITION BY date_trunc('DAY', load_finish_time) " +
            "DISTRIBUTED BY HASH(label) BUCKETS 3 " +
            "PROPERTIES( " +
            "'replication_num' = '1', " +
            "'partition_live_number' = '" + Config.loads_history_retained_days + "'" +
            ")",
            LOADS_HISTORY_TABLE_NAME);

    private static final String LOADS_HISTORY_SYNC =
            "INSERT INTO %s " +
            "SELECT * FROM information_schema.loads " +
            "WHERE load_finish_time IS NOT NULL " +
            "AND load_finish_time < NOW() - INTERVAL 1 MINUTE " +
            "AND load_finish_time > ( " +
            "SELECT COALESCE(MAX(load_finish_time), '0001-01-01 00:00:00') " +
            "FROM %s);";

    private boolean firstSync = true;

    private static final TableKeeper KEEPER =
            new TableKeeper(LOADS_HISTORY_DB_NAME, LOADS_HISTORY_TABLE_NAME, LOADS_HISTORY_TABLE_CREATE,
                    () -> Math.max(1, Config.loads_history_retained_days));

    private long syncedLoadFinishTime = -1L;

    public static TableKeeper createKeeper() {
        return KEEPER;
    }

    public LoadsHistorySyncer() {
        super("Load history syncer", Config.loads_history_sync_interval_second * 1000L);
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
    protected void runAfterCatalogReady() {
        if (FeConstants.runningUnitTest) {
            return;
        }
        try {
            // wait table keeper to create table
            if (firstSync) {
                firstSync = false;
                return;
            }

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

    /**
     * Generate SQL for operations
     */
    static class SQLBuilder {
        public static String buildSyncSql() {
            return String.format(LOADS_HISTORY_SYNC,
                    CatalogUtils.normalizeTableName(LOADS_HISTORY_DB_NAME, LOADS_HISTORY_TABLE_NAME),
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
