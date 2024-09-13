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
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.UserException;
import com.starrocks.common.util.AutoInferUtil;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.load.pipe.filelist.RepoExecutor;
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
            "CREATE TABLE IF NOT EXISTS %s (" +
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
            "properties('replication_num' = '%d') ";

    private static final String CORRECT_LOADS_HISTORY_REPLICATION_NUM =
            "ALTER TABLE %s SET ('default.replication_num'='3')";

    private static final String LOADS_HISTORY_SYNC =
            "INSERT INTO %s " +
            "SELECT * FROM information_schema.loads " +
            "WHERE load_finish_time IS NOT NULL " +
            "AND load_finish_time < NOW() - INTERVAL 1 MINUTE " +
            "AND load_finish_time > ( " +
            "SELECT COALESCE(MAX(load_finish_time), '0001-01-01 00:00:00') " +
            "FROM %s);";
    
    private boolean databaseExists = false;
    private boolean tableExists = false;
    private boolean tableCorrected = false;

    public LoadsHistorySyncer() {
        super("Load history syncer", Config.loads_history_sync_interval_second * 1000L);
    }

    public boolean checkDatabaseExists() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(LOADS_HISTORY_DB_NAME) != null;
    }

    public static void createTable() throws UserException {
        String sql = SQLBuilder.buildCreateTableSql();
        RepoExecutor.getInstance().executeDDL(sql);
    }

    public static boolean correctTable() {
        int numBackends = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getTotalBackendNumber();
        int replica = GlobalStateMgr.getCurrentState().getStarRocksMeta()
                .mayGetTable(LOADS_HISTORY_DB_NAME, LOADS_HISTORY_TABLE_NAME)
                .map(tbl -> ((OlapTable) tbl).getPartitionInfo().getMinReplicationNum())
                .orElse((short) 1);
        if (numBackends < 3) {
            LOG.info("not enough backends in the cluster, expected 3 but got {}", numBackends);
            return false;
        }
        if (replica < 3) {
            String sql = SQLBuilder.buildAlterTableSql();
            RepoExecutor.getInstance().executeDDL(sql);
        } else {
            LOG.info("table {} already has {} replicas, no need to alter replication_num",
                    LOADS_HISTORY_TABLE_NAME, replica);
        }
        return true;
    }

    public void checkMeta() throws UserException {
        if (!databaseExists) {
            databaseExists = checkDatabaseExists();
            if (!databaseExists) {
                LOG.warn("database not exists: " + LOADS_HISTORY_DB_NAME);
                return;
            }
        }
        if (!tableExists) {
            createTable();
            LOG.info("table created: " + LOADS_HISTORY_TABLE_NAME);
            tableExists = true;
        }
        if (!tableCorrected && correctTable()) {
            LOG.info("table corrected: " + LOADS_HISTORY_TABLE_NAME);
            tableCorrected = true;
        }

        if (getInterval() != Config.loads_history_sync_interval_second * 1000L) {
            setInterval(Config.loads_history_sync_interval_second * 1000L);
        }
    }

    public void syncData() {
        try {
            RepoExecutor.getInstance().executeDML(SQLBuilder.buildSyncSql());
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
            checkMeta();
            syncData();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of LoadJobScheduler with error message {}", e.getMessage(), e);
        }
    }

    /**
     * Generate SQL for operations
     */
    static class SQLBuilder {

        public static String buildCreateTableSql() throws UserException {
            int replica = AutoInferUtil.calDefaultReplicationNum();
            return String.format(LOADS_HISTORY_TABLE_CREATE,
                    CatalogUtils.normalizeTableName(LOADS_HISTORY_DB_NAME, LOADS_HISTORY_TABLE_NAME), replica);
        }

        public static String buildAlterTableSql() {
            return String.format(CORRECT_LOADS_HISTORY_REPLICATION_NUM,
                    CatalogUtils.normalizeTableName(LOADS_HISTORY_DB_NAME, LOADS_HISTORY_TABLE_NAME));
        }

        public static String buildSyncSql() {
            return String.format(LOADS_HISTORY_SYNC,
                    CatalogUtils.normalizeTableName(LOADS_HISTORY_DB_NAME, LOADS_HISTORY_TABLE_NAME),
                    CatalogUtils.normalizeTableName(LOADS_HISTORY_DB_NAME, LOADS_HISTORY_TABLE_NAME));
        }
    }

}
