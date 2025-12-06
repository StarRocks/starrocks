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

package com.starrocks.lake;

import com.starrocks.catalog.CatalogUtils;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.scheduler.history.TableKeeper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TabletWriteLogHistorySyncer extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(TabletWriteLogHistorySyncer.class);

    public static final String DB_NAME = "_statistics_";
    public static final String TABLE_NAME = "tablet_write_log_history";

    // Default retention days: 7
    private static final int RETAINED_DAYS = 7;

    private static final String TABLE_CREATE =
            String.format("CREATE TABLE IF NOT EXISTS %s (" +
                            "be_id bigint, " +
                            "begin_time datetime, " +
                            "finish_time datetime NOT NULL, " +
                            "txn_id bigint, " +
                            "tablet_id bigint, " +
                            "table_id bigint, " +
                            "partition_id bigint, " +
                            "log_type varchar(64), " +
                            "input_rows bigint, " +
                            "input_bytes bigint, " +
                            "output_rows bigint, " +
                            "output_bytes bigint, " +
                            "input_segments int, " +
                            "output_segments int, " +
                            "label varchar(1024), " +
                            "compaction_score bigint, " +
                            "compaction_type varchar(64)" +
                            ") " +
                            "PARTITION BY date_trunc('DAY', finish_time) " +
                            "DISTRIBUTED BY HASH(tablet_id) BUCKETS 3 " +
                            "PROPERTIES( " +
                            "'partition_live_number' = '" + RETAINED_DAYS + "'" +
                            ")",
                    TABLE_NAME);

    private static final String SYNC_SQL =
            "INSERT INTO %s " +
            "SELECT " +
            "be_id, begin_time, finish_time, txn_id, tablet_id, table_id, partition_id, log_type, " +
            "input_rows, input_bytes, output_rows, output_bytes, input_segments, output_segments, " +
            "label, compaction_score, compaction_type " +
            "FROM information_schema.be_tablet_write_log " +
            "WHERE finish_time > (SELECT COALESCE(MAX(finish_time), '0001-01-01 00:00:00') FROM %s) " +
            "AND finish_time < NOW() - INTERVAL 1 MINUTE";

    private boolean firstSync = true;

    private static final TableKeeper KEEPER =
            new TableKeeper(DB_NAME, TABLE_NAME, TABLE_CREATE, () -> RETAINED_DAYS);

    public static TableKeeper createKeeper() {
        return KEEPER;
    }

    public TabletWriteLogHistorySyncer() {
        super("TabletWriteLogHistorySyncer", 60 * 1000L); // 60s interval
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
            syncData();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of TabletWriteLogHistorySyncer with error message {}", e.getMessage(), e);
        }
    }

    public void syncData() {
        // TODO: Can add a switch to control whether to enable synchronization
        try {
            SimpleExecutor.getRepoExecutor().executeDML(SQLBuilder.buildSyncSql());
        } catch (Exception e) {
            LOG.error("Failed to sync tablet write log history", e);
        }
    }

    static class SQLBuilder {
        public static String buildSyncSql() {
            String tableName = CatalogUtils.normalizeTableName(DB_NAME, TABLE_NAME);
            return String.format(SYNC_SQL, tableName, tableName);
        }
    }
}
