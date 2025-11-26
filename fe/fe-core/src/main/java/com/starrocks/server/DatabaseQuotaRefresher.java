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

package com.starrocks.server;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class DatabaseQuotaRefresher extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(DatabaseQuotaRefresher.class);

    public DatabaseQuotaRefresher() {
        super("DatabaseQuotaRefresher", Config.db_used_data_quota_update_interval_secs * 1000L);
    }

    @Override
    protected void runAfterCatalogReady() {
        checkAndMayUpdateInterval();
        updateAllDatabaseUsedDataQuota();
    }

    protected void checkAndMayUpdateInterval() {
        long currentInterval = getInterval();
        long newInterval = Config.db_used_data_quota_update_interval_secs * 1000L;
        if (currentInterval == newInterval) {
            return;
        }
        setInterval(newInterval);
        LOG.info("Update DatabaseQuotaRefresher interval from {} ms to {} ms", currentInterval, newInterval);
    }

    private void updateAllDatabaseUsedDataQuota() {
        try {
            GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
            List<Long> dbIdList = globalStateMgr.getLocalMetastore().getDbIds();
            for (Long dbId : dbIdList) {
                Database db = globalStateMgr.getLocalMetastore().getDb(dbId);
                if (db == null) {
                    LOG.warn("Database [{}] does not exist, skip to update database used data quota", dbId);
                    continue;
                }
                if (db.isSystemDatabase()) {
                    continue;
                }

                // Always update the used data quota, because the Metrics will rely on this value.
                long usedDataQuota = getUsedDataQuota(db);
                db.usedDataQuotaBytes.set(usedDataQuota);

                if (db.getReplicaQuota() < FeConstants.DEFAULT_DB_REPLICA_QUOTA_SIZE) {
                    long usedReplicaQuota = getUsedReplicaQuota(db);
                    db.usedReplicaQuotaBytes.set(usedReplicaQuota);
                }
            }
        } catch (Throwable e) {
            LOG.error("Failed to update database used data quota", e);
        }
    }

    private long getUsedDataQuota(Database database) {
        long usedDataQuota = 0;
        for (Table table : database.getTables()) {
            // Collect data size for native tables and materialized views for both shared-nothing and shared-data.
            if (!table.isNativeTableOrMaterializedView()) {
                continue;
            }

            OlapTable olapTable = (OlapTable) table;
            try (AutoCloseableLock ignore =
                    new AutoCloseableLock(database.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ)) {
                // TODO: maybe time consuming iterating the entire table to get the data size, consider caching the value in OlapTable
                usedDataQuota = usedDataQuota + olapTable.getDataSize();
            }
        }
        return usedDataQuota;
    }

    private long getUsedReplicaQuota(Database database) {
        long usedReplicaQuota = 0;
        for (Table table : database.getTables()) {
            if (!table.isNativeTableOrMaterializedView()) {
                continue;
            }

            OlapTable olapTable = (OlapTable) table;
            try (AutoCloseableLock ignore =
                    new AutoCloseableLock(database.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ)) {
                usedReplicaQuota = usedReplicaQuota + olapTable.getReplicaCount();
            }
        }

        return usedReplicaQuota;
    }
}
