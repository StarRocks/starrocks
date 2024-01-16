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

package com.starrocks.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.alter.AlterJobMgr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatisticUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Optional;

/**
 * A daemon thread that check the MV active status, try to activate the MV it's inactive.
 */
public class MVActiveChecker extends LeaderDaemon {

    private static final Logger LOG = LogManager.getLogger(MVActiveChecker.class);

    public MVActiveChecker() {
        super("MVActiveChecker", Config.mv_active_checker_interval_seconds * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        // reset if the interval has been changed
        setInterval(Config.mv_active_checker_interval_seconds * 1000L);

        if (!Config.enable_mv_automatic_active_check) {
            return;
        }

        try {
            process();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of MVActiveChecker", e);
        }
    }

    @VisibleForTesting
    public void runForTest() {
        process();
    }

    private void process() {
        Collection<Database> dbs = GlobalStateMgr.getCurrentState().getIdToDb().values();
        for (Database db : CollectionUtils.emptyIfNull(dbs)) {
            for (Table table : CollectionUtils.emptyIfNull(db.getTables())) {
                if (table.isMaterializedView()) {
                    MaterializedView mv = (MaterializedView) table;
                    if (!mv.isActive()) {
                        tryToActivate(mv);
                    }
                }
            }
        }
    }

    public static void tryToActivate(MaterializedView mv) {
        // if the mv is set to inactive manually, we don't activate it
        String reason = mv.getInactiveReason();
        if (mv.isActive() || AlterJobMgr.MANUAL_INACTIVE_MV_REASON.equalsIgnoreCase(reason)) {
            return;
        }

        long dbId = mv.getDbId();
        Optional<String> dbName = GlobalStateMgr.getCurrentState().mayGetDb(dbId).map(Database::getFullName);
        if (!dbName.isPresent()) {
            LOG.warn("[MVActiveChecker] cannot activate MV {} since database {} not found", mv.getName(), dbId);
            return;
        }

        String mvFullName = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, dbName.get(),
                mv.getName()).toString();
        String sql = String.format("ALTER MATERIALIZED VIEW %s active", mvFullName);
        try {
            ConnectContext connect = StatisticUtils.buildConnectContext();
            connect.setDatabase(dbName.get());
            connect.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

            connect.executeSql(sql);
            if (mv.isActive()) {
                LOG.info("[MVActiveChecker] activate MV {} successfully", mvFullName);
            } else {
                LOG.warn("[MVActiveChecker] activate MV {} failed", mvFullName);
            }
        } catch (Exception e) {
            LOG.warn("[MVActiveChecker] activate MV {} failed", mvFullName, e);
            throw new RuntimeException(e);
        } finally {
            ConnectContext.remove();
        }
    }
}
