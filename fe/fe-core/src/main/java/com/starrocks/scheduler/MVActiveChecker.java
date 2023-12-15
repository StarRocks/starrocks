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
import com.google.common.collect.Maps;
import com.starrocks.alter.AlterJobMgr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatisticUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * A daemon thread that check the MV active status, try to activate the MV it's inactive.
 */
public class MVActiveChecker extends FrontendDaemon {

    private static final Logger LOG = LogManager.getLogger(MVActiveChecker.class);

    private static final Map<MvId, MvActiveInfo> MV_ACTIVE_INFO = Maps.newConcurrentMap();

    public MVActiveChecker() {
        super("MVActiveChecker", Config.mv_active_checker_interval_seconds * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        // reset if the interval has been changed
        setInterval(Config.mv_active_checker_interval_seconds * 1000L);

        if (!Config.enable_mv_automatic_active_check || FeConstants.runningUnitTest) {
            return;
        }

        try {
            process();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of MVActiveChecker", e);
        }
    }

    @VisibleForTesting
    public void runForTest(boolean clearGrace) {
        if (clearGrace) {
            clearGracePeriod();
        }
        process();
    }

    @VisibleForTesting
    private void clearGracePeriod() {
        MV_ACTIVE_INFO.clear();
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

        MvActiveInfo activeInfo = MV_ACTIVE_INFO.get(mv.getMvId());
        if (activeInfo != null && activeInfo.isInGracePeriod()) {
            return;
        }

        boolean activeOk = false;
        String mvFullName = new TableName(dbName.get(), mv.getName()).toString();
        String sql = String.format("ALTER MATERIALIZED VIEW %s active", mvFullName);
        LOG.info("[MVActiveChecker] Start to activate MV {} because of its inactive reason: {}", mvFullName, reason);
        try {
            ConnectContext connect = StatisticUtils.buildConnectContext();
            connect.setStatisticsContext(false);
            connect.setDatabase(dbName.get());

            connect.executeSql(sql);
            if (mv.isActive()) {
                activeOk = true;
                LOG.info("[MVActiveChecker] activate MV {} successfully", mvFullName);
            } else {
                LOG.warn("[MVActiveChecker] activate MV {} failed", mvFullName);
            }
        } catch (Exception e) {
            LOG.warn("[MVActiveChecker] activate MV {} failed", mvFullName, e);
        } finally {
            ConnectContext.remove();
        }

        if (activeOk) {
            MV_ACTIVE_INFO.remove(mv.getMvId());
        } else {
            if (activeInfo != null) {
                activeInfo.next();
            } else {
                MV_ACTIVE_INFO.put(mv.getMvId(), MvActiveInfo.firstFailure());
            }
        }
    }

    public static class MvActiveInfo {
        // Use 2 ** N as failure backoff, and set the max to 30 minutes
        public static final long MAX_BACKOFF_MINUTES = 60;
        private static final long BACKOFF_BASE = 2;
        private static final long MAX_BACKOFF_TIMES = (long) (Math.log(MAX_BACKOFF_MINUTES) / Math.log(BACKOFF_BASE));

        private LocalDateTime nextActive;
        private int failureTimes = 0;

        public static MvActiveInfo firstFailure() {
            MvActiveInfo info = new MvActiveInfo();
            info.next();
            return info;
        }

        /**
         * If in grace period, it should not activate the mv
         */
        public boolean isInGracePeriod() {
            LocalDateTime now = LocalDateTime.now(TimeUtils.getSystemTimeZone().toZoneId());
            return now.isBefore(nextActive);
        }

        public LocalDateTime getNextActive() {
            return nextActive;
        }

        public void next() {
            LocalDateTime lastActive = LocalDateTime.now(TimeUtils.getSystemTimeZone().toZoneId());
            this.failureTimes++;
            this.nextActive = lastActive.plus(failureBackoff(failureTimes));
        }

        private Duration failureBackoff(int failureTimes) {
            if (failureTimes >= MAX_BACKOFF_TIMES) {
                return Duration.ofMinutes(MAX_BACKOFF_MINUTES);
            }
            long expBackoff = (long) Math.pow(BACKOFF_BASE, failureTimes);
            return Duration.ofMinutes(expBackoff);
        }
    }

}
