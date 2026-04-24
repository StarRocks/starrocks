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

package com.starrocks.mv.refresh.pct;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.concurrent.lock.LockParams;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.scheduler.mv.BaseTableSnapshotInfo;
import com.starrocks.scheduler.mv.MVRefreshProcessor;
import com.starrocks.scheduler.mv.pct.PCTRefreshScope;
import com.starrocks.scheduler.mv.pct.PCTTableSnapshotInfo;
import com.starrocks.sql.analyzer.MaterializedViewAnalyzer;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Shared PCT/IVM refresh preparation flow extracted from {@link MVRefreshProcessor}.
 */
public final class MVPCTRefreshSynchronizer {
    private final MVRefreshProcessor processor;

    public MVPCTRefreshSynchronizer(MVRefreshProcessor processor) {
        this.processor = processor;
    }

    /**
     * Sync and check the partitions of the materialized view and its base tables.
     */
    public void syncAndCheckPCTPartitions() throws Exception {
        Map<BaseTableSnapshotInfo, PCellSortedSet> baseTableCandidatePartitions = Maps.newHashMap();
        if (processor.isExternalTablePreciseRefreshEnabled()) {
            try (Timer ignored = Tracers.watchScope("MVRefreshComputeCandidatePartitions")) {
                if (!syncPartitions()) {
                    throw new DmlException(String.format("materialized view %s.%s refresh task failed: "
                                    + "pre-sync partition failed during external table candidate partition computation",
                            processor.getDb().getFullName(), processor.getMv().getName()));
                }
                PCellSortedSet mvCandidatePartition = getPCTMVToRefreshedPartitions(true, false);
                PCTRefreshScope candidateScope = processor.buildPCTRefreshScope(mvCandidatePartition);
                baseTableCandidatePartitions = candidateScope.getRefTableRefreshPartitions();
            } catch (Exception e) {
                processor.getLogger().warn("failed to compute candidate partitions in sync partitions",
                        DebugUtil.getRootStackTrace(e));
                if (e.getMessage() == null || !e.getMessage().contains("not exist")) {
                    throw e;
                }
            }
        }

        try (Timer ignored = Tracers.watchScope("MVRefreshSyncAndCheckPartitions")) {
            if (!syncAndCheckPCTPartitions(baseTableCandidatePartitions)) {
                throw new DmlException(String.format("materialized view %s.%s refresh task failed: "
                                + "sync and check partition failed, base table partition may have changed "
                                + "too frequently or exceeded max retry times(%d)",
                        processor.getDb().getFullName(), processor.getMv().getName(),
                        Config.max_mv_check_base_table_change_retry_times));
            }
        }
    }

    /**
     * Compute PCT partition metadata for the current refresh.
     */
    public void updatePCTToRefreshMetas(boolean skipBatchFilter) throws Exception {
        PCellSortedSet mvPartitionsToRefresh = getPCTMVToRefreshedPartitions(false, skipBatchFilter);
        PCTRefreshScope refreshScope = processor.buildPCTRefreshScope(mvPartitionsToRefresh);
        processor.applyPCTRefreshScope(refreshScope);
        processor.updatePCTBaseTableSnapshotInfos(refreshScope.getRefTableRefreshPartitions());
        processor.updatePCTMVToRefreshInfoIntoTaskRun(refreshScope.getMvPartitionsToRefresh(),
                refreshScope.getRefTablePartitionNames());
        processor.getLogger().info("mvToRefreshedPartitions:{}, refTableRefreshPartitions:{}",
                refreshScope.getMvPartitionsToRefresh(), refreshScope.getRefTableRefreshPartitions());
    }

    public PCellSortedSet getPCTMVToRefreshedPartitions(boolean tentative,
                                                        boolean skipBatchFilter)
            throws AnalysisException, LockTimeoutException {
        processor.getMvRefreshParams().setIsTentative(tentative);
        final PCellSortedSet mvToRefreshedPartitions = processor.getMvPctRefreshPlanner()
                .getMVToRefreshedPartitions(processor.getSnapshotBaseTables(), skipBatchFilter);
        if (!tentative) {
            processor.updateCurrentRefreshParamsIntoTaskRun();
        }
        return mvToRefreshedPartitions;
    }

    public Map<BaseTableSnapshotInfo, PCellSortedSet> getPCTRefTableRefreshPartitions(PCellSortedSet mvToRefreshedPartitions) {
        return processor.buildPCTRefreshScope(mvToRefreshedPartitions).getRefTableRefreshPartitions();
    }

    boolean syncPartitions() throws AnalysisException, LockTimeoutException {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        processor.setSnapshotBaseTables(processor.collectBaseTableSnapshotInfos());

        if (!processor.getMvContext().isExplain() && processor.getMvRefreshParams().isNonTentativeForce()) {
            PCellSortedSet toRefreshPartitions = processor.getMvPctRefreshPartitioner().getMVPartitionsToRefreshByParams();
            if (toRefreshPartitions != null && !toRefreshPartitions.isEmpty()) {
                processor.getLogger().info("force refresh, drop partitions: [{}]",
                        Joiner.on(",").join(toRefreshPartitions.getPartitionNames()));

                Database db = processor.getDb();
                MaterializedView mv = processor.getMv();
                Locker locker = new Locker();
                if (!locker.tryLockTableWithIntensiveDbLock(db.getId(), mv.getId(), LockType.WRITE,
                        Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
                    processor.getLogger().warn("failed to lock database: {} in syncPartitions for force refresh",
                            db.getFullName());
                    throw new DmlException("Force refresh of materialized view %s.%s failed: "
                                    + "failed to acquire write lock on database %s within %d ms",
                            db.getFullName(), mv.getName(), db.getFullName(),
                            Config.mv_refresh_try_lock_timeout_ms);
                }
                try {
                    if (!mv.isPartitionedTable() || processor.getMvRefreshParams().isCompleteRefresh()) {
                        mv.getRefreshScheme().getAsyncRefreshContext().clearVisibleVersionMap();
                    } else {
                        mv.getRefreshScheme().getAsyncRefreshContext().clearVisibleVersionMapByMVPartitions(
                                toRefreshPartitions.getPartitionNames());
                    }
                } catch (Exception e) {
                    processor.getLogger().warn("failed to clear version map {} for force refresh",
                            Joiner.on(",").join(toRefreshPartitions.getPartitionNames()),
                            DebugUtil.getRootStackTrace(e));
                    throw new AnalysisException("failed to clear version map for force refresh: " + e.getMessage());
                } finally {
                    locker.unLockTableWithIntensiveDbLock(db.getId(), mv.getId(), LockType.WRITE);
                }
            }
        }

        boolean result = processor.getMvPctRefreshPartitioner().syncAddOrDropPartitions();
        if (result && processor.getMv().isPartitionedTable() && processor.getMvContext().getPartitionTopology() == null) {
            throw new DmlException("Materialized view %s.%s refresh failed: partition topology was not published "
                            + "after sync partitions succeeded",
                    processor.getDb().getFullName(), processor.getMv().getName());
        }
        processor.getLogger().info("finish sync partitions, cost(ms): {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return result;
    }

    private boolean syncAndCheckPCTPartitions(Map<BaseTableSnapshotInfo, PCellSortedSet> baseTableCandidatePartitions)
            throws AnalysisException, LockTimeoutException {
        int retryNum = 0;
        boolean checked = false;
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (!checked && retryNum++ < Config.max_mv_check_base_table_change_retry_times) {
            processor.increaseRefreshRetryMetaCount(1L);
            try (Timer ignored = Tracers.watchScope("MVRefreshExternalTable")) {
                if (!processor.isPinnedMode()) {
                    processor.refreshExternalTable(baseTableCandidatePartitions);
                } else {
                    processor.getLogger().info("Skip refreshExternalTable in pinned PCT mode");
                }
            }

            if (shouldSyncPartitionsAfterExternalRefresh(retryNum)) {
                try (Timer ignored = Tracers.watchScope("MVRefreshSyncPartitions")) {
                    if (!syncPartitions()) {
                        processor.getLogger().warn("Sync partitions failed.");
                        return false;
                    }
                }
            }

            try (Timer ignored = Tracers.watchScope("MVRefreshCheckBaseTableChange")) {
                if (checkPCTBaseTablePartitionChange()) {
                    processor.getLogger().info("materialized view base partition has changed. "
                            + "retry to sync partitions, retryNum:{}", retryNum);
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                    continue;
                }
            }
            checked = true;
        }
        Tracers.record("MVRefreshSyncPartitionsRetryTimes", String.valueOf(retryNum));
        processor.getLogger().info("sync and check mv partition changing after {} times: {}, costs: {} ms",
                retryNum, checked, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return checked;
    }

    private boolean shouldSyncPartitionsAfterExternalRefresh(int retryNum) {
        if (!processor.isExternalTablePreciseRefreshEnabled() || retryNum > 1) {
            return true;
        }

        for (BaseTableInfo baseTableInfo : processor.getMv().getBaseTableInfos()) {
            Optional<Table> optTable = MvUtils.getTable(baseTableInfo);
            if (optTable.isEmpty()) {
                continue;
            }
            Table table = optTable.get();
            if (isRefreshableExternalBaseTable(table) && !supportsPreciseExternalTableRefresh(table)) {
                return true;
            }
        }
        return false;
    }

    private boolean checkPCTBaseTablePartitionChange() throws LockTimeoutException {
        Locker locker = new Locker();
        LockParams lockParams = processor.collectDatabases();
        if (!locker.tryLockTableWithIntensiveDbLock(lockParams,
                LockType.READ, Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            processor.getLogger().warn("failed to lock database: {} in checkBaseTablePartitionChange", lockParams);
            throw new LockTimeoutException("Failed to lock database: " + lockParams
                    + " in checkBaseTablePartitionChange");
        }
        try {
            return processor.getSnapshotBaseTables().values().stream()
                    .anyMatch(snapshotInfo -> ((PCTTableSnapshotInfo) snapshotInfo).hasBaseTableChanged(processor.getMv()));
        } finally {
            locker.unLockTableWithIntensiveDbLock(lockParams, LockType.READ);
        }
    }

    private boolean isRefreshableExternalBaseTable(Table table) {
        return !(table.isNativeTableOrMaterializedView() || table.isView()
                || MaterializedViewAnalyzer.isExternalTableFromResource(table));
    }

    private boolean supportsPreciseExternalTableRefresh(Table table) {
        return table.isHiveTable() || table.isHudiTable();
    }
}
