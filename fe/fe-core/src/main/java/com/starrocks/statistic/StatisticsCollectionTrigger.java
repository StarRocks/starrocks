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

package com.starrocks.statistic;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.load.EtlStatus;
import com.starrocks.load.loadv2.LoadJobFinalOperation;
import com.starrocks.load.streamload.StreamLoadTxnCommitAttachment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DmlType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.InsertOverwriteJobStats;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.PartitionCommitInfo;
import com.starrocks.transaction.TableCommitInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TxnCommitAttachment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.starrocks.catalog.ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX;
import static com.starrocks.statistic.StatsConstants.AnalyzeType.SAMPLE;

/**
 * Trigger the statistics collection by data ingestion.
 * Basically we can have several types of data ingestion:
 * 1. Cold-start: ingest a large amount of data, then run some queries. *  We had better to wait for the ingestion
 * finish, so the following queries can have a good query plan
 * 2. Continuously small-batch ingest: it should not trigger the statistics collection, since the data characteristics
 * don't change so much
 * 3. Concurrent large-volume ingest: it may need to refresh the statistics collection, but we don't need to wait for
 * its finish.
 * 4. Another special case is data overwriting, if the data itself doesn't change so much, we don't either need to
 * refresh the statistics
 */
public class StatisticsCollectionTrigger {

    private static final Logger LOG = LogManager.getLogger(StatisticsCollectionTrigger.class);

    private DmlType dmlType;
    private InsertOverwriteJobStats overwriteJobStats;
    private TransactionState txnState;
    private Database db;
    private Table table;
    private boolean sync;
    private boolean useLock;

    // prepared parameters
    private final Set<Long> partitionIds = Sets.newHashSet();
    private StatsConstants.AnalyzeType analyzeType;

    // execution stats
    private Future<?> future;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    public static StatisticsCollectionTrigger triggerOnInsertOverwrite(InsertOverwriteJobStats stats,
                                                Database db,
                                                Table table,
                                                boolean sync,
                                                boolean useLock) {
        StatisticsCollectionTrigger trigger = new StatisticsCollectionTrigger();
        trigger.dmlType = DmlType.INSERT_OVERWRITE;
        trigger.db = db;
        trigger.table = table;
        trigger.sync = sync;
        trigger.overwriteJobStats = stats;
        trigger.useLock = useLock;
        trigger.process();
        return trigger;
    }

    public static StatisticsCollectionTrigger triggerOnFirstLoad(TransactionState txnState,
                                          Database db,
                                          Table table,
                                          boolean sync,
                                          boolean useLock) {
        StatisticsCollectionTrigger trigger = new StatisticsCollectionTrigger();
        trigger.dmlType = DmlType.INSERT_INTO;
        trigger.db = db;
        trigger.table = table;
        trigger.sync = sync;
        trigger.txnState = txnState;
        trigger.useLock = useLock;
        trigger.process();
        return trigger;
    }

    private void process() {
        // check if this feature is disabled
        if (!Config.enable_statistic_collect_on_first_load) {
            return;
        }
        // check if it's in black-list
        if (StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
            return;
        }
        // check it's the first-load
        if (txnState != null) {
            if (txnState.getIdToTableCommitInfos() == null) {
                return;
            }
            TableCommitInfo tableCommitInfo = txnState.getIdToTableCommitInfos().get(table.getId());
            if (tableCommitInfo == null) {
                return;
            }
            prepareAnalyzeJobForLoad();
        } else {
            prepareAnalyzeForOverwrite();
        }

        if (partitionIds.isEmpty()) {
            return;
        }

        if (dmlType == DmlType.INSERT_OVERWRITE && analyzeType == null) {
            // update the partition id of existing statistics
            ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
            try (ConnectContext.ScopeGuard guard = statsConnectCtx.bindScope()) {
                for (int i = 0; i < overwriteJobStats.getSourcePartitionIds().size(); i++) {
                    long sourcePartitionId = overwriteJobStats.getSourcePartitionIds().get(i);
                    long targetPartitionId = overwriteJobStats.getTargetPartitionIds().get(i);
                    if (table.getPartition(targetPartitionId) == null ||
                            table.getPartition(targetPartitionId).getName().startsWith(SHADOW_PARTITION_PREFIX)) {
                        continue;
                    }
                    StatisticExecutor.overwritePartitionStatistics(
                            statsConnectCtx, db.getId(), table.getId(), sourcePartitionId, targetPartitionId);
                }
            } catch (Exception e) {
                LOG.warn("overwrite partition stats failed table={} partitions={}",
                        table.getId(), overwriteJobStats.getTargetPartitionIds(), e);
            }
        } else if (analyzeType != null) {
            // collect
            execute();
            waitFinish();
        }
    }

    private void execute() {
        Map<String, String> properties = Maps.newHashMap();
        if (SAMPLE == analyzeType) {
            properties = StatsConstants.buildInitStatsProp();
        }
        AnalyzeStatus analyzeStatus = new NativeAnalyzeStatus(GlobalStateMgr.getCurrentState().getNextId(),
                db.getId(), table.getId(), null, analyzeType,
                StatsConstants.ScheduleType.ONCE, properties, LocalDateTime.now());
        analyzeStatus.setStatus(StatsConstants.ScheduleStatus.PENDING);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);

        try {
            future = GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAnalyzeTaskThreadPool()
                    .submit(() -> {
                        isRunning.set(true);
                        // reset the start time after pending, so [end-start] can represent execution period
                        analyzeStatus.setStartTime(LocalDateTime.now());
                        StatisticExecutor statisticExecutor = new StatisticExecutor();
                        ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
                        // set session id for temporary table
                        if (table.isTemporaryTable()) {
                            statsConnectCtx.setSessionId(((OlapTable) table).getSessionId());
                        }
                        statsConnectCtx.setThreadLocalInfo();

                        statisticExecutor.collectStatistics(statsConnectCtx,
                                StatisticsCollectJobFactory.buildStatisticsCollectJob(db, table,
                                        new ArrayList<>(partitionIds), null, null,
                                        analyzeType, StatsConstants.ScheduleType.ONCE,
                                        analyzeStatus.getProperties(), List.of(), List.of()), analyzeStatus, false);
                    });
        } catch (Throwable e) {
            LOG.error("failed to submit statistic collect job", e);
            return;
        }
    }

    private void waitFinish() {
        if (sync) {
            // wait a short-time for the task getting executed, if too many jobs in the queue we just give up
            // waiting, otherwise it may block the data loading for a long period
            try {
                future.get(1, TimeUnit.SECONDS);
                Preconditions.checkState(isRunning.get());
                return;
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("failed to execute statistic collect job", e);
            } catch (TimeoutException e) {
                if (!isRunning.get()) {
                    LOG.warn(
                            "await collect statistic task failed after 1 seconds, which mean too many jobs in the " +
                                    "queue");
                    return;
                }
            }

            // it's already running wait for the task finish
            Preconditions.checkState(isRunning.get());
            long await = Config.semi_sync_collect_statistic_await_seconds;
            try {
                future.get(await, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("failed to execute statistic collect job", e);
            } catch (TimeoutException e) {
                LOG.warn("await collect statistic failed after {} seconds", await);
            }
        }
    }

    private void prepareAnalyzeJobForLoad() {
        TableCommitInfo tableCommitInfo = txnState.getIdToTableCommitInfos().get(table.getId());
        Locker locker = new Locker();
        if (useLock) {
            locker.lockTablesWithIntensiveDbLock(db.getId(), List.of(table.getId()), LockType.READ);
        }
        try {
            for (var entry : tableCommitInfo.getIdToPartitionCommitInfo().entrySet()) {
                // partition commit info id is physical partition id.
                // statistic collect granularity is logic partition.
                long physicalPartitionId = entry.getKey();
                PartitionCommitInfo partitionCommitInfo = entry.getValue();
                if (partitionCommitInfo.getVersion() == Partition.PARTITION_INIT_VERSION + 1) {
                    PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
                    Partition partition = table.getPartition(physicalPartition.getParentId());
                    partitionIds.add(partition.getId());
                }
            }
        } finally {
            if (useLock) {
                locker.unLockTablesWithIntensiveDbLock(db.getId(), List.of(table.getId()), LockType.READ);
            }
        }
        if (partitionIds.isEmpty()) {
            return;
        }

        analyzeType = decideAnalyzeTypeForLoad(txnState, (OlapTable) table);
    }

    private void prepareAnalyzeForOverwrite() {
        partitionIds.addAll(overwriteJobStats.getTargetPartitionIds());
        analyzeType = decideAnalyzeTypeForOverwrite();
    }

    private StatsConstants.AnalyzeType decideAnalyzeTypeForOverwrite() {
        Preconditions.checkNotNull(overwriteJobStats, "must have overwrite stats");
        long sourceRows = overwriteJobStats.getSourceRows();
        long targetRows = overwriteJobStats.getTargetRows();
        double deltaRatio = 1.0 * (targetRows - sourceRows) / (sourceRows + 1);
        if (deltaRatio < Config.statistic_sample_collect_ratio_threshold_of_first_load) {
            return null;
        } else if (targetRows > Config.statistic_sample_collect_rows) {
            return StatsConstants.AnalyzeType.SAMPLE;
        } else {
            return StatsConstants.AnalyzeType.FULL;
        }
    }

    private StatsConstants.AnalyzeType decideAnalyzeTypeForLoad(TransactionState txnState, OlapTable table) {
        Long loadRows = null;
        TxnCommitAttachment attachment = txnState.getTxnCommitAttachment();
        if (attachment instanceof LoadJobFinalOperation) {
            EtlStatus loadingStatus = ((LoadJobFinalOperation) attachment).getLoadingStatus();
            loadRows = loadingStatus.getLoadedRows(table.getId());
        } else if (attachment instanceof InsertTxnCommitAttachment) {
            loadRows = ((InsertTxnCommitAttachment) attachment).getLoadedRows();
        } else if (attachment instanceof StreamLoadTxnCommitAttachment) {
            loadRows = ((StreamLoadTxnCommitAttachment) attachment).getNumRowsNormal();
        }
        if (loadRows == null) {
            return null;
        }

        long totalRows = partitionIds.stream()
                .mapToLong(p -> table.mayGetPartition(p).stream().mapToLong(Partition::getRowCount).sum())
                .sum();
        double deltaRatio = 1.0 * loadRows / (totalRows + 1);
        if (deltaRatio < Config.statistic_sample_collect_ratio_threshold_of_first_load) {
            return null;
        } else if (loadRows > Config.statistic_sample_collect_rows) {
            return StatsConstants.AnalyzeType.SAMPLE;
        } else {
            return StatsConstants.AnalyzeType.FULL;
        }
    }

    StatsConstants.AnalyzeType getAnalyzeType() {
        return analyzeType;
    }
}
