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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * BrokerLoadJob → coordinator bridge for Sample-Based Tablet Pre-Split on the
 * Broker Load path.
 *
 * <p>The hook is invoked from {@code BrokerLoadJob.createLoadingTask} after
 * the per-table {@link OlapTable} reference and the broker-resolved file
 * statuses are snapshotted — but <b>before</b> {@code beginTxn()} opens
 * {@code T_load} and before any {@code LoadLoadingTask.prepare()} pins the
 * sink plan. Pre-created partitions and the post-reshard tablet layout
 * therefore accelerate the triggering Broker Load itself, symmetric with
 * the INSERT hook ({@link InsertPreSplitHook}).
 *
 * <p>The hook resolves the Broker Load source into a {@link PreSplitFlow.Prepared}
 * bundle and hands control to {@link PreSplitFlow#dispatch}, which sync-awaits the
 * reshard daemon's {@code FINISHED} transition on both the single-partition and
 * multi-partition paths. Both are fail-safe: on timeout / abort / wait failure the
 * flow logs and proceeds against whatever tablet layout is currently visible — never
 * aborts the triggering load. Sync-await is deadlock-safe here specifically
 * because {@code BrokerLoadJob.unprotectedExecute()} defers {@code beginTxn}
 * until <b>after</b> the hook returns, so the reshard daemon's cleanup-phase
 * {@code isPreviousTransactionsFinished(endTransactionId, ...)} wait cannot
 * include the not-yet-allocated {@code T_load}.
 *
 * <p>Sampler-executor selection is delegated to
 * {@link DefaultPreSplitPipeline#forLoadKind}: meta tier uses
 * {@link BrokerLoadRowGroupStatisticsProvider}, data tier uses
 * {@link BrokerLoadSampleSubqueryExecutor}. The per-path Config flag
 * {@code enable_tablet_pre_split_for_broker_load} defaults to
 * {@code true} as of v4.1.0 (GA flip); set it to {@code false} to disable
 * cluster-wide. The session variable {@code enable_tablet_pre_split}
 * (also default {@code true}) provides a per-session opt-out checked
 * early in this hook so a session-opt-out load does not pay the
 * eligibility-target walk and scan-context build.
 */
public final class BrokerLoadPreSplitHook {

    private static final Logger LOG = LogManager.getLogger(BrokerLoadPreSplitHook.class);

    private BrokerLoadPreSplitHook() {
    }

    /**
     * Entry point invoked from {@code BrokerLoadJob.createLoadingTask}.
     *
     * <p>The method is fully self-contained: any throw is swallowed and the
     * load proceeds without pre-split. Failing here must not abort an
     * otherwise-valid load.
     *
     * <p>{@code shouldAbort} is polled between reshard-daemon polls so that
     * {@code processTimeout} / user-cancel releases the
     * {@code pending_load_task_scheduler} slot promptly rather than waiting
     * out the {@code tablet_pre_split_post_submit_wait_seconds} ceiling. A
     * {@code true} return short-circuits the wait without aborting the load.
     * Pass {@code () -> false} to disable.
     *
     * @param context      the load's {@link ConnectContext}. Mirrors the
     *                     {@link InsertPreSplitHook} parameter
     *                     threading: passing the context explicitly avoids
     *                     a thread-local fallback that would create an
     *                     uninitialized {@link ConnectContext} (no auth /
     *                     no session vars / no current DB) and surface as
     *                     a confusing analyze-time NPE inside
     *                     {@link PartitionSampleGrouper}.
     * @param fileStatuses nested per-file-group file statuses from
     *                     {@code BrokerPendingTaskAttachment.getFileStatusByTable}.
     */
    public static void maybeRunPreSplit(
            ConnectContext context, Database database, OlapTable targetTable, BrokerDesc brokerDesc,
            List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatuses,
            ComputeResource computeResource, BooleanSupplier shouldAbort) {
        try {
            tryRunPreSplit(context, database, targetTable, brokerDesc, fileGroups, fileStatuses,
                    computeResource, shouldAbort);
        } catch (Throwable unexpected) {
            LOG.warn("Sample-Based Tablet Pre-Split hook failed for Broker Load; proceeding without pre-split",
                    unexpected);
        }
    }

    private static void tryRunPreSplit(
            ConnectContext context, Database database, OlapTable targetTable, BrokerDesc brokerDesc,
            List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatuses,
            ComputeResource computeResource, BooleanSupplier shouldAbort) {
        Objects.requireNonNull(context, "context");
        if (!Config.enable_tablet_pre_split_for_broker_load) {
            // Record here: the coordinator's checkConfigAndSession is never
            // reached on this early return, so it can't bump the bucket itself.
            PreSplitMetrics.recordEligibilitySkip(SkipReason.DISABLED_BY_CONFIG);
            return;
        }
        // Honor the per-session opt-out before target resolution + scan-context
        // build. The caller (BrokerLoadJob.firePreSplitHooks) passes the load's
        // ConnectContext directly so we read the load's session variable rather
        // than relying on thread-local state. The helper bumps the
        // disabled_by_session bvar — the coordinator never sees this skip, but
        // operators still need the bvar.
        if (PreSplitMetrics.shortCircuitOnSessionOptOut(context.getSessionVariable())) {
            return;
        }
        // BrokerLoadJob.createLoadingTask guarantees database / targetTable / computeResource are
        // non-null by the time the hook fires; fileGroups / fileStatuses come from the attachment
        // and the pending-task contract makes them non-null too, but we tolerate null defensively.
        if (fileGroups == null || fileStatuses == null) {
            return;
        }
        // Table-level eligibility: structural checks shared with the multi-partition
        // coordinator's defensive re-check. Per-partition checks (single physical
        // partition, single base tablet, empty partition) remain with the legacy
        // single-partition path; the multi-partition path runs them per-bucket
        // after pre-create under its own short READ lock.
        SkipReason tableLevelSkip = PreSplitTargets.findEligibleTable(database, targetTable);
        if (tableLevelSkip != null) {
            PreSplitMetrics.recordEligibilitySkip(tableLevelSkip);
            return;
        }
        BrokerLoadScanContext scanContext = new BrokerLoadScanContext(
                brokerDesc, fileGroups, fileStatuses, computeResource);
        PreSplitFlow.Prepared prepared = new PreSplitFlow.Prepared(
                scanContext,
                MetaUtils.getRangeDistributionColumns(targetTable),
                targetTable.getPartitionInfo().getPartitionColumns(targetTable.getIdToColumn()),
                sumFileBytes(fileStatuses),
                computeResource);
        PreSplitFlow.dispatch(database, targetTable, prepared, LoadKind.BROKER_LOAD, shouldAbort, context);
    }

    private static long sumFileBytes(List<List<TBrokerFileStatus>> fileStatuses) {
        long total = 0L;
        for (List<TBrokerFileStatus> fileGroupStatuses : fileStatuses) {
            if (fileGroupStatuses == null) {
                continue;
            }
            for (TBrokerFileStatus fileStatus : fileGroupStatuses) {
                if (fileStatus != null) {
                    total += fileStatus.size;
                }
            }
        }
        return total;
    }
}
