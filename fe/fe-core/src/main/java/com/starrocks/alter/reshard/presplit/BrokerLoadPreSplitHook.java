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
import com.starrocks.planner.LoadScanNode;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * BrokerLoadJob → coordinator bridge for Sample-Based Tablet Pre-Split on the
 * Broker Load path.
 *
 * <p>The hook is invoked from {@code BrokerLoadJob.createLoadingTask} after
 * the per-table {@link OlapTable} is resolved and the file statuses are
 * known, but BEFORE the {@code LoadLoadingTask} is built. The Broker Load
 * txn is already open at that point, so the hook is fire-and-forget for the
 * same reason {@link InsertFromFilesPreSplitHook} is: synchronously waiting
 * for FINISHED would deadlock the reshard daemon's cleanup-phase prev-txn
 * wait against this same load txn.
 *
 * <p>Sampler-executor selection is delegated to
 * {@link DefaultPreSplitPipeline#forLoadKind}: Tier 1 uses
 * {@link BrokerLoadRowGroupStatisticsProvider}, Tier 2 uses
 * {@link BrokerLoadSampleSubqueryExecutor}. The per-path Config flag
 * {@code enable_tablet_pre_split_for_broker_load} defaults to
 * {@code false}, so the hook never reaches the executors until the
 * operator opts in.
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
     * @param fileStatuses nested per-file-group file statuses from
     *                     {@code BrokerPendingTaskAttachment.getFileStatusByTable}.
     */
    public static void maybeRunPreSplit(
            Database database, OlapTable targetTable, BrokerDesc brokerDesc,
            List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatuses,
            ComputeResource computeResource) {
        try {
            tryRunPreSplit(database, targetTable, brokerDesc, fileGroups, fileStatuses, computeResource);
        } catch (Throwable unexpected) {
            LOG.warn("Sample-Based Tablet Pre-Split hook failed for Broker Load; proceeding without pre-split",
                    unexpected);
        }
    }

    private static void tryRunPreSplit(
            Database database, OlapTable targetTable, BrokerDesc brokerDesc,
            List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatuses,
            ComputeResource computeResource) {
        if (!Config.enable_tablet_pre_split_for_broker_load) {
            return;
        }
        // BrokerLoadJob.createLoadingTask guarantees database / targetTable / computeResource are
        // non-null by the time the hook fires; fileGroups / fileStatuses come from the attachment
        // and the pending-task contract makes them non-null too, but we tolerate null defensively.
        if (fileGroups == null || fileStatuses == null) {
            return;
        }
        PreSplitTargets.EligibleTarget target = PreSplitTargets.findEligibleTarget(database, targetTable);
        if (target == null) {
            return;
        }
        submitToCoordinator(target, brokerDesc, fileGroups, fileStatuses, computeResource);
    }

    private static void submitToCoordinator(
            PreSplitTargets.EligibleTarget target, BrokerDesc brokerDesc,
            List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatuses,
            ComputeResource computeResource) {
        BrokerLoadScanContext scanContext = new BrokerLoadScanContext(
                brokerDesc, fileGroups, fileStatuses, computeResource);
        int activeComputeNodeCount = Math.max(1,
                LoadScanNode.getAvailableComputeNodes(computeResource).size());
        long fileTotalBytes = sumFileBytes(fileStatuses);

        DefaultPreSplitPipeline pipeline = DefaultPreSplitPipeline.forLoadKind(
                target.database(), target.olapTable(), target.oldTabletId(), fileTotalBytes,
                LoadKind.BROKER_LOAD);

        PreSplitOutcome outcome = TabletPreSplitCoordinator.submitAsynchronously(
                target.database(), target.olapTable(), target.partitionId(), scanContext,
                LoadKind.BROKER_LOAD, pipeline, activeComputeNodeCount);
        LOG.info("Sample-Based Tablet Pre-Split outcome for Broker Load on table {}: {}",
                target.olapTable().getName(), outcome);
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
