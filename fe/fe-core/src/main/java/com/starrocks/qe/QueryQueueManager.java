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

package com.starrocks.qe;

import com.starrocks.common.Pair;
<<<<<<< HEAD
import com.starrocks.common.UserException;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.ResourceGroupMetricMgr;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SchemaScanNode;
import com.starrocks.qe.scheduler.RecoverableException;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
=======
import com.starrocks.common.StarRocksException;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.ResourceGroupMetricMgr;
import com.starrocks.qe.scheduler.RecoverableException;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.qe.scheduler.slot.QueryQueueOptions;
import com.starrocks.qe.scheduler.slot.SlotEstimator;
import com.starrocks.qe.scheduler.slot.SlotEstimatorFactory;
import com.starrocks.qe.scheduler.slot.SlotProvider;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TWorkGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

<<<<<<< HEAD
import java.util.List;
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class QueryQueueManager {
    private static final Logger LOG = LogManager.getLogger(QueryQueueManager.class);

    private static final String PENDING_TIMEOUT_ERROR_MSG_FORMAT =
            "Failed to allocate resource to query: pending timeout [%d], " +
                    "you could modify the session variable [%s] to pending more time";

    private static class SingletonHolder {
        private static final QueryQueueManager INSTANCE = new QueryQueueManager();
    }

    public static QueryQueueManager getInstance() {
        return QueryQueueManager.SingletonHolder.INSTANCE;
    }

<<<<<<< HEAD
    public void maybeWait(ConnectContext context, Coordinator coord) throws UserException, InterruptedException {
        CoordinatorPreprocessor coordPrepare = coord.getPrepareInfo();
        coordPrepare.setNeedCheckQueued(needCheckQueue(coord));
        coordPrepare.setEnableQueue(isEnableQueue(coord));
        coordPrepare.setEnableGroupLevelQueue(coordPrepare.isEnableQueue() && GlobalVariable.isEnableGroupLevelQueryQueue());

        if (!coordPrepare.isNeedCheckQueued() || !coordPrepare.isEnableQueue()) {
            return;
        }

        long startMs = System.currentTimeMillis();
        boolean isPending = false;
        try {
            LogicalSlot slotRequirement = createSlot(coord);
=======
    public void maybeWait(ConnectContext context, DefaultCoordinator coord) throws StarRocksException, InterruptedException {
        SlotProvider slotProvider = coord.getJobSpec().getSlotProvider();
        long startMs = System.currentTimeMillis();
        boolean isPending = false;
        try {
            LogicalSlot slotRequirement = createSlot(context, coord);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            coord.setSlot(slotRequirement);

            isPending = true;
            context.setPending(true);
            MetricRepo.COUNTER_QUERY_QUEUE_PENDING.increase(1L);
            MetricRepo.COUNTER_QUERY_QUEUE_TOTAL.increase(1L);
            ResourceGroupMetricMgr.increaseQueuedQuery(context, 1L);

<<<<<<< HEAD
            long timeoutMs = slotRequirement.getExpiredPendingTimeMs();
=======
            long deadlineEpochMs = slotRequirement.getExpiredPendingTimeMs();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            LogicalSlot allocatedSlot = null;
            while (allocatedSlot == null) {
                // Check timeout.
                long currentMs = System.currentTimeMillis();
<<<<<<< HEAD
                if (currentMs >= timeoutMs) {
                    MetricRepo.COUNTER_QUERY_QUEUE_TIMEOUT.increase(1L);
                    GlobalStateMgr.getCurrentState().getSlotProvider().cancelSlotRequirement(slotRequirement);
=======
                if (slotRequirement.isPendingTimeout()) {
                    MetricRepo.COUNTER_QUERY_QUEUE_TIMEOUT.increase(1L);
                    slotProvider.cancelSlotRequirement(slotRequirement);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                    String errMsg = String.format(PENDING_TIMEOUT_ERROR_MSG_FORMAT,
                            GlobalVariable.getQueryQueuePendingTimeoutSecond(),
                            GlobalVariable.QUERY_QUEUE_PENDING_TIMEOUT_SECOND);
                    ResourceGroupMetricMgr.increaseTimeoutQueuedQuery(context, 1L);
<<<<<<< HEAD
                    throw new UserException(errMsg);
                }

                Future<LogicalSlot> slotFuture = GlobalStateMgr.getCurrentState().getSlotProvider().requireSlot(slotRequirement);

                // Wait for slot allocated.
                try {
                    allocatedSlot = slotFuture.get(timeoutMs - currentMs, TimeUnit.MILLISECONDS);
=======
                    throw new StarRocksException(errMsg);
                }

                Future<LogicalSlot> slotFuture = slotProvider.requireSlot(slotRequirement);

                // Wait for slot allocated.
                try {
                    allocatedSlot = slotFuture.get(deadlineEpochMs - currentMs, TimeUnit.MILLISECONDS);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                } catch (ExecutionException e) {
                    LOG.warn("[Slot] failed to allocate resource to query [slot={}]", slotRequirement, e);
                    if (e.getCause() instanceof RecoverableException) {
                        continue;
                    }
<<<<<<< HEAD
                    throw new UserException("Failed to allocate resource to query: " + e.getMessage(), e);
                } catch (TimeoutException e) {
                    // Check timeout in the next loop.
                } catch (CancellationException e) {
                    throw new UserException("Cancelled");
=======
                    throw new StarRocksException("Failed to allocate resource to query: " + e.getMessage(), e);
                } catch (TimeoutException e) {
                    // Check timeout in the next loop.
                } catch (CancellationException e) {
                    // There are two threads checking timeout, one is current thread, the other is CheckTimer.
                    // So this thread can get be cancelled by CheckTimer
                    if (slotRequirement.isPendingTimeout()) {
                        continue;
                    }
                    throw new StarRocksException("Cancelled", e);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                }
            }
        } finally {
            if (isPending) {
                context.auditEventBuilder.setPendingTimeMs(System.currentTimeMillis() - startMs);
                MetricRepo.COUNTER_QUERY_QUEUE_PENDING.increase(-1L);
                ResourceGroupMetricMgr.increaseQueuedQuery(context, -1L);
                context.setPending(false);
            }
        }
    }

<<<<<<< HEAD
    public boolean isEnableQueue(Coordinator coord) {
        ConnectContext context = coord.getConnectContext();
        if (context != null && context.getSessionVariable() != null && !context.getSessionVariable().isEnableQueryQueue()) {
            return false;
        }

        if (coord.isStatisticsJob()) {
            return GlobalVariable.isEnableQueryQueueStatistic();
        }

        if (coord.isLoadType()) {
            return GlobalVariable.isEnableQueryQueueLoad();
        }

        return GlobalVariable.isEnableQueryQueueSelect();
    }

    public boolean needCheckQueue(Coordinator coord) {
        if (!coord.isNeedQueued()) {
            return false;
        }

        // The queries only using schema meta will never been queued, because a MySQL client will
        // query schema meta after the connection is established.
        List<ScanNode> scanNodes = coord.getScanNodes();
        boolean notNeed = scanNodes.isEmpty() || scanNodes.stream().allMatch(SchemaScanNode.class::isInstance);
        return !notNeed;
    }

    private LogicalSlot createSlot(Coordinator coord) throws UserException {
        Pair<String, Integer> selfIpAndPort = GlobalStateMgr.getCurrentState().getNodeMgr().getSelfIpAndRpcPort();
        Frontend frontend = GlobalStateMgr.getCurrentState().getFeByHost(selfIpAndPort.first);
        if (frontend == null) {
            throw new UserException("cannot get frontend from the local host: " + selfIpAndPort.first);
        }

        TWorkGroup group = coord.getResourceGroup();
        long groupId = group == null ? LogicalSlot.ABSENT_GROUP_ID : group.getId();

        long nowMs = System.currentTimeMillis();
        long queryTimeoutSecond = coord.getQueryOptions().getQuery_timeout();
=======
    private LogicalSlot createSlot(ConnectContext context, DefaultCoordinator coord) throws StarRocksException {
        Pair<String, Integer> selfIpAndPort = GlobalStateMgr.getCurrentState().getNodeMgr().getSelfIpAndRpcPort();
        Frontend frontend = GlobalStateMgr.getCurrentState().getNodeMgr().getFeByHost(selfIpAndPort.first);
        if (frontend == null) {
            throw new StarRocksException("cannot get frontend from the local host: " + selfIpAndPort.first);
        }

        TWorkGroup group = coord.getJobSpec().getResourceGroup();
        long groupId = group == null ? LogicalSlot.ABSENT_GROUP_ID : group.getId();

        long nowMs = context.getStartTime();
        long queryTimeoutSecond = coord.getJobSpec().getQueryOptions().getQuery_timeout();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        long expiredPendingTimeMs =
                nowMs + Math.min(GlobalVariable.getQueryQueuePendingTimeoutSecond(), queryTimeoutSecond) * 1000L;
        long expiredAllocatedTimeMs = nowMs + queryTimeoutSecond * 1000L;

        int numFragments = coord.getFragments().size();
<<<<<<< HEAD
        int pipelineDop = coord.getQueryOptions().getPipeline_dop();
        if (!coord.isStatisticsJob() && !coord.isLoadType()
                && ConnectContext.get() != null && ConnectContext.get().getSessionVariable().isEnablePipelineAdaptiveDop()) {
            pipelineDop = 0;
        }

        return new LogicalSlot(coord.getQueryId(), frontend.getNodeName(), groupId, 1, expiredPendingTimeMs,
                expiredAllocatedTimeMs, frontend.getStartTime(), numFragments, pipelineDop);
    }
=======
        int pipelineDop = coord.getJobSpec().getQueryOptions().getPipeline_dop();
        if (!coord.getJobSpec().isStatisticsJob() && !coord.isLoadType()
                && context.getSessionVariable().isEnablePipelineAdaptiveDop()) {
            pipelineDop = 0;
        }

        int numSlots = estimateNumSlots(context, coord);

        return new LogicalSlot(coord.getQueryId(), frontend.getNodeName(), groupId, numSlots, expiredPendingTimeMs,
                expiredAllocatedTimeMs, frontend.getStartTime(), numFragments, pipelineDop);
    }

    private int estimateNumSlots(ConnectContext context, DefaultCoordinator coord) {
        QueryQueueOptions opts = QueryQueueOptions.createFromEnvAndQuery(coord);

        SlotEstimator estimator = SlotEstimatorFactory.create(opts);
        final int numSlots = estimator.estimateSlots(opts, context, coord);
        // Write numSlots to the audit log if query queue v2 is enabled, since numSlots is always 1 for query queue v1 and
        // may be different for query queue v2.
        if (opts.isEnableQueryQueueV2()) {
            context.auditEventBuilder.setNumSlots(numSlots);
        }

        return numSlots;
    }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
}
