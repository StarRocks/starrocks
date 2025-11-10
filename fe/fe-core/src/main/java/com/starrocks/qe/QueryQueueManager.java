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
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.ResourceGroupMetricMgr;
import com.starrocks.qe.scheduler.RecoverableException;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.qe.scheduler.slot.BaseSlotManager;
import com.starrocks.qe.scheduler.slot.LocalSlotProvider;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.qe.scheduler.slot.QueryQueueOptions;
import com.starrocks.qe.scheduler.slot.SlotEstimator;
import com.starrocks.qe.scheduler.slot.SlotEstimatorFactory;
import com.starrocks.qe.scheduler.slot.SlotProvider;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TWorkGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class QueryQueueManager {
    private static final Logger LOG = LogManager.getLogger(QueryQueueManager.class);

    private static final String PENDING_TIMEOUT_ERROR_MSG_FORMAT =
            "Failed to allocate resource to query: pending timeout [%ds], you could modify %s to pending more time";

    private static class SingletonHolder {
        private static final QueryQueueManager INSTANCE = new QueryQueueManager();
    }

    public static QueryQueueManager getInstance() {
        return QueryQueueManager.SingletonHolder.INSTANCE;
    }

    public void maybeWait(ConnectContext context, DefaultCoordinator coord) throws StarRocksException, InterruptedException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Maybe wait for query queue, queryId: {}", UUIDUtil.fromTUniqueid(coord.getQueryId()).toString());
        }
        final JobSpec jobSpec = coord.getJobSpec();
        final SlotProvider slotProvider = jobSpec.getSlotProvider();
        long startMs = System.currentTimeMillis();
        boolean isPending = false;
        try {
            LogicalSlot slotRequirement = createSlot(context, coord);
            coord.setSlot(slotRequirement);

            // register listeners
            if (jobSpec.isQueryType())  {
                context.registerListener(new LogicalSlot.ConnectContextListener(slotRequirement));
            }

            // LocalSlotProvider does not need to queue, just return directly. Currently, it is only used to adjust DOP
            // through requireSlot->PipelineDriverAllocator.
            if (slotProvider instanceof LocalSlotProvider) {
                slotProvider.requireSlot(slotRequirement);
                return;
            }

            isPending = true;
            context.setPending(true);
            MetricRepo.COUNTER_QUERY_QUEUE_PENDING.increase(1L);
            MetricRepo.COUNTER_QUERY_QUEUE_TOTAL.increase(1L);
            ResourceGroupMetricMgr.increaseQueuedQuery(context, 1L);

            long deadlineEpochMs = slotRequirement.getExpiredPendingTimeMs();
            LogicalSlot allocatedSlot = null;
            final long warehouseId = context.getCurrentWarehouseId();
            final BaseSlotManager slotManager = GlobalStateMgr.getCurrentState().getSlotManager();
            // first update pending time in context to avoid query timeout when the query in the query queue
            int queryQueuePendingTimeout = slotManager.getQueryQueuePendingTimeoutSecond(warehouseId);
            context.setPendingTimeSecond(queryQueuePendingTimeout);

            while (allocatedSlot == null) {
                // Check timeout.
                long currentMs = System.currentTimeMillis();
                if (slotRequirement.isPendingTimeout()) {
                    MetricRepo.COUNTER_QUERY_QUEUE_TIMEOUT.increase(1L);
                    slotProvider.cancelSlotRequirement(slotRequirement);
                    String timeoutVar =
                            String.format("the session variable [%s]", GlobalVariable.QUERY_QUEUE_PENDING_TIMEOUT_SECOND);
                    String errMsg = String.format(PENDING_TIMEOUT_ERROR_MSG_FORMAT,
                            queryQueuePendingTimeout, timeoutVar);
                    ResourceGroupMetricMgr.increaseTimeoutQueuedQuery(context, 1L);
                    throw new StarRocksException(errMsg);
                }

                Future<LogicalSlot> slotFuture = slotProvider.requireSlot(slotRequirement);

                // Wait for slot allocated.
                try {
                    allocatedSlot = slotFuture.get(deadlineEpochMs - currentMs, TimeUnit.MILLISECONDS);
                } catch (ExecutionException e) {
                    LOG.warn("[Slot] failed to allocate resource to query [slot={}]", slotRequirement, e);
                    if (e.getCause() instanceof RecoverableException) {
                        continue;
                    }
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
                }
            }
        } finally {
            if (isPending) {
                long pendingTimeMs = System.currentTimeMillis() - startMs;
                // then update the real pending time in context that the time should bec calculated in query's timeout.
                context.auditEventBuilder.setPendingTimeMs(pendingTimeMs);
                context.setPendingTimeSecond((int) (pendingTimeMs / 1000));

                MetricRepo.COUNTER_QUERY_QUEUE_PENDING.increase(-1L);
                ResourceGroupMetricMgr.increaseQueuedQuery(context, -1L);
                context.setPending(false);
            }
        }
    }

    private LogicalSlot createSlot(ConnectContext context, DefaultCoordinator coord) throws StarRocksException {
        Pair<String, Integer> selfIpAndPort = GlobalStateMgr.getCurrentState().getNodeMgr().getSelfIpAndRpcPort();
        Frontend frontend = GlobalStateMgr.getCurrentState().getNodeMgr().getFeByHost(selfIpAndPort.first);
        if (frontend == null) {
            throw new StarRocksException("cannot get frontend from the local host: " + selfIpAndPort.first);
        }

        TWorkGroup group = coord.getJobSpec().getResourceGroup();
        long groupId = group == null ? LogicalSlot.ABSENT_GROUP_ID : group.getId();

        long nowMs = context.getStartTime();
        final BaseSlotManager slotManager = GlobalStateMgr.getCurrentState().getSlotManager();
        long queryQueuePendingTimeoutSecond =
                slotManager.getQueryQueuePendingTimeoutSecond(context.getCurrentWarehouseId());
        long expiredPendingTimeMs = nowMs + queryQueuePendingTimeoutSecond * 1000L;
        long expiredAllocatedTimeMs = nowMs + queryQueuePendingTimeoutSecond * 1000L;

        int numFragments = coord.getFragments().size();
        int pipelineDop = coord.getJobSpec().getQueryOptions().getPipeline_dop();
        if (!coord.getJobSpec().isStatisticsJob() && !coord.isLoadType()
                && context.getSessionVariable().isEnablePipelineAdaptiveDop()) {
            pipelineDop = 0;
        }

        int numSlots = estimateNumSlots(context, coord);
        long warehouseId = context.getCurrentWarehouseId();

        return new LogicalSlot(coord.getQueryId(), frontend.getNodeName(), warehouseId,
                groupId, numSlots, expiredPendingTimeMs, expiredAllocatedTimeMs,
                frontend.getStartTime(), numFragments, pipelineDop);
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

}
