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
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.ResourceGroupMetricMgr;
import com.starrocks.qe.scheduler.RecoverableException;
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
        SlotProvider slotProvider = coord.getJobSpec().getSlotProvider();
        long startMs = System.currentTimeMillis();
        boolean isPending = false;
        try {
            LogicalSlot slotRequirement = createSlot(context, coord);
            coord.setSlot(slotRequirement);

            isPending = true;
            context.setPending(true);
            MetricRepo.COUNTER_QUERY_QUEUE_PENDING.increase(1L);
            MetricRepo.COUNTER_QUERY_QUEUE_TOTAL.increase(1L);
            ResourceGroupMetricMgr.increaseQueuedQuery(context, 1L);

            long deadlineEpochMs = slotRequirement.getExpiredPendingTimeMs();
            LogicalSlot allocatedSlot = null;
            while (allocatedSlot == null) {
                // Check timeout.
                long currentMs = System.currentTimeMillis();
                if (slotRequirement.isPendingTimeout()) {
                    MetricRepo.COUNTER_QUERY_QUEUE_TIMEOUT.increase(1L);
                    slotProvider.cancelSlotRequirement(slotRequirement);
                    int queryQueuePendingTimeout = GlobalVariable.getQueryQueuePendingTimeoutSecond();
                    int queryTimeout = coord.getJobSpec().getQueryOptions().query_timeout;
                    String timeoutVar = queryQueuePendingTimeout < queryTimeout ?
                            String.format("the session variable [%s]", GlobalVariable.QUERY_QUEUE_PENDING_TIMEOUT_SECOND) :
                            "query/insert timeout";
                    String errMsg = String.format(PENDING_TIMEOUT_ERROR_MSG_FORMAT,
                            Math.min(queryQueuePendingTimeout, queryTimeout),
                            timeoutVar);
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
                context.auditEventBuilder.setPendingTimeMs(System.currentTimeMillis() - startMs);
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
        long queryTimeoutSecond = coord.getJobSpec().getQueryOptions().getQuery_timeout();
        long expiredPendingTimeMs =
                nowMs + Math.min(GlobalVariable.getQueryQueuePendingTimeoutSecond(), queryTimeoutSecond) * 1000L;
        long expiredAllocatedTimeMs = nowMs + queryTimeoutSecond * 1000L;

        int numFragments = coord.getFragments().size();
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

}
