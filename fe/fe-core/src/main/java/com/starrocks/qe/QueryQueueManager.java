// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.qe;

import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.metric.MetricRepo;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SchemaScanNode;
import com.starrocks.qe.scheduler.RecoverableException;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TWorkGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
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

    public void maybeWait(ConnectContext context, Coordinator coord) throws UserException, InterruptedException {
        if (!needCheckQueue(coord) || !isEnableQueue(coord)) {
            return;
        }

        long startMs = System.currentTimeMillis();
        boolean isPending = false;
        try {
            LogicalSlot slotRequirement = createSlot(coord);
            coord.setSlot(slotRequirement);

            isPending = true;
            context.setPending(true);
            MetricRepo.COUNTER_QUERY_QUEUE_PENDING.increase(1L);
            MetricRepo.COUNTER_QUERY_QUEUE_TOTAL.increase(1L);

            long timeoutMs = slotRequirement.getExpiredPendingTimeMs();
            LogicalSlot allocatedSlot = null;
            while (allocatedSlot == null) {
                // Check timeout.
                long currentMs = System.currentTimeMillis();
                if (currentMs >= timeoutMs) {
                    MetricRepo.COUNTER_QUERY_QUEUE_TIMEOUT.increase(1L);
                    GlobalStateMgr.getCurrentState().getSlotProvider().cancelSlotRequirement(slotRequirement);
                    String errMsg = String.format(PENDING_TIMEOUT_ERROR_MSG_FORMAT,
                            GlobalVariable.getQueryQueuePendingTimeoutSecond(),
                            GlobalVariable.QUERY_QUEUE_PENDING_TIMEOUT_SECOND);
                    throw new UserException(errMsg);
                }

                Future<LogicalSlot> slotFuture = GlobalStateMgr.getCurrentState().getSlotProvider().requireSlot(slotRequirement);

                // Wait for slot allocated.
                try {
                    allocatedSlot = slotFuture.get(timeoutMs - currentMs, TimeUnit.MILLISECONDS);
                } catch (ExecutionException e) {
                    LOG.warn("[Slot] failed to allocate resource to query [slot={}]", slotRequirement, e);
                    if (e.getCause() instanceof RecoverableException) {
                        continue;
                    }
                    throw new UserException("Failed to allocate resource to query: " + e.getMessage(), e);
                } catch (TimeoutException e) {
                    // Check timeout in the next loop.
                } catch (CancellationException e) {
                    throw new UserException("Cancelled");
                }
            }
        } finally {
            if (isPending) {
                context.auditEventBuilder.setPendingTimeMs(System.currentTimeMillis() - startMs);
                MetricRepo.COUNTER_QUERY_QUEUE_PENDING.increase(-1L);
                context.setPending(false);
            }
        }
    }

    public boolean isEnableQueue(Coordinator coord) {
        if (coord.isStatisticsJob()) {
            return GlobalVariable.isEnableQueryQueueStatistic();
        }

        if (coord.isLoadType()) {
            return GlobalVariable.isEnableQueryQueueLoad();
        }

        return GlobalVariable.isEnableQueryQueueSelect();
    }

    public boolean needCheckQueue(Coordinator coord) {
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
        long expiredPendingTimeMs =
                nowMs + Math.min(GlobalVariable.getQueryQueuePendingTimeoutSecond(), queryTimeoutSecond) * 1000L;
        long expiredAllocatedTimeMs = nowMs + queryTimeoutSecond * 1000L;

        return new LogicalSlot(coord.getQueryId(), frontend.getNodeName(), groupId, 1, expiredPendingTimeMs,
                expiredAllocatedTimeMs, frontend.getStartTime());
    }
}
