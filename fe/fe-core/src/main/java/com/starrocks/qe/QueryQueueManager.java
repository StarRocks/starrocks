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

import com.google.common.base.Preconditions;
import com.starrocks.common.UserException;
import com.starrocks.metric.MetricRepo;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.ResultSink;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SchemaScanNode;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class QueryQueueManager {
    private static final Logger LOG = LogManager.getLogger(QueryQueueManager.class);

    private static class PendingQueryInfo {
        private final Coordinator coordinator;
        private final ConnectContext connectCtx;
        private final ReentrantLock lock;
        private final Condition condition;
        private boolean isCancelled = false;

        private PendingQueryInfo(ConnectContext connectCtx, ReentrantLock lock, Coordinator coordinator) {
            Preconditions.checkState(connectCtx != null);
            this.coordinator = coordinator;
            this.connectCtx = connectCtx;
            this.lock = lock;
            this.condition = this.lock.newCondition();
        }

        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            Preconditions.checkState(lock.isHeldByCurrentThread());
            return condition.await(timeout, unit);
        }

        public void signalAfterLock() {
            Preconditions.checkState(lock.isHeldByCurrentThread());
            condition.signal();
        }

        public void cancelAfterLock() {
            Preconditions.checkState(lock.isHeldByCurrentThread());
            isCancelled = true;
            signalAfterLock();
        }
    }

    private static class SingletonHolder {
        private static final QueryQueueManager INSTANCE = new QueryQueueManager();
    }

    public static QueryQueueManager getInstance() {
        return QueryQueueManager.SingletonHolder.INSTANCE;
    }

    private static final long CHECK_INTERVAL_MS = 1000L;

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<ConnectContext, PendingQueryInfo> pendingQueryInfoMap = new ConcurrentHashMap<>();

    public void cancelQuery(ConnectContext connectCtx) {
        if (connectCtx == null) {
            return;
        }

        try {
            lock.lock();

            PendingQueryInfo queryInfo = pendingQueryInfoMap.get(connectCtx);
            if (queryInfo != null) {
                queryInfo.cancelAfterLock();
            }
        } finally {
            lock.unlock();
        }
    }

    public void updateResourceUsage(long backendId, int numRunningQueries, long memLimitBytes, long memUsedBytes,
                                    int cpuUsedPermille) {
        ComputeNode node = GlobalStateMgr.getCurrentSystemInfo().getBackendOrComputeNode(backendId);
        if (node == null) {
            LOG.warn("backend or computed node doesn't exist. id: {}", backendId);
            return;
        }

        try {
            lock.lock();

            node.updateResourceUsage(numRunningQueries, memLimitBytes, memUsedBytes, cpuUsedPermille);
            maybeNotifyAfterLock();
        } finally {
            lock.unlock();
        }
    }

    public void maybeWait(ConnectContext connectCtx, Coordinator coord) throws UserException, InterruptedException {
        if (!needCheckQueue(coord)) {
            return;
        }
        if (!enableCheckQueue(coord) || canRunMore()) {
            return;
        }

        long startMs = System.currentTimeMillis();
        long timeoutMs;
        PendingQueryInfo info = new PendingQueryInfo(connectCtx, lock, coord);
        boolean isPending = false;

        try {
            lock.lock();
            if (!enableCheckQueue(coord) || canRunMore()) {
                return;
            }

            if (!canQueueMore()) {
                throw new UserException("Need be queued but exceed query queue capacity");
            }

            isPending = true;
            info.connectCtx.setPending(true);
            pendingQueryInfoMap.put(info.connectCtx, info);
            MetricRepo.COUNTER_QUERY_QUEUE_PENDING.increase(1L);
            MetricRepo.COUNTER_QUERY_QUEUE_TOTAL.increase(1L);

            while (enableCheckQueue(coord) && !canRunMore()) {
                timeoutMs = startMs + GlobalVariable.getQueryQueuePendingTimeoutSecond() * 1000L;
                long currentMs = System.currentTimeMillis();
                if (currentMs >= timeoutMs) {
                    MetricRepo.COUNTER_QUERY_QUEUE_TIMEOUT.increase(1L);
                    throw new UserException("Pending timeout");
                }

                info.await(Math.min(timeoutMs - currentMs, CHECK_INTERVAL_MS), TimeUnit.MILLISECONDS);

                if (info.isCancelled) {
                    throw new UserException("Cancelled");
                }
            }
        } finally {
            if (isPending) {
                info.connectCtx.auditEventBuilder.setPendingTimeMs(System.currentTimeMillis() - startMs);
                MetricRepo.COUNTER_QUERY_QUEUE_PENDING.increase(-1L);
                pendingQueryInfoMap.remove(info.connectCtx);
                info.connectCtx.setPending(false);
            }

            lock.unlock();
        }
    }

    // Public for test.
    public void maybeNotifyAfterLock() {
        Preconditions.checkState(lock.isHeldByCurrentThread());

        if (pendingQueryInfoMap.isEmpty()) {
            return;
        }
        if (canRunMore()) {
            for (PendingQueryInfo queryInfo : pendingQueryInfoMap.values()) {
                queryInfo.signalAfterLock();
            }
        }
    }

    public void maybeNotify() {
        try {
            lock.lock();
            maybeNotifyAfterLock();
        } finally {
            lock.unlock();
        }
    }

    public boolean enableCheckQueue(Coordinator coord) {
        if (coord.isLoadType()) {
            return GlobalVariable.isEnableQueryQueueLoad();
        }

        DataSink sink = coord.getFragments().get(0).getSink();
        if (sink instanceof ResultSink) {
            ResultSink resultSink = (ResultSink) sink;
            if (resultSink.isQuerySink()) {
                return GlobalVariable.isEnableQueryQueueSelect();
            } else if (resultSink.isStatisticSink()) {
                return GlobalVariable.isEnableQueryQueueStatistic();
            }
        }

        return false;
    }

    public boolean needCheckQueue(Coordinator coord) {
        // The queries only using schema meta will never been queued, because a MySQL client will
        // query schema meta after the connection is established.
        List<ScanNode> scanNodes = coord.getScanNodes();
        boolean notNeed = scanNodes.isEmpty() || scanNodes.stream().allMatch(SchemaScanNode.class::isInstance);
        return !notNeed;
    }

    public boolean canRunMore() {
        return GlobalStateMgr.getCurrentSystemInfo().backendAndComputeNodeStream()
                .noneMatch(ComputeNode::isResourceOverloaded);
    }

    private boolean canQueueMore() {
        return !GlobalVariable.isQueryQueueMaxQueuedQueriesEffective() ||
                pendingQueryInfoMap.size() < GlobalVariable.getQueryQueueMaxQueuedQueries();
    }

    public int numPendingQueries() {
        return pendingQueryInfoMap.size();
    }

}
