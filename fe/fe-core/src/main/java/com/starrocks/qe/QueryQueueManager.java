// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.qe;

import com.google.common.base.Preconditions;
import com.starrocks.common.UserException;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.MysqlTableSink;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.ResultSink;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SchemaScanNode;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
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

    public boolean updateResourceUsage(long backendId, int numRunningQueries, long memLimitBytes, long memUsedBytes,
                                       int cpuUsedPermille) {
        Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(backendId);
        if (backend == null) {
            LOG.warn("backend doesn't exist. id: {}", backendId);
            return false;
        }

        try {
            lock.lock();

            boolean isChanged =
                    backend.updateResourceUsage(numRunningQueries, memLimitBytes, memUsedBytes, cpuUsedPermille);
            if (isChanged) {
                LOG.debug("resource usage from backend {} has changed", backendId);
                maybeNotifyAfterLock();
            }
            return isChanged;
        } finally {
            lock.unlock();
        }
    }

    public void maybeWait(ConnectContext connectCtx, Coordinator coord) throws UserException, InterruptedException {
        if (!needCheckQueue(coord) || canRunMore()) {
            return;
        }

        long startMs = System.currentTimeMillis();
        long timeoutMs = startMs + GlobalVariable.getQueryQueuePendingTimeoutSecond() * 1000L;
        PendingQueryInfo info = new PendingQueryInfo(connectCtx, lock, coord);

        try {
            lock.lock();
            if (!needCheckQueue(coord) || canRunMore()) {
                return;
            }

            if (!canQueueMore()) {
                throw new UserException("Need be queued but exceed query queue capacity");
            }
            info.connectCtx.setPending(true);
            pendingQueryInfoMap.put(info.connectCtx, info);

            while (!canRunMore()) {
                long currentMs = System.currentTimeMillis();
                if (currentMs >= timeoutMs) {
                    throw new UserException("Pending timeout");
                }

                boolean timeout = !info.await(timeoutMs - currentMs, TimeUnit.MILLISECONDS);
                if (timeout) {
                    throw new UserException("Pending timeout");
                }

                if (info.isCancelled) {
                    throw new UserException("Cancelled");
                }
            }
        } finally {
            info.connectCtx.auditEventBuilder.setPendingTimeMs(System.currentTimeMillis() - startMs);
            info.connectCtx.setPending(false);
            pendingQueryInfoMap.remove(info.connectCtx);

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

    // For test.
    public void maybeNotify() {
        try {
            lock.lock();
            maybeNotifyAfterLock();
        } finally {
            lock.unlock();
        }
    }

    public boolean needCheckQueue(Coordinator coord) {
        if (coord.isLoadType()) {
            return false;
        }

        // The queries only using schema meta will never been queued, because a MySQL client will
        // query schema meta after the connection is established.
        List<ScanNode> scanNodes = coord.getScanNodes();
        boolean notNeed = scanNodes.isEmpty() || scanNodes.stream().allMatch(SchemaScanNode.class::isInstance);
        if (notNeed) {
            return false;
        }

        DataSink sink = coord.getFragments().get(0).getSink();
        if (sink instanceof OlapTableSink || sink instanceof MysqlTableSink) {
            return GlobalVariable.isQueryQueueInsertEnable();
        } else if (sink instanceof ResultSink) {
            ResultSink resultSink = (ResultSink) sink;
            if (resultSink.isQuerySink()) {
                return GlobalVariable.isQueryQueueSelectEnable();
            } else if (resultSink.isStatisticSink()) {
                return GlobalVariable.isQueryQueueStatisticEnable();
            }
        }

        return false;
    }

    public boolean canRunMore() {
        return GlobalStateMgr.getCurrentSystemInfo().getBackends().stream()
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
