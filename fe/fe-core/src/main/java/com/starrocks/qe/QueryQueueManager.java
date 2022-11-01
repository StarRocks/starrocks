// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.qe;

import com.google.common.base.Preconditions;
import com.starrocks.common.UserException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class QueryQueueManager {
    private static final Logger LOG = LogManager.getLogger(QueryQueueManager.class);

    private static class PendingQueryInfo {
        private final ConnectContext connectCtx;
        private final ReentrantLock lock;
        private final Condition condition;
        private boolean isCancelled = false;

        private PendingQueryInfo(ConnectContext connectCtx, ReentrantLock lock) {
            Preconditions.checkState(connectCtx != null);
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

    // Public for test.
    public void maybeNotifyAfterLock() {
        Preconditions.checkState(lock.isHeldByCurrentThread());

        if (pendingQueryInfoMap.isEmpty()) {
            return;
        }
        if (needWait()) {
            return;
        }

        for (PendingQueryInfo queryInfo : pendingQueryInfoMap.values()) {
            queryInfo.signalAfterLock();
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

    public void maybeWait(ConnectContext connectCtx) throws UserException, InterruptedException {
        if (!GlobalVariable.isQueryQueueEnable()) {
            return;
        }

        if (!needWait()) {
            return;
        }

        long startMs = System.currentTimeMillis();
        long timeoutMs = startMs + GlobalVariable.getQueryQueuePendingTimeoutSecond() * 1000L;
        PendingQueryInfo queryInfo = new PendingQueryInfo(connectCtx, lock);

        try {
            lock.lock();

            if (GlobalVariable.isQueryQueueMaxQueuedQueriesEffective() &&
                    pendingQueryInfoMap.size() >= GlobalVariable.getQueryQueueMaxQueuedQueries()) {
                throw new UserException("Need pend but exceed query queue capacity");
            }

            queryInfo.connectCtx.setPending(true);
            pendingQueryInfoMap.put(queryInfo.connectCtx, queryInfo);

            while (needWait()) {
                long currentMs = System.currentTimeMillis();
                if (currentMs >= timeoutMs) {
                    throw new UserException("Pending timeout");
                }

                boolean timeout = !queryInfo.await(timeoutMs - currentMs, TimeUnit.MILLISECONDS);
                if (timeout) {
                    throw new UserException("Pending timeout");
                }

                if (queryInfo.isCancelled) {
                    throw new UserException("Cancelled");
                }
            }
        } finally {
            queryInfo.connectCtx.auditEventBuilder.setPendingTimeMs(System.currentTimeMillis() - startMs);
            queryInfo.connectCtx.setPending(false);
            pendingQueryInfoMap.remove(queryInfo.connectCtx);

            lock.unlock();
        }
    }

    public boolean needWait() {
        return GlobalStateMgr.getCurrentSystemInfo().getBackends().stream()
                .anyMatch(ComputeNode::isResourceOverloaded);
    }

    public int numPendingQueries() {
        return pendingQueryInfoMap.size();
    }

}
