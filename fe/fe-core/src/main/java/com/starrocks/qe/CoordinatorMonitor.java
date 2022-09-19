// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.qe;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Queues;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.proto.PPlanFragmentCancelReason;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * CoordinatorMonitor gets notified when some backends become dead and cancels coordinates related to them.
 */
public class CoordinatorMonitor {
    private static final Logger LOG = LogManager.getLogger(CoordinatorMonitor.class);

    private static class SingletonHolder {
        private static final CoordinatorMonitor INSTANCE = new CoordinatorMonitor();
    }

    private static final int COMING_DEAD_BACKEND_QUEUE_CAPACITY = 1_000_000;

    private final BlockingQueue<Long> comingDeadBackendIDQueue;
    private final AtomicBoolean started;
    private final DeadBackendAndComputeNodeChecker checker;

    public CoordinatorMonitor() {
        comingDeadBackendIDQueue = Queues.newLinkedBlockingDeque(COMING_DEAD_BACKEND_QUEUE_CAPACITY);
        started = new AtomicBoolean(false);
        checker = new DeadBackendAndComputeNodeChecker();
    }

    public static CoordinatorMonitor getInstance() {
        return SingletonHolder.INSTANCE;
    }

    public boolean addDeadBackend(Long backendID) {
        return comingDeadBackendIDQueue.offer(backendID);
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            checker.start();
        }
    }

    private class DeadBackendAndComputeNodeChecker extends Thread {
        @Override
        public void run() {
            List<Long> deadBackendIDs = Lists.newArrayList();
            Long backendID;
            for (; ; ) {
                try {
                    backendID = comingDeadBackendIDQueue.take();
                    deadBackendIDs.add(backendID);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                // Try to non-blocking take all the backend IDs from queue.
                while ((backendID = comingDeadBackendIDQueue.poll()) != null) {
                    deadBackendIDs.add(backendID);
                }

                final List<Coordinator> coordinators = QeProcessorImpl.INSTANCE.getCoordinators();
                for (Coordinator coord : coordinators) {
                    boolean isUsingDeadBackend = deadBackendIDs.stream().anyMatch(coord::isUsingBackend);
                    if (isUsingDeadBackend) {
                        if (LOG.isWarnEnabled()) {
                            LOG.warn("Cancel query [{}], because some related backend is not alive",
                                    DebugUtil.printId(coord.getQueryId()));
                        }
                        coord.cancel(PPlanFragmentCancelReason.INTERNAL_ERROR,
                                "Backend not found. Check if any backend is down or not");
                    }
                }

                deadBackendIDs.clear();
            }
        }
    }

}
