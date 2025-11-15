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


package com.staros.heartbeat;

import com.staros.util.AbstractServer;
import com.staros.util.Config;
import com.staros.util.LogUtils;
import com.staros.util.Utils;
import com.staros.worker.Worker;
import com.staros.worker.WorkerManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class HeartbeatManager extends AbstractServer  {
    private static final Logger LOG = LogManager.getLogger(HeartbeatManager.class);

    private final int heartbeatInterval;
    private final WorkerManager workerManager;
    private ScheduledThreadPoolExecutor executors;

    public HeartbeatManager(WorkerManager workerManager) {
        int interval = Config.WORKER_HEARTBEAT_INTERVAL_SEC;
        if (interval < 1 || interval > 60) {
            LOG.warn("worker heartbeat interval {} is not suitable, change it to default 10.", interval);
            interval = 10;
        }
        this.heartbeatInterval = interval;
        this.workerManager = workerManager;
    }

    @Override
    public void doStart() {
        this.executors = new ScheduledThreadPoolExecutor(1, Utils.namedThreadFactory("starmgr-heartbeatmgr"));
        this.executors.setMaximumPoolSize(1);
        this.executors.execute(this::runOnceHeartbeatCheck);
    }

    @Override
    public void doStop() {
        // force stop all active tasks
        executors.shutdownNow();
        Utils.shutdownExecutorService(executors);
    }

    // adjust executor threads according to the number of workers
    // Some expected data on thread numbers: (interval = 10, timeout = 2)
    // - 10 workers: threadPoolSize = 4
    // - 64 workers: threadPoolSize = 19
    // - 128 workers: threadPoolSize = 39
    // - 256 workers: threadPoolSize = 78
    private void adjustExecutorThreads(int nWorkers) {
        // In the worse case, every other workers are all timeout but one alive,
        // ensure that the worker can still have a chance to get a heartbeat rpc in the single `heartbeatInterval`.
        int singleRound = Integer.max(1, heartbeatInterval / Config.WORKER_HEARTBEAT_GRPC_RPC_TIME_OUT_SEC);
        int minRange = nWorkers / singleRound + 1;
        int maxRange = minRange * 2;
        int nCoreThreads = executors.getCorePoolSize();
        int expected = (minRange + maxRange) / 2; // minRange * 1.5
        if (nCoreThreads < minRange || nCoreThreads > maxRange) {
            LOG.info("Adjust heartbeatManager ThreadPool size from {} to {}", nCoreThreads, expected);
            Utils.adjustFixedThreadPoolExecutors(executors, expected);
        }
    }

    private void runOnceHeartbeatCheck() {
        if (!isRunning()) {
            return;
        }

        try {
            LOG.debug("running heartbeat once.");
            List<Long> allWorkerIds = workerManager.getAllWorkerIds();
            adjustExecutorThreads(allWorkerIds.size());
            for (long id : allWorkerIds) {
                Worker worker = workerManager.getWorker(id);
                if (worker == null) {
                    continue;
                }
                // TODO: 1. send epoch, which should be obtained from star manager ha module
                // TODO: 2. Random delay of each worker's heartbeat RPC, avoid all the workers RPC requests rushing together
                this.executors.execute(() -> workerManager.doWorkerHeartbeat(id));
            }
        } catch (Exception exception) {
            LOG.warn("Fail to submit tasks to executor. error: ", exception);
        }

        try { // schedule next running
            this.executors.schedule(this::runOnceHeartbeatCheck, heartbeatInterval, TimeUnit.SECONDS);
        } catch (Exception exception) {
            if (isRunning()) {
                LogUtils.fatal(LOG, "Fail to schedule next round of worker heartbeat check, error: {}", exception);
            } else {
                LOG.info("Fail to schedule next round of worker heartbeat check because heartbeat manager is shutting down.");
            }
        }
    }
}
