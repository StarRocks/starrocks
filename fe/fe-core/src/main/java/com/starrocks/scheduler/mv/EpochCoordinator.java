// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler.mv;

import com.starrocks.common.FeConstants;
import com.starrocks.common.util.LeaderDaemon;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

/**
 * EpochCoordinator coordinate the epoch execution, running as daemon thread
 * 1. Begin epoch for all task executors, send information about txn/binlog/table-version
 * 2. Wait for all executors finish
 * 3. Commit the epoch if all executor succeed: commit the transaction, update the binlog consumption LSN
 * 4. Handle the failure to epoch if any executor failed
 * TODO(murphy) separate the coordinator and worker, to support concurrent execution
 */
public class EpochCoordinator extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(EpochCoordinator.class);

    public EpochCoordinator() {
        super("MV Epoch Coordinator", FeConstants.EPOCH_COORDINATOR_RUNNING_INTERVAL_MILLIS);
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            runImpl();
        } catch (Throwable e) {
            LOG.error("Failed to run the epoch coordinator: ", e);
        }
    }

    private void runImpl() {
        List<MVMaintenanceJob> jobs = MVManager.getInstance().getRunnableJobs();
        long startMillis = System.currentTimeMillis();

        for (MVMaintenanceJob job : jobs) {
            if (!job.isRunnable()) {
                LOG.warn("Job {} is in {} state, skip it", job, job.getState());
                continue;
            }
            job.onSchedule();
        }

        String mvNameList = jobs.stream().map(x -> x.getView().getName()).collect(Collectors.joining(", "));
        long duration = System.currentTimeMillis() - startMillis;
        LOG.info("[EpochCoordinator] finish schedule batch of jobs in {}ms: {}", duration, mvNameList);
    }

}
