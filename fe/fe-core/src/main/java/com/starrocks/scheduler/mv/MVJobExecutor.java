// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler.mv;

import com.starrocks.common.util.LeaderDaemon;
import org.apache.commons.collections.CollectionUtils;
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
 * <p>
 * TODO(murphy) extend executor to multi-threading
 */
public class MVJobExecutor extends LeaderDaemon {
    private static final long EXECUTOR_INTERVAL_MILLIS = 1000;
    private static final Logger LOG = LogManager.getLogger(MVJobExecutor.class);

    public MVJobExecutor() {
        super("MV Job Executor", EXECUTOR_INTERVAL_MILLIS);
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            runImpl();
        } catch (Throwable e) {
            LOG.error("Failed to run the MVJobExecutor ", e);
        }
    }

    private void runImpl() {
        List<MVMaintenanceJob> jobs = MVManager.getInstance().getRunnableJobs();
        if (CollectionUtils.isEmpty(jobs)) {
            return;
        }
        long startMillis = System.currentTimeMillis();

        for (MVMaintenanceJob job : jobs) {
            if (!job.isRunnable()) {
                LOG.warn("Job {} is in {} state, skip it", job, job.getState());
                continue;
            }
            try {
                job.onSchedule();
            } catch (Exception e) {
                LOG.warn("[MVJobExecutor] execute job got exception", e);
            }
        }

        String mvNameList = jobs.stream().map(x -> x.getView().getName()).collect(Collectors.joining(", "));
        long duration = System.currentTimeMillis() - startMillis;
        LOG.info("[MVJobExecutor] finish schedule batch of jobs in {}ms: {}", duration, mvNameList);
    }

}
