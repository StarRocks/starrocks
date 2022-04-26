// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.mv;

import com.starrocks.statistic.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class MaterializedViewJobExecutor {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewJobExecutor.class);
    // pool for execute actual task no limit
    private final ExecutorService refreshTaskPool = Executors.newCachedThreadPool();

    public void addRefreshJob(MaterializedViewRefreshJob job) {
        if (job == null) {
            return;
        }

        if (job.getStatus() == Constants.MaterializedViewJobStatus.SUCCESS ||
                job.getStatus() == Constants.MaterializedViewJobStatus.CANCELED) {
            LOG.warn("materialized view job {} is in final status {} ", job.getId(), job.getStatus());
            return;
        }

        if (job.getRetryTime() == 0) {
            job.generateTasks();
            job.setStatus(Constants.MaterializedViewJobStatus.RUNNING);
        } else {
            job.setStatus(Constants.MaterializedViewJobStatus.RETRYING);
        }

        Future<?> future = refreshTaskPool.submit(() -> {
            try {
                job.setStartTime(LocalDateTime.now());
                job.runTasks();
                job.setEndTime(LocalDateTime.now());
            } catch (Exception ex) {
                LOG.warn("failed to run materialized view refresh task.", ex);
            }
        });
        job.setFuture(future);
    }

}
