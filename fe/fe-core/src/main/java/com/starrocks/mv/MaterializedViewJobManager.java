// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.mv;


import com.starrocks.statistic.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class MaterializedViewJobManager {

    private static final Logger LOG = LogManager.getLogger(MaterializedViewJobManager.class);

    // job id -> MaterializedViewRefreshJob
    private final Map<Long, MaterializedViewRefreshJob> mvRefreshJobs;
    // pool for execute actual task no limit
    private final ExecutorService refreshTaskPool = Executors.newCachedThreadPool();

    public MaterializedViewJobManager() {
        this.mvRefreshJobs = new ConcurrentHashMap<>();
    }

    public void addRefreshJob(MaterializedViewRefreshJob job) {
        if (job == null) {
            return;
        }

        if (job.getStatus() == Constants.MaterializedViewJobStatus.SUCCESS) {
            return;
        }

        if (mvRefreshJobs.containsKey(job.getDbId())) {
            return;
        }

        job.generateTasks();
        Future<?> future = refreshTaskPool.submit(() -> {
            try {
                job.setStatus(Constants.MaterializedViewJobStatus.RUNNING);
                job.setStartTime(LocalDateTime.now());
                job.runTasks();
                job.setEndTime(LocalDateTime.now());
            } catch (Exception ex) {
                LOG.warn("failed to run task.", ex);
            }
        });
        job.setFuture(future);
        mvRefreshJobs.put(job.getId(), job);
    }

}
