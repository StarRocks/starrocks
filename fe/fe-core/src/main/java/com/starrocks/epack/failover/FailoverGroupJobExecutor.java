// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.epack.failover.job.FailoverGroupJob;
import com.starrocks.replication.ReplicationJob;
import com.starrocks.replication.ReplicationJobState;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

public class FailoverGroupJobExecutor {
    private static final Logger LOG = LogManager.getLogger(FailoverGroupJobExecutor.class);

    private final Set<FailoverGroupJob> failoverGroupJobs = Sets.newConcurrentHashSet();

    private final Map<Long, ReplicationJob> runningReplicationJobs = Maps.newConcurrentMap();
    private final Map<Long, ReplicationJob> committedReplicationJobs = Maps.newConcurrentMap();
    private final Map<Long, ReplicationJob> abortedReplicationJobs = Maps.newConcurrentMap();

    private final ThreadPoolExecutor threadPoolExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
            Config.failover_group_job_threads, Integer.MAX_VALUE, "failover_group_job", true);

    public boolean addFailoverGroupJob(FailoverGroupJob job) {
        if (!failoverGroupJobs.add(job)) {
            return false;
        }

        threadPoolExecutor.execute(job);
        return true;
    }

    public boolean removeFailoverGroupJob(FailoverGroupJob job) {
        return failoverGroupJobs.remove(job);
    }

    public boolean addReplicationJob(ReplicationJob job) {
        if (committedReplicationJobs.containsKey(job.getTableId())) {
            return false;
        }

        if (runningReplicationJobs.putIfAbsent(job.getTableId(), job) != null) {
            return false;
        }

        try {
            GlobalStateMgr.getServingState().getReplicationMgr().addReplicationJob(job);
        } catch (Exception e) {
            runningReplicationJobs.remove(job.getTableId());
            abortedReplicationJobs.put(job.getTableId(), job);
            LOG.warn("Failed to add replication job, will retry, exception: ", e);
            return false;
        }

        abortedReplicationJobs.remove(job.getTableId());
        return true;
    }

    public boolean isAllJobsFinished() {
        List<ReplicationJob> toRemovedJobs = Lists.newArrayList();
        for (ReplicationJob job : runningReplicationJobs.values()) {
            ReplicationJobState state = job.getState();
            if (state.equals(ReplicationJobState.COMMITTED)) {
                toRemovedJobs.add(job);
                committedReplicationJobs.put(job.getTableId(), job);
            } else if (state.equals(ReplicationJobState.ABORTED)) {
                toRemovedJobs.add(job);
                abortedReplicationJobs.put(job.getTableId(), job);
            }
        }

        for (ReplicationJob job : toRemovedJobs) {
            runningReplicationJobs.remove(job.getTableId());
        }

        return failoverGroupJobs.isEmpty() && runningReplicationJobs.isEmpty();
    }

    public boolean hasFailedJobs() {
        return !abortedReplicationJobs.isEmpty();
    }

    public void clear() {
        for (FailoverGroupJob job : failoverGroupJobs) {
            job.cancel();
        }
        failoverGroupJobs.clear();
        for (ReplicationJob job : runningReplicationJobs.values()) {
            job.cancel();
        }
        runningReplicationJobs.clear();
        committedReplicationJobs.clear();
        abortedReplicationJobs.clear();
    }
}
