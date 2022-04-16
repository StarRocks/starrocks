// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.mv;


import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.starrocks.catalog.Catalog;
import com.starrocks.common.util.QueryableReentrantLock;
import com.starrocks.common.util.Util;
import com.starrocks.statistic.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.starrocks.mv.MaterializedViewRefreshJob.DEFAULT_UNASSIGNED_ID;


public class MaterializedViewJobScheduler {

    private static final Logger LOG = LogManager.getLogger(MaterializedViewJobScheduler.class);

    // first long is mvTableId
    Map<Long, Queue<MaterializedViewRefreshJob>> pendingJobMap;
    // first long is mvTableId, for each mv only support 1 running job currently
    Map<Long, MaterializedViewRefreshJob> runningJobMap;
    // first long is scheduledId, manage generate periodicity tasks, only support 1 schedule currently
    Map<Long, MaterializedViewSchedulerInfo> periodScheduledManager;
    Deque<MaterializedViewRefreshJob> jobHistory;
    private final ScheduledExecutorService periodScheduled = Executors.newScheduledThreadPool(1);
    private final QueryableReentrantLock lock;


    public MaterializedViewJobScheduler() {
        pendingJobMap = Maps.newConcurrentMap();
        runningJobMap = Maps.newConcurrentMap();
        periodScheduledManager = Maps.newConcurrentMap();
        jobHistory = Queues.newLinkedBlockingDeque();
        lock = new QueryableReentrantLock(true);

        // for dispatch tasks
        ScheduledExecutorService dispatchTaskScheduler = Executors.newScheduledThreadPool(1);
        dispatchTaskScheduler.scheduleAtFixedRate(() -> {
            if (!tryLock()) {
                return;
            }
            try {
                // check if a running job is complete and remove it from running map
                Iterator<Long> runningIterator = runningJobMap.keySet().iterator();
                while (runningIterator.hasNext()) {
                    Long mvTableId = runningIterator.next();
                    MaterializedViewRefreshJob job = runningJobMap.get(mvTableId);
                    Future<?> future = job.getFuture();
                    if (future.isDone()) {
                        job.updateStatusAfterDone();
                        runningIterator.remove();
                        jobHistory.addFirst(job);
                        // log JobIsDone
                    }
                }

                // put the pending job that can be run into running map
                Iterator<Long> pendingIterator = pendingJobMap.keySet().iterator();
                while (pendingIterator.hasNext()) {
                    Long mvTableId = pendingIterator.next();
                    MaterializedViewRefreshJob runningJob = runningJobMap.get(mvTableId);
                    if (runningJob == null) {
                        Queue<MaterializedViewRefreshJob> jobQueue = pendingJobMap.get(mvTableId);
                        if (jobQueue.size() == 0) {
                            pendingIterator.remove();
                        } else {
                            MaterializedViewRefreshJob pendingJob = jobQueue.poll();
                            pendingJob.setStatus(Constants.MaterializedViewJobStatus.RUNNING);
                            runningJobMap.put(mvTableId, pendingJob);
                            Catalog.getCurrentCatalog().getMaterializedViewJobManager().addRefreshJob(pendingJob);
                            // log addRefreshJob
                        }
                    }
                }
            } catch (Exception ex) {
                LOG.warn("failed to dispatch job.", ex);
            } finally {
                unlock();
            }

        }, 0, 1, TimeUnit.SECONDS);
    }

    public Long registerScheduledJob(MaterializedViewRefreshJobBuilder builder,
                                     LocalDateTime startTime, Long period, TimeUnit timeUnit) {
        Duration duration = Duration.between(LocalDateTime.now(), startTime);
        long initialDelay = duration.getSeconds();
        ScheduledFuture<?> future = periodScheduled.scheduleAtFixedRate(() -> addPendingJob(builder.build()),
                initialDelay, period, timeUnit);
        MaterializedViewSchedulerInfo info = new MaterializedViewSchedulerInfo(startTime, period, timeUnit);
        long scheduledId = Catalog.getCurrentCatalog().getNextId();
        info.setId(scheduledId);
        info.setFuture(future);
        periodScheduledManager.put(scheduledId, info);
        // log registerScheduledJob
        return scheduledId;
    }

    public void deregisterScheduledJob(Long scheduledId) {
        MaterializedViewSchedulerInfo info = periodScheduledManager.get(scheduledId);
        ScheduledFuture<?> future = info.getFuture();
        future.cancel(true);
        // log deregisterScheduledJob
    }

    public boolean addPendingJob(MaterializedViewRefreshJob job) {
        long oldId = job.getId();
        if (oldId != DEFAULT_UNASSIGNED_ID) {
            return false;
        }
        long jobId = Catalog.getCurrentCatalog().getNextId();
        job.setId(jobId);
        long mvTableId = job.getMvTableId();
        Queue<MaterializedViewRefreshJob> jobQueue = pendingJobMap.get(mvTableId);
        if (jobQueue == null) {
            jobQueue = Queues.newConcurrentLinkedQueue();
            jobQueue.offer(job);
            pendingJobMap.put(mvTableId, jobQueue);
        } else {
            mergeOrOfferJob(jobQueue, job);
        }
        // log addPendingJob
        return true;
    }

    // For tasks manually created by users, we do not merge jobs by default.
    // For tasks that are automatically create by the system duo to create
    // materialized view or time-triggered tasks. Since it is currently a full
    // refresh, we merge this tasks and update mergeCount field.
    private void mergeOrOfferJob(Queue<MaterializedViewRefreshJob> jobQueue, MaterializedViewRefreshJob job) {
        if (job.getTriggerType() == Constants.MaterializedViewTriggerType.MANUAL) {
            jobQueue.offer(job);
        } else {
            boolean isMerged = false;
            for (MaterializedViewRefreshJob mayCanMergeJob : jobQueue) {
                // auto trigger task is full refresh currently
                if (mayCanMergeJob.getTriggerType() == Constants.MaterializedViewTriggerType.AUTO) {
                    mayCanMergeJob.incrementMergeCount();
                    isMerged = true;
                }
            }
            if (!isMerged) {
                jobQueue.offer(job);
            }
        }
    }


    public boolean cancelJob(Long mvTableId, Long jobId) {
        if (!lock.tryLock()) {
            return false;
        }
        try {
            // cancel running job
            MaterializedViewRefreshJob runningJob = runningJobMap.get(mvTableId);
            if (runningJob != null && runningJob.getId() == jobId) {
                Future<?> future = runningJob.getFuture();
                future.cancel(true);
                runningJob.setStatus(Constants.MaterializedViewJobStatus.CANCELED);
                runningJobMap.remove(mvTableId);
                jobHistory.addFirst(runningJob);
                // log cancelJob
                return true;
            }
            // cancel pending job
            Queue<MaterializedViewRefreshJob> pendingJobQueue = pendingJobMap.get(mvTableId);
            if (pendingJobQueue != null) {
                Iterator<MaterializedViewRefreshJob> queueIter = pendingJobQueue.iterator();
                while (queueIter.hasNext()) {
                    MaterializedViewRefreshJob pendingJob = queueIter.next();
                    if (pendingJob.getId() == jobId) {
                        Future<?> future = pendingJob.getFuture();
                        if (future != null) {
                            future.cancel(true);
                        }
                        pendingJob.setStatus(Constants.MaterializedViewJobStatus.CANCELED);
                        jobHistory.addFirst(pendingJob);
                        queueIter.remove();
                        // log cancelJob
                        return true;
                    }
                }
            }
        } finally {
            unlock();
        }
        return false;
    }

    public List<MaterializedViewRefreshJob> listJob() {
        List<MaterializedViewRefreshJob> jobList = Lists.newArrayList();
        for (Queue<MaterializedViewRefreshJob> pJobs : pendingJobMap.values()) {
            jobList.addAll(pJobs);
        }
        jobList.addAll(runningJobMap.values());
        jobList.addAll(jobHistory);
        return jobList;
    }

    private boolean tryLock() {
        try {
            if (!lock.tryLock(1, TimeUnit.SECONDS)) {
                Thread owner = lock.getOwner();
                if (owner != null) {
                    LOG.warn("materialized view lock is held by: {}", Util.dumpThread(owner, 50));
                } else {
                    LOG.warn("materialized view lock owner is null");
                }
            }
            return true;
        } catch (InterruptedException e) {
            LOG.warn("got exception while getting materialized view lock", e);
        }
        return lock.isHeldByCurrentThread();
    }

    private void unlock() {
        this.lock.unlock();
    }
}
