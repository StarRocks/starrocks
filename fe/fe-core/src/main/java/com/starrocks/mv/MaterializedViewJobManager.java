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


public class MaterializedViewJobManager {

    private static final Logger LOG = LogManager.getLogger(MaterializedViewJobManager.class);
    // first long is mvTableId
    Map<Long, Queue<MaterializedViewRefreshJob>> pendingJobMap;
    // mvTableId -> running MaterializedViewRefreshJob, for each mv only support 1 running job currently
    Map<Long, MaterializedViewRefreshJob> runningJobMap;
    // first long is scheduledId, manage generate periodicity tasks, only support 1 schedule currently
    Map<Long, MaterializedViewSchedulerInfo> periodScheduledManager;
    Deque<MaterializedViewRefreshJob> jobHistory;
    // The periodScheduler is responsible for periodically checking whether the running task is completed
    // and updating the status. It is also responsible for placing pending tasks in the running queue.
    // This operation cannot be completed concurrently, so only one thread is required to lock it.
    private final ScheduledExecutorService periodScheduler = Executors.newScheduledThreadPool(1);
    // for dispatch tasks
    private final ScheduledExecutorService dispatchTaskScheduler = Executors.newScheduledThreadPool(1);
    private final QueryableReentrantLock lock;

    private final MaterializedViewJobExecutor jobExecutor;

    public MaterializedViewJobManager() {
        pendingJobMap = Maps.newConcurrentMap();
        runningJobMap = Maps.newConcurrentMap();
        periodScheduledManager = Maps.newConcurrentMap();
        jobExecutor = new MaterializedViewJobExecutor();
        jobHistory = Queues.newLinkedBlockingDeque();
        lock = new QueryableReentrantLock(true);

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
                        Constants.MaterializedViewJobStatus jobStatus = job.updateStatusAfterDone();
                        runningIterator.remove();
                        jobHistory.addFirst(job);
                        MaterializedViewRefreshJobStatusChange statusChange =
                                new MaterializedViewRefreshJobStatusChange(job.getMvTableId(), job.getId(),
                                        Constants.MaterializedViewJobStatus.RUNNING, jobStatus);
                        // Catalog.getCurrentCatalog().getEditLog().logRefreshJobStatusChange(statusChange);
                    }
                }

                // schedule the pending jobs that can be run into running map
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
                            jobExecutor.addRefreshJob(pendingJob);
                            MaterializedViewRefreshJobStatusChange statusChange =
                                    new MaterializedViewRefreshJobStatusChange(pendingJob.getMvTableId(),
                                            pendingJob.getId(), Constants.MaterializedViewJobStatus.PENDING,
                                            Constants.MaterializedViewJobStatus.PENDING);
                            // Catalog.getCurrentCatalog().getEditLog().logRefreshJobStatusChange(statusChange);
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
        ScheduledFuture<?> future = periodScheduler.scheduleAtFixedRate(() -> addPendingJob(builder.build()),
                initialDelay, period, timeUnit);
        MaterializedViewSchedulerInfo info = new MaterializedViewSchedulerInfo(startTime, period, timeUnit, builder);
        long scheduledId = Catalog.getCurrentCatalog().getNextId();
        info.setId(scheduledId);
        info.setFuture(future);
        periodScheduledManager.put(scheduledId, info);
        // Catalog.getCurrentCatalog().getEditLog().logRegisterScheduledJob(info);
        return scheduledId;
    }

    public void deregisterScheduledJob(Long scheduledId) {
        MaterializedViewSchedulerInfo info = periodScheduledManager.get(scheduledId);
        ScheduledFuture<?> future = info.getFuture();
        if (future != null) {
            future.cancel(true);
        }
        periodScheduledManager.remove(scheduledId);
        // Catalog.getCurrentCatalog().getEditLog().logDeregisterScheduledJob(scheduledId);
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
        // Catalog.getCurrentCatalog().getEditLog().logAddPendingJob(job);
        return true;
    }

    // For tasks manually created by users, we do not merge jobs by default.
    // For tasks that are automatically created by the system duo, like creating
    // materialized view or time-triggered tasks. Since it is currently a full
    // refresh, we merge this tasks and update mergeCount field.
    private void mergeOrOfferJob(Queue<MaterializedViewRefreshJob> jobQueue, MaterializedViewRefreshJob job) {
        if (job.getTriggerType() == Constants.MaterializedViewRefreshTriggerType.MANUAL) {
            jobQueue.offer(job);
        } else {
            boolean isMerged = false;
            for (MaterializedViewRefreshJob mayCanMergeJob : jobQueue) {
                // auto trigger task is full refresh currently
                if (mayCanMergeJob.getTriggerType() == Constants.MaterializedViewRefreshTriggerType.AUTO) {
                    mayCanMergeJob.incrementMergeCount();
                    isMerged = true;
                }
            }
            if (!isMerged) {
                jobQueue.offer(job);
            }
        }
    }

    public boolean retryJob(Long jobId) {
        // Generally the retried tasks are the most recent tasks
        Iterator<MaterializedViewRefreshJob> jobIter = jobHistory.iterator();
        while (jobIter.hasNext()) {
            MaterializedViewRefreshJob job = jobIter.next();
            if (job.getId() == jobId) {
                if (!(job.getStatus() == Constants.MaterializedViewJobStatus.FAILED ||
                        job.getStatus() == Constants.MaterializedViewJobStatus.PARTIAL_SUCCESS)) {
                    return false;
                }
                job.setStatus(Constants.MaterializedViewJobStatus.PENDING);
                long mvTableId = job.getMvTableId();
                job.incrementRetryTime();
                Queue<MaterializedViewRefreshJob> jobQueue = pendingJobMap.get(mvTableId);
                if (jobQueue == null) {
                    jobQueue = Queues.newConcurrentLinkedQueue();
                    jobQueue.offer(job);
                    pendingJobMap.put(mvTableId, jobQueue);
                } else {
                    jobQueue.offer(job);
                }
                jobIter.remove();
                return true;
            }
        }
        return false;
    }

    public boolean cancelJob(Long mvTableId, Long jobId) {
        if (!tryLock()) {
            return false;
        }
        try {
            // cancel running job
            MaterializedViewRefreshJob runningJob = runningJobMap.get(mvTableId);
            if (runningJob != null && runningJob.getId() == jobId) {
                Future<?> future = runningJob.getFuture();
                boolean isCanceled = future.cancel(true);
                if (!isCanceled) {
                    return false;
                }
                runningJob.setStatus(Constants.MaterializedViewJobStatus.CANCELED);
                runningJobMap.remove(mvTableId);
                jobHistory.addFirst(runningJob);
                MaterializedViewRefreshJobStatusChange statusChange =
                        new MaterializedViewRefreshJobStatusChange(mvTableId,
                                jobId, Constants.MaterializedViewJobStatus.RUNNING,
                                Constants.MaterializedViewJobStatus.CANCELED);
                // Catalog.getCurrentCatalog().getEditLog().logRefreshJobStatusChange(statusChange);
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
                        MaterializedViewRefreshJobStatusChange statusChange =
                                new MaterializedViewRefreshJobStatusChange(mvTableId,
                                        jobId, Constants.MaterializedViewJobStatus.PENDING,
                                        Constants.MaterializedViewJobStatus.CANCELED);
                        // Catalog.getCurrentCatalog().getEditLog().logRefreshJobStatusChange(statusChange);
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
                return false;
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

    public void replayRegisterScheduledJob(MaterializedViewSchedulerInfo info) {
        periodScheduledManager.put(info.getId(), info);
    }

    public void replayDeregisterScheduledJob(long scheduledId) {
        periodScheduledManager.remove(scheduledId);
    }

    public void replayAddPendingJob(MaterializedViewRefreshJob job) {
        Queue<MaterializedViewRefreshJob> jobQueue = pendingJobMap.get(job.getId());
        if (jobQueue == null) {
            jobQueue = Queues.newConcurrentLinkedQueue();
            jobQueue.offer(job);
            pendingJobMap.put(job.getMvTableId(), jobQueue);
        } else {
            mergeOrOfferJob(jobQueue, job);
        }
    }

    public void replayJobStatusChange(MaterializedViewRefreshJobStatusChange statusChange) {
        Constants.MaterializedViewJobStatus fromStatus = statusChange.getFromStatus();
        Constants.MaterializedViewJobStatus toStatus = statusChange.getToStatus();
        long mvTableId = statusChange.getMvTableId();
        if (fromStatus == Constants.MaterializedViewJobStatus.PENDING) {
            if (toStatus == Constants.MaterializedViewJobStatus.RUNNING) {
                Queue<MaterializedViewRefreshJob> jobQueue = pendingJobMap.get(mvTableId);
                if (jobQueue != null && jobQueue.size() != 0) {
                    MaterializedViewRefreshJob pendingJob = jobQueue.poll();
                    pendingJob.setStatus(Constants.MaterializedViewJobStatus.RUNNING);
                    runningJobMap.put(mvTableId, pendingJob);
                    jobExecutor.addRefreshJob(pendingJob);
                }
            } else if (toStatus == Constants.MaterializedViewJobStatus.CANCELED) {
                Queue<MaterializedViewRefreshJob> pendingJobQueue = pendingJobMap.get(mvTableId);
                if (pendingJobQueue != null) {
                    Iterator<MaterializedViewRefreshJob> queueIter = pendingJobQueue.iterator();
                    while (queueIter.hasNext()) {
                        MaterializedViewRefreshJob pendingJob = queueIter.next();
                        if (pendingJob.getId() == statusChange.getJobId()) {
                            pendingJob.setStatus(toStatus);
                            jobHistory.addFirst(pendingJob);
                            queueIter.remove();
                        }
                    }
                }
            }
        } else if (fromStatus == Constants.MaterializedViewJobStatus.RUNNING) {
            if (toStatus == Constants.MaterializedViewJobStatus.SUCCESS |
                    toStatus == Constants.MaterializedViewJobStatus.CANCELED |
                    toStatus == Constants.MaterializedViewJobStatus.FAILED |
                    toStatus == Constants.MaterializedViewJobStatus.PARTIAL_SUCCESS) {
                MaterializedViewRefreshJob job = runningJobMap.remove(mvTableId);
                job.setStatus(toStatus);
                jobHistory.addFirst(job);
            }
        }
    }
}
