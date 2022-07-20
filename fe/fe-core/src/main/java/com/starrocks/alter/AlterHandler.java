// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/alter/AlterHandler.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.alter;

import com.google.common.collect.Maps;
import com.starrocks.alter.AlterJob.JobState;
import com.starrocks.analysis.AlterClause;
import com.starrocks.analysis.CancelStmt;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.UserException;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.RemoveAlterJobV2OperationLog;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AlterReplicaTask;
import com.starrocks.thrift.TTabletInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AlterHandler extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(AlterHandler.class);

    // tableId -> AlterJob
    @Deprecated
    protected ConcurrentHashMap<Long, AlterJob> alterJobs = new ConcurrentHashMap<>();
    @Deprecated
    protected ConcurrentLinkedQueue<AlterJob> finishedOrCancelledAlterJobs = new ConcurrentLinkedQueue<>();

    // queue of alter job v2
    protected ConcurrentMap<Long, AlterJobV2> alterJobsV2 = Maps.newConcurrentMap();

    /**
     * lock to perform atomic operations.
     * eg.
     * When job is finished, it will be moved from alterJobs to finishedOrCancelledAlterJobs,
     * and this requires atomic operations. So the lock must be held to do this operations.
     * Operations like Get or Put do not need lock.
     */
    protected ReentrantLock lock = new ReentrantLock();

    protected ThreadPoolExecutor executor;

    protected void lock() {
        lock.lock();
    }

    protected void unlock() {
        lock.unlock();
    }

    public AlterHandler(String name) {
        super(name, FeConstants.default_scheduler_interval_millisecond);
        executor = ThreadPoolManager
                .newDaemonCacheThreadPool(Config.alter_max_worker_threads, Config.alter_max_worker_queue_size,
                        name + "_pool", true);
    }

    protected void addAlterJobV2(AlterJobV2 alterJob) {
        this.alterJobsV2.put(alterJob.getJobId(), alterJob);
        LOG.info("add {} job {}", alterJob.getType(), alterJob.getJobId());
    }

    public List<AlterJobV2> getUnfinishedAlterJobV2ByTableId(long tblId) {
        List<AlterJobV2> unfinishedAlterJobList = new ArrayList<>();
        for (AlterJobV2 alterJob : alterJobsV2.values()) {
            if (alterJob.getTableId() == tblId
                    && alterJob.getJobState() != AlterJobV2.JobState.FINISHED
                    && alterJob.getJobState() != AlterJobV2.JobState.CANCELLED) {
                unfinishedAlterJobList.add(alterJob);
            }
        }
        return unfinishedAlterJobList;
    }

    public AlterJobV2 getUnfinishedAlterJobV2ByJobId(long jobId) {
        for (AlterJobV2 alterJob : alterJobsV2.values()) {
            if (alterJob.getJobId() == jobId && !alterJob.isDone()) {
                return alterJob;
            }
        }
        return null;
    }

    public Map<Long, AlterJobV2> getAlterJobsV2() {
        return this.alterJobsV2;
    }

    // should be removed in version 0.13
    @Deprecated
    private void clearExpireFinishedOrCancelledAlterJobs() {
        long curTime = System.currentTimeMillis();
        // clean history job
        Iterator<AlterJob> iter = finishedOrCancelledAlterJobs.iterator();
        while (iter.hasNext()) {
            AlterJob historyJob = iter.next();
            if ((curTime - historyJob.getCreateTimeMs()) / 1000 > Config.history_job_keep_max_second) {
                iter.remove();
                LOG.info("remove history {} job[{}]. finish at {}", historyJob.getType(),
                        historyJob.getTableId(), TimeUtils.longToTimeString(historyJob.getFinishedTime()));
            }
        }
    }

    private void clearExpireFinishedOrCancelledAlterJobsV2() {
        Iterator<Map.Entry<Long, AlterJobV2>> iterator = alterJobsV2.entrySet().iterator();
        while (iterator.hasNext()) {
            AlterJobV2 alterJobV2 = iterator.next().getValue();
            if (alterJobV2.isExpire()) {
                iterator.remove();
                RemoveAlterJobV2OperationLog log =
                        new RemoveAlterJobV2OperationLog(alterJobV2.getJobId(), alterJobV2.getType());
                GlobalStateMgr.getCurrentState().getEditLog().logRemoveExpiredAlterJobV2(log);
                LOG.info("remove expired {} job {}. finish at {}", alterJobV2.getType(),
                        alterJobV2.getJobId(), TimeUtils.longToTimeString(alterJobV2.getFinishedTimeMs()));
            }
        }
    }

    public void replayRemoveAlterJobV2(RemoveAlterJobV2OperationLog log) {
        if (alterJobsV2.remove(log.getJobId()) != null) {
            LOG.info("replay removing expired {} job {}.", log.getType(), log.getJobId());
        } else {
            // should not happen, but it does no matter, just add a warn log here to observe
            LOG.warn("failed to find {} job {} when replay removing expired job.", log.getType(), log.getJobId());
        }
    }

    @Deprecated
    protected void addAlterJob(AlterJob alterJob) {
        this.alterJobs.put(alterJob.getTableId(), alterJob);
        LOG.info("add {} job[{}]", alterJob.getType(), alterJob.getTableId());
    }

    @Deprecated
    public AlterJob getAlterJob(long tableId) {
        return this.alterJobs.get(tableId);
    }

    @Deprecated
    public boolean hasUnfinishedAlterJob(long tableId) {
        return this.alterJobs.containsKey(tableId);
    }

    @Deprecated
    public int getAlterJobNum(JobState state, long dbId) {
        int jobNum = 0;
        if (state == JobState.PENDING || state == JobState.RUNNING || state == JobState.FINISHING) {
            for (AlterJob alterJob : alterJobs.values()) {
                if (alterJob.getState() == state && alterJob.getDbId() == dbId) {
                    ++jobNum;
                }
            }
        } else if (state == JobState.FINISHED) {
            // lock to perform atomically
            lock();
            try {
                for (AlterJob alterJob : alterJobs.values()) {
                    if (alterJob.getState() == JobState.FINISHED && alterJob.getDbId() == dbId) {
                        ++jobNum;
                    }
                }

                for (AlterJob alterJob : finishedOrCancelledAlterJobs) {
                    if (alterJob.getState() == JobState.FINISHED && alterJob.getDbId() == dbId) {
                        ++jobNum;
                    }
                }
            } finally {
                unlock();
            }

        } else if (state == JobState.CANCELLED) {
            for (AlterJob alterJob : finishedOrCancelledAlterJobs) {
                if (alterJob.getState() == JobState.CANCELLED && alterJob.getDbId() == dbId) {
                    ++jobNum;
                }
            }
        }

        return jobNum;
    }

    public Long getAlterJobV2Num(com.starrocks.alter.AlterJobV2.JobState state, long dbId) {
        return alterJobsV2.values().stream().filter(e -> e.getJobState() == state && e.getDbId() == dbId).count();
    }

    public Long getAlterJobV2Num(com.starrocks.alter.AlterJobV2.JobState state) {
        return alterJobsV2.values().stream().filter(e -> e.getJobState() == state).count();
    }

    @Deprecated
    public Map<Long, AlterJob> unprotectedGetAlterJobs() {
        return this.alterJobs;
    }

    @Deprecated
    public ConcurrentLinkedQueue<AlterJob> unprotectedGetFinishedOrCancelledAlterJobs() {
        return this.finishedOrCancelledAlterJobs;
    }

    @Deprecated
    public void addFinishedOrCancelledAlterJob(AlterJob alterJob) {
        alterJob.clear();
        LOG.info("add {} job[{}] to finished or cancel list", alterJob.getType(), alterJob.getTableId());
        this.finishedOrCancelledAlterJobs.add(alterJob);
    }

    @Deprecated
    protected AlterJob removeAlterJob(long tableId) {
        return this.alterJobs.remove(tableId);
    }

    // For UT
    public void clearJobs() {
        this.alterJobsV2.clear();
    }

    @Deprecated
    public void removeDbAlterJob(long dbId) {
        Iterator<Map.Entry<Long, AlterJob>> iterator = alterJobs.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, AlterJob> entry = iterator.next();
            AlterJob alterJob = entry.getValue();
            if (alterJob.getDbId() == dbId) {
                iterator.remove();
            }
        }
    }

    /*
     * handle task report
     * reportVersion is used in schema change job.
     */
    @Deprecated
    public void handleFinishedReplica(AgentTask task, TTabletInfo finishTabletInfo, long reportVersion)
            throws MetaNotFoundException {
        long tableId = task.getTableId();

        AlterJob alterJob = getAlterJob(tableId);
        if (alterJob == null) {
            throw new MetaNotFoundException("Cannot find " + task.getTaskType().name() + " job[" + tableId + "]");
        }
        alterJob.handleFinishedReplica(task, finishTabletInfo, reportVersion);
    }

    protected void cancelInternal(AlterJob alterJob, OlapTable olapTable, String msg) {
        // cancel
        alterJob.cancel(olapTable, msg);
        jobDone(alterJob);
    }

    protected void jobDone(AlterJob alterJob) {
        lock();
        try {
            // remove job
            AlterJob alterJobRemoved = removeAlterJob(alterJob.getTableId());
            // add to finishedOrCancelledAlterJobs
            if (alterJobRemoved != null) {
                // add alterJob not alterJobRemoved, because the alterJob maybe a new object
                // deserialized from journal, and the finished state is set to the new object
                addFinishedOrCancelledAlterJob(alterJob);
            }
        } finally {
            unlock();
        }
    }

    public void replayInitJob(AlterJob alterJob, GlobalStateMgr globalStateMgr) {
        Database db = globalStateMgr.getDb(alterJob.getDbId());
        alterJob.replayInitJob(db);
        // add rollup job
        addAlterJob(alterJob);
    }

    public void replayFinishing(AlterJob alterJob, GlobalStateMgr globalStateMgr) {
        Database db = globalStateMgr.getDb(alterJob.getDbId());
        alterJob.replayFinishing(db);
        alterJob.setState(JobState.FINISHING);
        // !!! the alter job should add to the cache again, because the alter job is deserialized from journal
        // it is a different object compared to the cache
        addAlterJob(alterJob);
    }

    public void replayFinish(AlterJob alterJob, GlobalStateMgr globalStateMgr) {
        Database db = globalStateMgr.getDb(alterJob.getDbId());
        alterJob.replayFinish(db);
        alterJob.setState(JobState.FINISHED);

        jobDone(alterJob);
    }

    public void replayCancel(AlterJob alterJob, GlobalStateMgr globalStateMgr) {
        removeAlterJob(alterJob.getTableId());
        alterJob.setState(JobState.CANCELLED);
        Database db = globalStateMgr.getDb(alterJob.getDbId());
        if (db != null) {
            // we log rollup job cancelled even if db is dropped.
            // so check db != null here
            alterJob.replayCancel(db);
        }

        addFinishedOrCancelledAlterJob(alterJob);
    }

    @Override
    protected void runAfterCatalogReady() {
        clearExpireFinishedOrCancelledAlterJobs();
        clearExpireFinishedOrCancelledAlterJobsV2();
    }

    @Override
    public synchronized void start() {
        super.start();
    }

    /*
     * abstract
     */
    /*
     * get alter job's info for show
     */
    public abstract List<List<Comparable>> getAlterJobInfosByDb(Database db);

    /*
     * entry function. handle alter ops
     */
    public abstract ShowResultSet process(List<AlterClause> alterClauses, Database db, OlapTable olapTable)
            throws UserException;

    /*
     * cancel alter ops
     */
    public abstract void cancel(CancelStmt stmt) throws DdlException;

    @Deprecated
    public Integer getAlterJobNumByState(JobState state) {
        int jobNum = 0;
        for (AlterJob alterJob : alterJobs.values()) {
            if (alterJob.getState() == state) {
                ++jobNum;
            }
        }
        return jobNum;
    }

    public void handleFinishAlterTask(AlterReplicaTask task) throws RejectedExecutionException {
        executor.submit(task);
    }

    // replay the alter job v2
    public void replayAlterJobV2(AlterJobV2 alterJob) {
        AlterJobV2 existingJob = alterJobsV2.get(alterJob.getJobId());
        if (existingJob == null) {
            // This is the first time to replay the alter job, so just using the replayed alterJob to call replay();
            alterJob.replay(alterJob);
            alterJobsV2.put(alterJob.getJobId(), alterJob);
        } else {
            existingJob.replay(alterJob);
        }
    }
}
