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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.UserException;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.RemoveAlterJobV2OperationLog;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.CancelStmt;
import com.starrocks.task.AlterReplicaTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AlterHandler extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(AlterHandler.class);
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
        super(name, Config.alter_scheduler_interval_millisecond);
        executor = ThreadPoolManager
                .newDaemonCacheThreadPool(Config.alter_max_worker_threads, Config.alter_max_worker_queue_size,
                        name + "_pool", true);
    }


    public void addAlterJobV2(AlterJobV2 alterJob) {
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

    public Long getAlterJobV2Num(com.starrocks.alter.AlterJobV2.JobState state, long dbId) {
        return alterJobsV2.values().stream().filter(e -> e.getJobState() == state && e.getDbId() == dbId).count();
    }

    public Long getAlterJobV2Num(com.starrocks.alter.AlterJobV2.JobState state) {
        return alterJobsV2.values().stream().filter(e -> e.getJobState() == state).count();
    }

    // For UT
    public void clearJobs() {
        this.alterJobsV2.clear();
    }

    @Override
    protected void runAfterCatalogReady() {
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
