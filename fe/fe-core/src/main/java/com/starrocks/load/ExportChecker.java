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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/ExportChecker.java

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

package com.starrocks.load;

import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.load.ExportJob.JobState;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.task.ExportExportingTask;
import com.starrocks.task.ExportPendingTask;
import com.starrocks.task.LeaderTaskExecutor;
import com.starrocks.task.PriorityLeaderTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public final class ExportChecker extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(ExportChecker.class);

    // checkers for running job state
    private static Map<JobState, ExportChecker> checkers = Maps.newHashMap();
    // executors for pending tasks
    private static Map<JobState, LeaderTaskExecutor> executors = Maps.newHashMap();
    private JobState jobState;

    private static LeaderTaskExecutor exportingSubTaskExecutor;

    private ExportChecker(JobState jobState, long intervalMs) {
        super("export checker " + jobState.name().toLowerCase(), intervalMs);
        this.jobState = jobState;
    }

    public static void init(long intervalMs) {
        checkers.put(JobState.PENDING, new ExportChecker(JobState.PENDING, intervalMs));
        checkers.put(JobState.EXPORTING, new ExportChecker(JobState.EXPORTING, intervalMs));

        int poolSize = Config.export_running_job_num_limit == 0 ? 5 : Config.export_running_job_num_limit;
        LeaderTaskExecutor pendingTaskExecutor = new LeaderTaskExecutor("export_pending_job", poolSize, true);
        executors.put(JobState.PENDING, pendingTaskExecutor);

        LeaderTaskExecutor exportingTaskExecutor = new LeaderTaskExecutor("export_exporting_job", poolSize, true);
        executors.put(JobState.EXPORTING, exportingTaskExecutor);

        // One export job will be split into multiple exporting sub tasks, the queue size is not determined, so set Integer.MAX_VALUE.
        exportingSubTaskExecutor = new LeaderTaskExecutor("export_exporting_sub_task", Config.export_task_pool_size,
                Integer.MAX_VALUE, true);
    }

    public static void startAll() {
        for (ExportChecker exportChecker : checkers.values()) {
            exportChecker.start();
        }
        for (LeaderTaskExecutor leaderTaskExecutor : executors.values()) {
            leaderTaskExecutor.start();
        }
        exportingSubTaskExecutor.start();
    }

    public static LeaderTaskExecutor getExportingSubTaskExecutor() {
        return exportingSubTaskExecutor;
    }

    @Override
    protected void runAfterCatalogReady() {
        LOG.debug("start check export jobs. job state: {}", jobState.name());
        switch (jobState) {
            case PENDING:
                runPendingJobs();
                break;
            case EXPORTING:
                runExportingJobs();
                break;
            default:
                LOG.warn("wrong export job state: {}", jobState.name());
                break;
        }
    }

    private void runPendingJobs() {
        ExportMgr exportMgr = GlobalStateMgr.getCurrentState().getExportMgr();
        List<ExportJob> pendingJobs = exportMgr.getExportJobs(JobState.PENDING);

        // check to limit running etl job num
        int runningJobNumLimit = Config.export_running_job_num_limit;
        if (runningJobNumLimit > 0 && !pendingJobs.isEmpty()) {
            // pending executor running + exporting state
            int runningJobNum = executors.get(JobState.PENDING).getTaskNum()
                    + executors.get(JobState.EXPORTING).getTaskNum();
            if (runningJobNum >= runningJobNumLimit) {
                LOG.info("running export job num {} exceeds system limit {}", runningJobNum, runningJobNumLimit);
                return;
            }

            int remain = runningJobNumLimit - runningJobNum;
            if (pendingJobs.size() > remain) {
                pendingJobs = pendingJobs.subList(0, remain);
            }
        }

        LOG.debug("pending export job num: {}", pendingJobs.size());

        for (ExportJob job : pendingJobs) {
            try {
                PriorityLeaderTask task = new ExportPendingTask(job);
                if (executors.get(JobState.PENDING).submit(task)) {
                    LOG.info("run pending export job. job: {}", job);
                }
            } catch (Exception e) {
                LOG.warn("run pending export job error", e);
            }
        }
    }

    private void runExportingJobs() {
        List<ExportJob> jobs = GlobalStateMgr.getCurrentState().getExportMgr().getExportJobs(JobState.EXPORTING);
        LOG.debug("exporting export job num: {}", jobs.size());
        for (ExportJob job : jobs) {
            boolean cancelled = checkJobNeedCancel(job);
            if (cancelled) {
                continue;
            }
            try {
                PriorityLeaderTask task = new ExportExportingTask(job);
                if (executors.get(JobState.EXPORTING).submit(task)) {
                    LOG.info("run exporting export job. job: {}", job);
                }
            } catch (Exception e) {
                LOG.warn("run export exporing job error", e);
            }
        }
    }

    private boolean checkJobNeedCancel(ExportJob job) {

        boolean beHasErr = false;
        String errMsg = "";
        for (Long beId : job.getBeStartTimeMap().keySet()) {
            Backend be = GlobalStateMgr.getCurrentSystemInfo().getBackend(beId);
            if (null == be) {
                // The current implementation, if the be in the job is not found, 
                // the job will be cancelled
                beHasErr = true;
                errMsg = "be not found during exec export job. be:" + beId;
                LOG.warn(errMsg + " job: {}", job);
                break;
            }
            if (!be.isAlive()) {
                beHasErr = true;
                errMsg = "be not alive during exec export job. be:" + beId;
                LOG.warn(errMsg + " job: {}", job);
                break;
            }
            if (be.getLastStartTime() > job.getBeStartTimeMap().get(beId)) {
                beHasErr = true;
                errMsg = "be has rebooted during exec export job. be:" + beId;
                LOG.warn(errMsg + " job: {}", job);
                break;
            }
        }
        if (beHasErr) {
            try {
                job.cancel(ExportFailMsg.CancelType.BE_STATUS_ERR, errMsg);
            } catch (UserException e) {
                LOG.warn("try to cancel a completed job. job: {}", job);
            }
            return true;
        }
        return false;
    }
}
