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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/routineload/RoutineLoadScheduler.java

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

package com.starrocks.load.routineload;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class RoutineLoadScheduler extends LeaderDaemon {

    private static final Logger LOG = LogManager.getLogger(RoutineLoadScheduler.class);

    private RoutineLoadMgr routineLoadManager;

    @VisibleForTesting
    public RoutineLoadScheduler() {
        super();
        routineLoadManager = GlobalStateMgr.getCurrentState().getRoutineLoadMgr();
    }

    public RoutineLoadScheduler(RoutineLoadMgr routineLoadManager) {
        super("Routine load scheduler", FeConstants.default_scheduler_interval_millisecond);
        this.routineLoadManager = routineLoadManager;
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            process();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of RoutineLoadScheduler", e);
        }
    }

    private void process() throws UserException {
        // update
        routineLoadManager.updateRoutineLoadJob();
        // get need schedule routine jobs
        List<RoutineLoadJob> routineLoadJobList = getNeedScheduleRoutineJobs();

        if (!routineLoadJobList.isEmpty()) {
            LOG.info("there are {} job(s) need to be scheduled", routineLoadJobList.size());
        }
        for (RoutineLoadJob routineLoadJob : routineLoadJobList) {
            RoutineLoadJob.JobState errorJobState = null;
            UserException userException = null;
            try {
                routineLoadJob.prepare();
                // judge nums of tasks more than max concurrent tasks of cluster
                int desiredConcurrentTaskNum = routineLoadJob.calculateCurrentConcurrentTaskNum();
                if (desiredConcurrentTaskNum <= 0) {
                    // the job will be rescheduled later.
                    LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                            .add("msg", "the current concurrent num is less then or equal to zero, "
                                    + "job will be rescheduled later")
                            .build());
                    continue;
                }
                // check state and divide job into tasks
                routineLoadJob.divideRoutineLoadJob(desiredConcurrentTaskNum);
            } catch (MetaNotFoundException e) {
                errorJobState = RoutineLoadJob.JobState.CANCELLED;
                userException = e;
                LOG.warn(userException.getMessage());
            } catch (UserException e) {
                errorJobState = RoutineLoadJob.JobState.PAUSED;
                userException = e;
                LOG.warn(userException.getMessage());
            }

            if (errorJobState != null) {
                LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                        .add("current_state", routineLoadJob.getState())
                        .add("desired_state", errorJobState)
                        .add("warn_msg",
                                "failed to scheduler job, change job state to desired_state with error reason " +
                                        userException.getMessage())
                        .build(), userException);
                try {
                    ErrorReason reason = new ErrorReason(userException.getErrorCode(), userException.getMessage());
                    routineLoadJob.updateState(errorJobState, reason, false);
                } catch (UserException e) {
                    LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                            .add("current_state", routineLoadJob.getState())
                            .add("desired_state", errorJobState)
                            .add("warn_msg", "failed to change state to desired state")
                            .build(), e);
                }
            }
        }

        // check timeout tasks
        routineLoadManager.processTimeoutTasks();
    }

    private List<RoutineLoadJob> getNeedScheduleRoutineJobs() {
        return routineLoadManager.getRoutineLoadJobByState(Sets.newHashSet(RoutineLoadJob.JobState.NEED_SCHEDULE));
    }
}
