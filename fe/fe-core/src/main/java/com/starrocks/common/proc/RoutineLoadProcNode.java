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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/RoutineLoadProcNode.java

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

package com.starrocks.common.proc;

import com.starrocks.common.AnalysisException;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ShowRoutineLoadTaskStmt;

/*
    SHOW RPOC "/routine_loads/{jobName}/{jobId}"
    show routine load task info belong to job

    RESULT:
    show result is sames as show routine load task
 */
public class RoutineLoadProcNode implements ProcNodeInterface {

    private final long jobId;

    public RoutineLoadProcNode(long jobId) {
        this.jobId = jobId;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        // check job id
        RoutineLoadManager routineLoadManager = GlobalStateMgr.getCurrentState().getRoutineLoadManager();
        RoutineLoadJob routineLoadJob = routineLoadManager.getJob(jobId);
        if (routineLoadJob == null) {
            throw new AnalysisException("Job[" + jobId + "] does not exist");
        }

        BaseProcResult result = new BaseProcResult();
        result.setNames(ShowRoutineLoadTaskStmt.getTitleNames());
        result.setRows(routineLoadJob.getTasksShowInfo());
        return result;
    }
}
