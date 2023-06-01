//  Copyright 2021-present StarRocks, Inc. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package com.starrocks.load.pipe;

import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;

import java.util.Map;

public class PipeTaskDesc {
    private String dbName;
    private String uniqueName;
    private String sqlTask;
    private Map<String, String> properties;
    private Map<Long, Integer> beSlotRequirement;
    private PipeTaskState state = PipeTaskState.RUNNABLE;

    // Execution state
    private Task task;

    public PipeTaskDesc(String uniqueName, String sqlTask, Map<Long, Integer> beSlotRequirement) {
        this.uniqueName = uniqueName;
        this.sqlTask = sqlTask;
        this.beSlotRequirement = beSlotRequirement;
    }

    public void onRunning() {
        this.state = PipeTaskState.RUNNING;
    }

    public void onFinished() {
        this.state = PipeTaskState.FINISHED;
    }

    public void onError() {
        this.state = PipeTaskState.ERROR;
    }

    public boolean isRunnable() {
        return this.state.equals(PipeTaskState.RUNNABLE);
    }

    public boolean isRunning() {
        return this.state.equals(PipeTaskState.RUNNING);
    }

    public void interrupt() {
        if (!this.state.equals(PipeTaskState.RUNNING)) {
            return;
        }
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        taskManager.killTask(task.getName(), true);
    }

    public String getDbName() {
        return dbName;
    }

    public String getUniqueName() {
        return uniqueName;
    }

    public String getSqlTask() {
        return sqlTask;
    }

    public Map<Long, Integer> getBeSlotRequirement() {
        return beSlotRequirement;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    enum PipeTaskState {
        RUNNABLE,
        RUNNING,
        FINISHED,
        ERROR,
    }
}
