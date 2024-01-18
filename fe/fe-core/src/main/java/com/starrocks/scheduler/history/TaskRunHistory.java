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

package com.starrocks.scheduler.history;

import com.starrocks.common.NotImplementedException;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.scheduler.persist.TaskRunStatusChange;

import java.util.List;

/**
 * Record the history of all task-runs
 */
public interface TaskRunHistory {

    default TaskRunStatus getTask(String queryId) {
        return null;
    }

    default void removeTask(String queryId) {
    }

    default void addHistory(TaskRunStatus status) {
    }

    void addHistory(TaskRunStatus status, boolean isReplay);

    List<TaskRunStatus> getTaskByName(String taskName);

    void replayTaskRunChange(String queryId, TaskRunStatusChange change);

    /**
     * @return removed TaskRunId in the history
     */
    void gc();

    void forceGC();

    List<TaskRunStatus> getAllHistory();

    long getTaskRunCount();
}
