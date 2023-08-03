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


package com.starrocks.scheduler;

import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.scheduler.persist.TaskRunStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TaskRunHistory {
    // Thread-Safe history map: QueryId -> TaskRunStatus
    // The same task-id may contain multi history task run status, so use query_id instead.
    private final Map<String, TaskRunStatus> historyTaskRunMap =
            Collections.synchronizedMap(Maps.newLinkedHashMap());

    public void addHistory(TaskRunStatus status) {
        historyTaskRunMap.put(status.getQueryId(), status);
    }

    public TaskRunStatus getTask(String queryId) {
        if (queryId == null) {
            return null;
        }
        return historyTaskRunMap.get(queryId);
    }

    public void removeTask(String queryId) {
        if (queryId == null) {
            return;
        }
        historyTaskRunMap.remove(queryId);
    }

    // Reserve historyTaskRunMap values to keep the last insert at the first.
    public List<TaskRunStatus> getAllHistory() {
        List<TaskRunStatus> historyRunStatus =
                new ArrayList<>(historyTaskRunMap.values());
        Collections.reverse(historyRunStatus);
        return historyRunStatus;
    }

    public void forceGC() {
        List<TaskRunStatus> allHistory = getAllHistory();
        int startIndex = Math.max(0, allHistory.size() - Config.task_runs_max_history_number);
        allHistory.subList(startIndex, allHistory.size())
                .forEach(taskRunStatus -> historyTaskRunMap.remove(taskRunStatus.getQueryId()));
    }

    public long getTaskRunCount() {
        return historyTaskRunMap.size();
    }
}
