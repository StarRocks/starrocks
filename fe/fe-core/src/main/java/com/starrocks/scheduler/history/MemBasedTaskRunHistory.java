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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.scheduler.persist.TaskRunStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MemBasedTaskRunHistory implements TaskRunHistory {

    private static final Logger LOG = LogManager.getLogger(MemBasedTaskRunHistory.class);

    // Thread-Safe history map: QueryId -> TaskRunStatus
    // The same task-id may contain multi history task run status, so use query_id instead.
    private final Map<String, TaskRunStatus> historyTaskRunMap =
            Collections.synchronizedMap(Maps.newLinkedHashMap());
    private final Map<String, TaskRunStatus> taskName2Status =
            Collections.synchronizedMap(Maps.newLinkedHashMap());

    @Override
    public void addHistory(TaskRunStatus status, boolean isReplay) {
        historyTaskRunMap.put(status.getQueryId(), status);
        taskName2Status.put(status.getTaskName(), status);
    }

    @Override
    public List<TaskRunStatus> getTaskByName(String taskName) {
        return Lists.newArrayList(taskName2Status.get(taskName));
    }

    @Override
    public TaskRunStatus getTask(String queryId) {
        if (queryId == null) {
            return null;
        }
        return historyTaskRunMap.get(queryId);
    }

    private void removeTask(String queryId) {
        if (queryId == null) {
            return;
        }
        TaskRunStatus task = historyTaskRunMap.remove(queryId);
        taskName2Status.remove(task.getTaskName());
    }

    // Reserve historyTaskRunMap values to keep the last insert at the first.
    @Override
    public List<TaskRunStatus> getAllHistory() {
        List<TaskRunStatus> historyRunStatus =
                new ArrayList<>(historyTaskRunMap.values());
        Collections.reverse(historyRunStatus);
        return historyRunStatus;
    }

    @Override
    public void forceGC() {
        List<TaskRunStatus> allHistory = getAllHistory();
        int startIndex = Math.min(allHistory.size(), Config.task_runs_max_history_number);
        allHistory.subList(startIndex, allHistory.size())
                .forEach(taskRunStatus -> removeTask(taskRunStatus.getQueryId()));
    }

    @Override
    public void gc() {
        // only SUCCESS and FAILED in taskRunHistory
        long currentTimeMs = System.currentTimeMillis();
        List<TaskRunStatus> taskRunHistory = getAllHistory();
        Iterator<TaskRunStatus> iterator = taskRunHistory.iterator();
        List<String> historyToDelete = Lists.newArrayList();
        while (iterator.hasNext()) {
            TaskRunStatus taskRunStatus = iterator.next();
            long expireTime = taskRunStatus.getExpireTime();
            if (currentTimeMs > expireTime) {
                historyToDelete.add(taskRunStatus.getQueryId());
                removeTask(taskRunStatus.getQueryId());
                iterator.remove();
            }
        }
        LOG.info("[GC] remove run history:{}", historyToDelete);
    }

    @Override
    public long getTaskRunCount() {
        return historyTaskRunMap.size();
    }
}
