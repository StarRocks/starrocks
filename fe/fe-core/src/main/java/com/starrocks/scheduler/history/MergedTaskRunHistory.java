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

import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.scheduler.persist.TaskRunStatusChange;
import org.apache.commons.collections.ListUtils;

import java.util.List;

/**
 * Merge MemBased and TableBased to make it backward compatible
 */
public class MergedTaskRunHistory implements TaskRunHistory {

    private final MemBasedTaskRunHistory mem;
    private final TableBasedTaskRunHistory table;

    public MergedTaskRunHistory(MemBasedTaskRunHistory mem, TableBasedTaskRunHistory table) {
        this.mem = mem;
        this.table = table;
    }

    @Override
    public void addHistory(TaskRunStatus status, boolean isReplay) {
        if (status.isUseTableBasedHistory()) {
            table.addHistory(status, isReplay);
        } else {
            mem.addHistory(status, isReplay);
        }
    }

    @Override
    public List<TaskRunStatus> getTaskByName(String taskName) {
        return ListUtils.union(table.getTaskByName(taskName), mem.getTaskByName(taskName));
    }

    @Override
    public void replayTaskRunChange(String queryId, TaskRunStatusChange change) {
        if (!change.isUseTableBasedHistory()) {
            mem.replayTaskRunChange(queryId, change);
        }
    }

    @Override
    public void gc() {
        mem.gc();
        table.gc();
    }

    @Override
    public void forceGC() {
        mem.forceGC();
        table.forceGC();
    }

    @Override
    public List<TaskRunStatus> getAllHistory() {
        return ListUtils.union(mem.getAllHistory(), table.getAllHistory());
    }

    @Override
    public long getTaskRunCount() {
        return mem.getTaskRunCount() + table.getTaskRunCount();
    }
}
