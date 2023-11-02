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

package com.starrocks.task;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Task queue
 */
public class TaskQueue {
    private static final Logger LOG = LogManager.getLogger(TaskQueue.class);

    private static int taskNum = 0;

    // backendId -> (txnId -> create table task)
    private static Table<Long, Long, CreateTableTask> create_table_tasks = HashBasedTable.create();

    public static synchronized boolean addCreateTableTask(CreateTableTask task, long txnId) {
        long backendId = task.getBackendId();
        create_table_tasks.put(backendId, txnId, task);
        taskNum++;
        return true;
    }

    public static synchronized boolean removeCreateTableTask(CreateTableTask task, long txnId) {
        long backendId = task.getBackendId();
        create_table_tasks.remove(backendId, txnId);
        return true;
    }

    public static synchronized CreateTableTask getTask(long backendId, long txnId) {
        return create_table_tasks.get(backendId, txnId);
    }

    // only for test now
    public static synchronized void clearAllTasks() {
        create_table_tasks.clear();
        taskNum = 0;
    }

    public static synchronized int getTaskNum() {
        return taskNum;
    }
}

