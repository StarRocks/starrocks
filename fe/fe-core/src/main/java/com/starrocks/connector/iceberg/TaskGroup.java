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

package com.starrocks.connector.iceberg;

import org.apache.iceberg.FileScanTask;

import java.util.ArrayList;
import java.util.List;

public class TaskGroup {
    private List<FileScanTask> tasks;
    private long totalSize;

    public TaskGroup() {
        this.tasks = new ArrayList<>();
        this.totalSize = 0;
    }

    public void addTask(FileScanTask task) {
        tasks.add(task);
        totalSize += task.file().fileSizeInBytes();
    }

    public List<FileScanTask> getTasks() {
        return tasks;
    }

    public long getTotalSize() {
        return totalSize;
    }
}
