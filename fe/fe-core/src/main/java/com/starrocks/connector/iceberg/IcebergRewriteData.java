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

import com.starrocks.connector.RemoteFileInfo;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.util.StructLikeWrapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;


public class IcebergRewriteData {

    private IcebergConnectorScanRangeSource source;
    private Map<StructLikeWrapper, List<TaskGroup>> partitionedTasks;
    private int maxScanRangeLength = 100;
    private long batchSize = 10L * 1024 * 1024 * 1024;
    private Iterator<List<TaskGroup>> outer;
    private Iterator<TaskGroup> inner = Collections.emptyIterator();

    public void setSource(IcebergConnectorScanRangeSource source) {
        this.source = source;
    }

    public void setBatchSize(long batchSize) {
        this.batchSize = batchSize;
    }

    public void setMaxScanRangeLength(int maxScanRangeLength) {
        this.maxScanRangeLength = maxScanRangeLength;
    }

    public void buildNewScanNodeRange(long fileSizeThreshold, boolean allFiles) {
        partitionedTasks = new HashMap<>();
        List<FileScanTask> tasks = new ArrayList<>();
        do {
            if (source != null) {
                tasks = source.getSourceFileScanOutputs(maxScanRangeLength, fileSizeThreshold, allFiles);
            }
            for (FileScanTask task : tasks) {
                PartitionSpec spec = task.spec();
                StructLikeWrapper partitionWrapper = StructLikeWrapper.forType(spec.partitionType());
                StructLikeWrapper partition = partitionWrapper.copyFor(task.file().partition());
                partitionedTasks.putIfAbsent(partition, new ArrayList<TaskGroup>());
                List<TaskGroup> taskGroups = partitionedTasks.get(partition);
                boolean put = false;

                for (TaskGroup tg : taskGroups) {
                    if (tg.getTotalSize() + task.file().fileSizeInBytes() <= batchSize) {
                        tg.addTask(task);
                        put = true;
                        break;
                    }
                }

                if (!put) {
                    TaskGroup tg = new TaskGroup();
                    tg.addTask(task);
                    taskGroups.add(tg);
                }
            }
        } while (!tasks.isEmpty());
        this.outer = partitionedTasks.values().iterator();
    }

    public boolean hasMoreTaskGroup() {
        while (!inner.hasNext() && outer.hasNext()) {
            inner = outer.next().iterator();
        }
        return inner.hasNext();
    }

    public List<RemoteFileInfo> nextTaskGroup() {
        if (!hasMoreTaskGroup()) {
            throw new NoSuchElementException();
        }
        TaskGroup tg = inner.next();
        return tg.getTasks().stream().map(IcebergRemoteFileInfo::new).collect(Collectors.toList());
    }

}
