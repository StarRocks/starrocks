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

import com.google.common.base.Preconditions;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoSource;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.connector.iceberg.IcebergMORParams.ScanTaskType.DATA_FILE_WITHOUT_EQ_DELETE;
import static com.starrocks.connector.iceberg.IcebergMORParams.ScanTaskType.DATA_FILE_WITH_EQ_DELETE;
import static com.starrocks.connector.iceberg.IcebergMORParams.ScanTaskType.EQ_DELETE;

// only used for iceberg table with equality delete files.
public class IcebergRemoteSourceTrigger {
    private final RemoteFileInfoSource delegate;

    // It is only used to distinguish equality delete files of different equality_ids.
    // If there is only one identifier column combination, the queues is empty. and we use `eqDeleteQueue` instead of it.
    private final Map<IcebergMORParams, Deque<RemoteFileInfo>> queues = new HashMap<>();

    private Optional<Deque<RemoteFileInfo>> dataFileWithEqDeleteQueue = Optional.empty();
    private Optional<Deque<RemoteFileInfo>> dataFileWithoutEqDeleteQueue = Optional.empty();
    private Optional<Deque<RemoteFileInfo>> eqDeleteQueue = Optional.empty();

    private final boolean needToCheckEqualityIds;

    // for incremental scan range
    public IcebergRemoteSourceTrigger(RemoteFileInfoSource remoteFileInfoSource, IcebergTableMORParams tableFullMORParams) {
        this.delegate = remoteFileInfoSource;
        Preconditions.checkArgument(tableFullMORParams.size() >= 3);
        this.needToCheckEqualityIds = tableFullMORParams.size() != 3;
        for (IcebergMORParams params : tableFullMORParams.getMorParamsList()) {
            IcebergMORParams.ScanTaskType type = params.getScanTaskType();
            if (type == DATA_FILE_WITHOUT_EQ_DELETE) {
                dataFileWithoutEqDeleteQueue = Optional.of(new ArrayDeque<>());
            } else if (type == DATA_FILE_WITH_EQ_DELETE) {
                dataFileWithEqDeleteQueue = Optional.of(new ArrayDeque<>());
            } else {
                if (needToCheckEqualityIds) {
                    queues.put(params, new ArrayDeque<>());
                } else {
                    eqDeleteQueue = Optional.of(new ArrayDeque<>());
                }
            }
        }
    }

    // for full scan ranges.
    // Only fill the queue corresponding to morParams, and the remaining queues will not be filled with any file scan tasks.
    public IcebergRemoteSourceTrigger(RemoteFileInfoSource remoteFileInfoSource,
                                      IcebergMORParams morParams,
                                      boolean needToCheckEqualityIds) {
        this.delegate = remoteFileInfoSource;
        this.needToCheckEqualityIds = needToCheckEqualityIds;

        IcebergMORParams.ScanTaskType type = morParams.getScanTaskType();
        if (type == DATA_FILE_WITHOUT_EQ_DELETE) {
            dataFileWithoutEqDeleteQueue = Optional.of(new ArrayDeque<>());
        } else if (type == DATA_FILE_WITH_EQ_DELETE) {
            dataFileWithEqDeleteQueue = Optional.of(new ArrayDeque<>());
        } else {
            if (needToCheckEqualityIds) {
                queues.put(morParams, new ArrayDeque<>());
            } else {
                eqDeleteQueue = Optional.of(new ArrayDeque<>());
            }
        }
    }

    public boolean hasMoreOutput() {
        return delegate.hasMoreOutput();
    }

    public synchronized void trigger() {
        if (!delegate.hasMoreOutput()) {
            return;
        }

        IcebergRemoteFileInfo remoteFileInfo = delegate.getOutput().cast();
        FileScanTask scanTask = remoteFileInfo.getFileScanTask();
        Optional<DeleteFile> eqDeleteFile = scanTask.deletes().stream()
                .filter(f -> f.content() == FileContent.EQUALITY_DELETES)
                .findFirst();

        // dispatch the iceberg file scan task to the corresponding queue.
        if (eqDeleteFile.isEmpty()) {
            dataFileWithoutEqDeleteQueue.map(queue -> queue.add(remoteFileInfo));
        } else {
            dataFileWithEqDeleteQueue.map(queue -> queue.add(remoteFileInfo));
            if (!needToCheckEqualityIds) {
                eqDeleteQueue.map(queue -> queue.add(remoteFileInfo));
            } else {
                Deque<RemoteFileInfo> queue = queues.get(IcebergMORParams.of(EQ_DELETE, eqDeleteFile.get().equalityFieldIds()));
                if (queue != null) {
                    queue.add(remoteFileInfo);
                }
            }
        }
    }

    public Deque<RemoteFileInfo> getQueue(IcebergMORParams params) {
        if (params.getScanTaskType() == DATA_FILE_WITHOUT_EQ_DELETE) {
            return dataFileWithoutEqDeleteQueue.get();
        } else if (params.getScanTaskType() == DATA_FILE_WITH_EQ_DELETE) {
            return dataFileWithEqDeleteQueue.get();
        }

        if (!needToCheckEqualityIds) {
            return eqDeleteQueue.get();
        } else {
            return queues.get(params);
        }
    }
}
