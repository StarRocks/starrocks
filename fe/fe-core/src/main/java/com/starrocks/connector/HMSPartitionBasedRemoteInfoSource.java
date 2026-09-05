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

package com.starrocks.connector;

import com.google.common.base.Preconditions;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.hive.Partition;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Function;

public class HMSPartitionBasedRemoteInfoSource extends AsyncTaskQueue<RemoteFileInfo> implements RemoteFileInfoSource {
    public static final String HMS_PARTITIONS_LIST_FIES_ASYNC_GET = "HMS.PARTITIONS.LIST_FS_ASYNC.GET";
    public static final String HMS_PARTITIONS_LIST_FILES_ASYNC_WAIT = "HMS.PARTITIONS.LIST_FS_ASYNC.WAIT";
    public static final int HMS_PARTITION_BATCH_SIZE_MIN = 16;
    public static final int HMS_PARTITION_BATCH_SIZE_MAX = 128;

    private GetRemoteFilesParams params = null;
    private Function<GetRemoteFilesParams, List<Partition>> fnGetPartitionValues = null;

    private Function<Partition, RemoteFileInfo> fnGetRemoteFileInfo = null;

    public HMSPartitionBasedRemoteInfoSource(Executor executor, GetRemoteFilesParams params,
                                             Function<Partition, RemoteFileInfo> fnGetRemoteFileInfo,
                                             Function<GetRemoteFilesParams, List<Partition>> fnGetPartitionValues) {
        super(executor);
        this.params = params;
        this.fnGetPartitionValues = fnGetPartitionValues;
        this.fnGetRemoteFileInfo = fnGetRemoteFileInfo;
    }

    @Override
    public List<RemoteFileInfo> getOutputs(int maxSize) {
        try (Timer ignored = Tracers.watchScope(Tracers.Module.EXTERNAL, HMS_PARTITIONS_LIST_FIES_ASYNC_GET)) {
            return super.getOutputs(maxSize);
        }
    }

    @Override
    public boolean hasMoreOutput() {
        // `hasMoreOutput` will trigger tasks to generate output.
        try (Timer ignored = Tracers.watchScope(Tracers.Module.EXTERNAL, HMS_PARTITIONS_LIST_FILES_ASYNC_WAIT)) {
            return super.hasMoreOutput();
        }
    }

    @Override
    public int computeOutputSize(RemoteFileInfo output) {
        List<RemoteFileDesc> files = (output.getFiles());
        int size = 1;
        if (files != null) {
            size = Math.max(size, files.size());
        }
        return size;
    }

    @Override
    public RemoteFileInfo getOutput() {
        List<RemoteFileInfo> res = getOutputs(1);
        Preconditions.checkArgument(res.size() == 1);
        return res.get(0);
    }

    class ListPartitionFilesTask implements AsyncTaskQueue.Task {
        Partition partition;
        Object attachment;

        @Override
        public List run() throws InterruptedException {
            RemoteFileInfo remoteFileInfo = fnGetRemoteFileInfo.apply(partition);
            remoteFileInfo.setAttachment(attachment);
            return List.of(remoteFileInfo);
        }
    }

    class GetPartitionValuesTask implements AsyncTaskQueue.Task<RemoteFileInfo> {
        List<GetRemoteFilesParams> requests;
        int usedIndex = 0;

        @Override
        public List<RemoteFileInfo> run() throws InterruptedException {
            if (requests == null) {
                requests = params.partitionExponentially(HMS_PARTITION_BATCH_SIZE_MIN, HMS_PARTITION_BATCH_SIZE_MAX);
                usedIndex = 0;
            }
            return null;
        }

        @Override
        public boolean isDone() {
            return usedIndex >= requests.size();
        }

        @Override
        public List subTasks() {
            GetRemoteFilesParams params = requests.get(usedIndex);
            usedIndex += 1;

            List<Partition> partitions = fnGetPartitionValues.apply(params);
            List attachments = params.getPartitionAttachments();

            List<ListPartitionFilesTask> tasks = new ArrayList<>();
            for (int i = 0; i < partitions.size(); i++) {
                Partition p = partitions.get(i);
                ListPartitionFilesTask t = new ListPartitionFilesTask();
                t.partition = p;
                t.attachment = (attachments != null) ? attachments.get(i) : null;
                tasks.add(t);
            }
            return tasks;
        }
    }

    public void run() {
        GetPartitionValuesTask task = new GetPartitionValuesTask();
        start(List.of(task));
    }
}
