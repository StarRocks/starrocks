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

import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.AgentTask;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TPartitionSnapshotRequest;
import com.starrocks.thrift.TTaskType;

import java.util.List;
import java.util.Map;

public class ClusterSnapshotTask extends AgentTask {

    private final long jobId;
    private final long preVersion;
    private final long newVersion;
    private final long physicalPartitionId;
    private final long virtualTabletId;
    private Map<TBackend, List<Long>> nodeToTablets;

    public ClusterSnapshotTask(long backendId, long dbId, long tableId, long partitionId, long physicalPartitionId,
            long jobId, long preVersion, long newVersion, long virtualTabletId) {
        super(null, backendId, TTaskType.PARTITION_SNAPSHOT, dbId, tableId, partitionId, -1L, -1L,
                GlobalStateMgr.getCurrentState().getNextId());
        this.jobId = jobId;
        this.preVersion = preVersion;
        this.newVersion = newVersion;
        this.physicalPartitionId = physicalPartitionId;
        this.virtualTabletId = virtualTabletId;
    }

    public long getJobId() {
        return jobId;
    }

    public long getPreVersion() {
        return preVersion;
    }

    public long getNewVersion() {
        return newVersion;
    }

    public long getPhysicalPartitionId() {
        return physicalPartitionId;
    }

    public long getVirtualTablet() {
        return virtualTabletId;
    }

    public Map<TBackend, List<Long>> getNodeToTablets() {
        return nodeToTablets;
    }

    public void setNodeToTablets(Map<TBackend, List<Long>> nodeToTablets) {
        this.nodeToTablets = nodeToTablets;
    }

    public TPartitionSnapshotRequest toThrift() {
        TPartitionSnapshotRequest request = new TPartitionSnapshotRequest();
        request.setJob_id(jobId);
        request.setDb_id(dbId);
        request.setTable_id(tableId);
        request.setPartition_id(partitionId);
        request.setPhysical_partition_id(physicalPartitionId);
        request.setPre_version(preVersion);
        request.setNew_version(newVersion);
        request.setVirtual_tablet(virtualTabletId);
        request.setNode_to_tablets(nodeToTablets);
        return request;
    }
}