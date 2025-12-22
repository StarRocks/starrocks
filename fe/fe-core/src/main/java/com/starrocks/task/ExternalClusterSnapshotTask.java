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

import com.starrocks.task.AgentTask;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TExternalClusterSnapshotRequest;
import com.starrocks.thrift.TTaskType;

import java.util.List;

public class ExternalClusterSnapshotTask extends AgentTask {

    private final long jobId;
    private final long preVersion;
    private final long newVersion;
    private final long physicalPartitionId;
    private final long destTablet;
    private List<Long> tablets;
    private List<TBackend> computeNodes;

    public ExternalClusterSnapshotTask(long backendId, long dbId, long tableId, long partitionId,
            long physicalPartitionId, long jobId, long preVersion, long newVersion, long destTablet) {
        super(null, backendId, TTaskType.EXTERNAL_CLUSTER_SNAPSHOT, dbId, tableId, partitionId, -1L, -1L,
                physicalPartitionId);
        this.jobId = jobId;
        this.preVersion = preVersion;
        this.newVersion = newVersion;
        this.physicalPartitionId = physicalPartitionId;
        this.destTablet = destTablet;
    }

    public long getJobId() {
        return jobId;
    }

    public void setTablets(List<Long> tablets) {
        this.tablets = tablets;
    }

    public void setComputeNodes(List<TBackend> computeNodes) {
        this.computeNodes = computeNodes;
    }

    public TExternalClusterSnapshotRequest toThrift() {
        TExternalClusterSnapshotRequest request = new TExternalClusterSnapshotRequest();
        request.setJob_id(jobId);
        request.setDb_id(dbId);
        request.setTable_id(tableId);
        request.setPartition_id(partitionId);
        request.setPhysical_partition_id(physicalPartitionId);
        request.setPre_version(preVersion);
        request.setNew_version(newVersion);
        request.setDest_tablet_id(destTablet);
        request.setSrc_tablets(tablets);
        request.setCompute_nodes(computeNodes);
        return request;
    }
}