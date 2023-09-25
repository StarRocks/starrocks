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
import com.starrocks.thrift.TReplicationRequest;
import com.starrocks.thrift.TTaskType;

import java.util.List;

public class ReplicationTask extends AgentTask {
    private final int schemaHash;
    private final long snapshotVersion;

    private final String srcToken;
    private final long srcTabletId;
    private final List<TBackend> srcBackends;

    private final int timeoutS;

    public ReplicationTask(long backendId, long dbId, long tableId, long partitionId, long indexId, long tabletId,
            long replicaId, int schemaHash, long snapshotVersion, String srcToken, long srcTabletId,
            List<TBackend> srcBackends, int timeoutS) {
        super(null, backendId, TTaskType.REPLICATION, dbId, tableId, partitionId, indexId, tabletId, replicaId);
        this.schemaHash = schemaHash;
        this.snapshotVersion = snapshotVersion;
        this.srcToken = srcToken;
        this.srcTabletId = srcTabletId;
        this.srcBackends = srcBackends;
        this.timeoutS = timeoutS;
    }

    public long getReplicaId() {
        return signature;
    }

    public TReplicationRequest toThrift() {
        TReplicationRequest request = new TReplicationRequest();

        request.setTable_id(tableId);
        request.setPartition_id(partitionId);
        request.setTablet_id(tabletId);

        request.setSchema_hash(schemaHash);
        request.setSnapshot_version(snapshotVersion);

        request.setSrc_token(srcToken);
        request.setSrc_tablet_id(srcTabletId);
        request.setSrc_backends(srcBackends);

        request.setTimeout_s(timeoutS);
        return request;
    }
}
