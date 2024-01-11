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
import com.starrocks.thrift.TRemoteSnapshotRequest;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;

import java.util.List;

public class RemoteSnapshotTask extends AgentTask {
    private final long transactionId;

    private final TTabletType tabletType;
    private final int schemaHash;
    private final long visibleVersion;

    private final String srcToken;
    private final long srcTabletId;
    private final TTabletType srcTabletType;
    private final int srcSchemaHash;
    private final long srcVisibleVersion;
    private final List<TBackend> srcBackends;

    private final int timeoutSec;

    public RemoteSnapshotTask(long backendId, long dbId, long tableId, long partitionId, long indexId, long tabletId,
            TTabletType tabletType, long transactionId, int schemaHash, long visibleVersion,
            String srcToken, long srcTabletId, TTabletType srcTabletType, int srcSchemaHash,
            long srcVisibleVersion, List<TBackend> srcBackends, int timeoutSec) {
        super(null, backendId, TTaskType.REMOTE_SNAPSHOT, dbId, tableId, partitionId, indexId, tabletId, tabletId,
                System.currentTimeMillis());
        this.transactionId = transactionId;
        this.tabletType = tabletType;
        this.schemaHash = schemaHash;
        this.visibleVersion = visibleVersion;
        this.srcToken = srcToken;
        this.srcTabletId = srcTabletId;
        this.srcTabletType = srcTabletType;
        this.srcSchemaHash = srcSchemaHash;
        this.srcVisibleVersion = srcVisibleVersion;
        this.srcBackends = srcBackends;
        this.timeoutSec = timeoutSec;
    }

    public TRemoteSnapshotRequest toThrift() {
        TRemoteSnapshotRequest request = new TRemoteSnapshotRequest();
        request.setTransaction_id(transactionId);

        request.setTable_id(tableId);
        request.setPartition_id(partitionId);
        request.setTablet_id(tabletId);
        request.setTablet_type(tabletType);
        request.setSchema_hash(schemaHash);
        request.setVisible_version(visibleVersion);

        request.setSrc_token(srcToken);
        request.setSrc_tablet_id(srcTabletId);
        request.setSrc_tablet_type(srcTabletType);
        request.setSrc_schema_hash(srcSchemaHash);
        request.setSrc_visible_version(srcVisibleVersion);
        request.setSrc_backends(srcBackends);

        request.setTimeout_sec(timeoutSec);
        return request;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("transaction id: ").append(transactionId);
        sb.append(", tablet id: ").append(tabletId).append(", tablet type: ").append(tabletType);
        sb.append(", schema hash: ").append(schemaHash);
        sb.append(", visible version: ").append(visibleVersion);
        sb.append(", src token: ").append(srcToken).append(", src tablet id: ").append(srcTabletId);
        sb.append(", src tablet type:").append(srcTabletType).append(", src schema hash: ").append(srcSchemaHash);
        sb.append(", src visible version: ").append(srcVisibleVersion);
        sb.append(", src backends: ").append(srcBackends);
        sb.append(", dest backend: ").append(backendId).append(", timeout sec: ").append(timeoutSec);
        return sb.toString();
    }
}