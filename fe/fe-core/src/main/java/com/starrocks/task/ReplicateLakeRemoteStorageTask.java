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

import com.starrocks.thrift.TReplicateLakeRemoteStorageRequest;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;

public class ReplicateLakeRemoteStorageTask extends AgentTask {
    private final long transactionId;

    private final TTabletType tabletType;
    private final int schemaHash;
    private final long visibleVersion;
    private final long dataVersion;

    private final long srcTabletId;
    private final TTabletType srcTabletType;
    private final int srcSchemaHash;
    private final long srcVisibleVersion;
    private final byte[] encryptionMeta;
    private final long fakedShardId;
    private final long srcDbId;
    private final long srcTableId;
    private final long srcPartitionId;

    public ReplicateLakeRemoteStorageTask(long backendId, long dbId, long tableId, long partitionId, long indexId,
                                          long tabletId, TTabletType tabletType, long transactionId,
                                          int schemaHash, long visibleVersion, long dataVersion,
                                          long srcTabletId, TTabletType srcTabletType,
                                          int srcSchemaHash, long srcVisibleVersion,
                                          byte[] encryptionMeta, long fakedShardId,
                                          long srcDbId, long srcTableId, long srcPartitionId) {
        super(null, backendId, TTaskType.REPLICATE_LAKE_REMOTE_STORAGE, dbId, tableId, partitionId, indexId, tabletId, tabletId,
                System.currentTimeMillis());
        this.transactionId = transactionId;
        this.tabletType = tabletType;
        this.schemaHash = schemaHash;
        this.visibleVersion = visibleVersion;
        this.dataVersion = dataVersion;
        this.srcTabletId = srcTabletId;
        this.srcTabletType = srcTabletType;
        this.srcSchemaHash = srcSchemaHash;
        this.srcVisibleVersion = srcVisibleVersion;
        this.encryptionMeta = encryptionMeta;
        this.fakedShardId = fakedShardId;
        this.srcDbId = srcDbId;
        this.srcTableId = srcTableId;
        this.srcPartitionId = srcPartitionId;
    }

    public TReplicateLakeRemoteStorageRequest toThrift() {
        TReplicateLakeRemoteStorageRequest request = new TReplicateLakeRemoteStorageRequest();
        request.setTransaction_id(transactionId);

        request.setTable_id(tableId);
        request.setPartition_id(partitionId);
        request.setTablet_id(tabletId);
        request.setTablet_type(tabletType);
        request.setSchema_hash(schemaHash);
        request.setVisible_version(visibleVersion);
        request.setData_version(dataVersion);

        request.setSrc_tablet_id(srcTabletId);
        request.setSrc_tablet_type(srcTabletType);
        request.setSrc_schema_hash(srcSchemaHash);
        request.setSrc_visible_version(srcVisibleVersion);
        request.setEncryption_meta(encryptionMeta);
        request.setFaked_shard_id(fakedShardId);

        request.setSrc_db_id(srcDbId);
        request.setSrc_table_id(srcTableId);
        request.setSrc_partition_id(srcPartitionId);
        return request;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("db id: ").append(dbId);
        sb.append(", table id: ").append(tableId);
        sb.append(", partition id: ").append(partitionId);
        sb.append(", transaction id: ").append(transactionId);
        sb.append(", tablet id: ").append(tabletId).append(", tablet type: ").append(tabletType);
        sb.append(", schema hash: ").append(schemaHash);
        sb.append(", visible version: ").append(visibleVersion);
        sb.append(", data version: ").append(dataVersion);
        sb.append(", src tablet type:").append(srcTabletType).append(", src schema hash: ").append(srcSchemaHash);
        sb.append(", src visible version: ").append(srcVisibleVersion);
        sb.append(", src db id: ").append(srcDbId);
        sb.append(", src table id: ").append(srcTableId);
        sb.append(", src partition id: ").append(srcPartitionId);
        return sb.toString();
    }
}
