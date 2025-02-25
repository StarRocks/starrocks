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

package com.starrocks.persist;

import com.google.common.collect.Table;
import com.google.gson.annotations.SerializedName;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.common.io.Writable;

import java.util.Map;

public class LakeTableAsyncFastSchemaChangeJobInfo implements Writable {
    @SerializedName(value = "watershedTxnId")
    private long watershedTxnId;
    @SerializedName(value = "watershedGtid")
    private long watershedGtid;
    // PhysicalPartitionId -> indexId -> MaterializedIndex
    @SerializedName(value = "partitionIndexMap")
    private Table<Long, Long, MaterializedIndex> physicalPartitionIndexMap;
    @SerializedName(value = "commitVersionMap")
    private Map<Long, Long> commitVersionMap;

    @SerializedName(value = "type")
    protected AlterJobV2.JobType type;
    @SerializedName(value = "jobId")
    protected long jobId;
    @SerializedName(value = "jobState")
    protected AlterJobV2.JobState jobState;

    @SerializedName(value = "dbId")
    protected long dbId;
    @SerializedName(value = "tableId")
    protected long tableId;
    @SerializedName(value = "tableName")
    protected String tableName;

    @SerializedName(value = "errMsg")
    protected String errMsg;
    @SerializedName(value = "createTimeMs")
    protected long createTimeMs;
    @SerializedName(value = "finishedTimeMs")
    protected long finishedTimeMs;
    @SerializedName(value = "timeoutMs")
    protected long timeoutMs;
    @SerializedName(value = "warehouseId")
    protected long warehouseId;

    public LakeTableAsyncFastSchemaChangeJobInfo(long watershedTxnId, long watershedGtid,
                                                 Table<Long, Long, MaterializedIndex> physicalPartitionIndexMap,
                                                 Map<Long, Long> commitVersionMap, AlterJobV2.JobType type, long jobId,
                                                 AlterJobV2.JobState jobState, long dbId, long tableId, String tableName,
                                                 String errMsg, long createTimeMs, long finishedTimeMs, long timeoutMs,
                                                 long warehouseId) {
        this.watershedTxnId = watershedTxnId;
        this.watershedGtid = watershedGtid;
        this.physicalPartitionIndexMap = physicalPartitionIndexMap;
        this.commitVersionMap = commitVersionMap;
        this.type = type;
        this.jobId = jobId;
        this.jobState = jobState;
        this.dbId = dbId;
        this.tableId = tableId;
        this.tableName = tableName;
        this.errMsg = errMsg;
        this.createTimeMs = createTimeMs;
        this.finishedTimeMs = finishedTimeMs;
        this.timeoutMs = timeoutMs;
        this.warehouseId = warehouseId;
    }

    public long getWatershedTxnId() {
        return watershedTxnId;
    }

    public long getWatershedGtid() {
        return watershedGtid;
    }

    public Table<Long, Long, MaterializedIndex> getPhysicalPartitionIndexMap() {
        return physicalPartitionIndexMap;
    }

    public Map<Long, Long> getCommitVersionMap() {
        return commitVersionMap;
    }

    public AlterJobV2.JobType getType() {
        return type;
    }

    public long getJobId() {
        return jobId;
    }

    public AlterJobV2.JobState getJobState() {
        return jobState;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public long getCreateTimeMs() {
        return createTimeMs;
    }

    public long getFinishedTimeMs() {
        return finishedTimeMs;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public long getWarehouseId() {
        return warehouseId;
    }
}
