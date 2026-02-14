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

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.JsonWriter;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.StorageInfo;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class RecyclePartitionInfo extends JsonWriter {
    private static final Logger LOG = LogManager.getLogger(RecyclePartitionInfo.class);
    @SerializedName(value = "dbId")
    protected long dbId;
    @SerializedName(value = "tableId")
    protected long tableId;
    @SerializedName(value = "partition")
    protected Partition partition;
    @SerializedName(value = "dataProperty")
    protected DataProperty dataProperty;
    @SerializedName(value = "replicationNum")
    protected short replicationNum;
    @SerializedName(value = "recoverable")
    protected boolean recoverable;

    /**
     * partition retention period in `second` unit
     */
    @SerializedName(value = "retentionPeriod")
    protected long retentionPeriod = 0L;

    public RecyclePartitionInfo() {
        recoverable = true;
    }

    public RecyclePartitionInfo(long dbId, long tableId, Partition partition,
                                DataProperty dataProperty, short replicationNum) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partition = partition;
        this.dataProperty = dataProperty;
        this.replicationNum = replicationNum;
        this.recoverable = true;
        this.retentionPeriod = 0L;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public Partition getPartition() {
        return partition;
    }

    public DataProperty getDataProperty() {
        return dataProperty;
    }

    public short getReplicationNum() {
        return replicationNum;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public boolean isRecoverable() {
        return recoverable;
    }

    public void setRecoverable(boolean recoverable) {
        this.recoverable = recoverable;
    }

    public void setRetentionPeriod(long retentionPeriod) {
        this.retentionPeriod = retentionPeriod;
    }

    public long getRetentionPeriod() {
        return retentionPeriod;
    }

    public boolean delete() {
        GlobalStateMgr.getCurrentState().getLocalMetastore().onErasePartition(partition);
        return true;
    }

    abstract Range<PartitionKey> getRange();

    abstract DataCacheInfo getDataCacheInfo();

    abstract void checkRecoverable(OlapTable table) throws DdlException;

    abstract void recover(OlapTable table);

    public void checkRecoverableForRangePartition(OlapTable table) throws DdlException {
        Preconditions.checkState(this.isRecoverable());
        // check if range is invalid
        final String partitionName = this.getPartition().getName();
        Range<PartitionKey> recoverRange = this.getRange();
        RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
        if (partitionInfo.getAnyIntersectRange(recoverRange, false) != null) {
            throw new DdlException("Cannot recover partition '" + partitionName + "': Range conflict.");
        }
    }

    protected void recoverRangePartition(OlapTable table) {
        // recover partition
        Partition recoverPartition = this.getPartition();
        table.addPartition(recoverPartition);

        // recover partition info
        Range<PartitionKey> recoverRange = this.getRange();
        RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
        long partitionId = recoverPartition.getId();
        partitionInfo.setRange(partitionId, false, recoverRange);
        partitionInfo.setDataProperty(partitionId, this.getDataProperty());
        partitionInfo.setReplicationNum(partitionId, this.getReplicationNum());
        if (this.getDataCacheInfo() != null) {
            partitionInfo.setDataCacheInfo(partitionId, this.getDataCacheInfo());
        }

        syncDataCacheInfoWithTable(table, partitionInfo, partitionId);
    }

    /**
     * Synchronize the recovered partition's DataCacheInfo with the table's current
     * datacache.enable property. This handles the scenario where:
     * 1. User drops a partition (non-force, so it goes to recycle bin)
     * 2. User alters the table's datacache.enable property
     * 3. User recovers the partition from recycle bin
     * The recovered partition should reflect the table's current datacache.enable setting.
     */
    protected void syncDataCacheInfoWithTable(OlapTable table, PartitionInfo partitionInfo, long partitionId) {
        if (!table.isCloudNativeTableOrMaterializedView()) {
            return;
        }
        TableProperty tableProperty = table.getTableProperty();
        if (tableProperty == null) {
            return;
        }
        StorageInfo storageInfo = tableProperty.getStorageInfo();
        if (storageInfo == null) {
            return;
        }
        boolean tableDataCacheEnable = storageInfo.isEnableDataCache();
        DataCacheInfo currentDataCacheInfo = partitionInfo.getDataCacheInfo(partitionId);
        boolean asyncWriteBack = currentDataCacheInfo != null && currentDataCacheInfo.isAsyncWriteBack();

        if (currentDataCacheInfo == null || currentDataCacheInfo.isEnabled() != tableDataCacheEnable) {
            partitionInfo.setDataCacheInfo(partitionId, new DataCacheInfo(tableDataCacheEnable, asyncWriteBack));
            LOG.info("Synced recovered partition {} DataCacheInfo with table's current datacache.enable={}",
                    partitionId, tableDataCacheEnable);
        }
    }
}
