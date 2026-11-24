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
import com.starrocks.lake.RecycleLakeListPartitionInfo;
import com.starrocks.lake.RecycleLakeRangePartitionInfo;
import com.starrocks.lake.RecycleLakeUnPartitionInfo;
import com.starrocks.lake.StorageInfo;
import com.starrocks.persist.image.ProtoKinds;
import com.starrocks.persist.proto.RecyclePartitionInfoCorePB;
import com.starrocks.persist.proto.RecyclePartitionInfoPB;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

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

    /**
     * Whether this partition is generated from table-level deletion flow.
     * Used to skip partition-level erase edit log because table-level erase log
     * will be recorded after all related partitions are deleted.
     * When true, also force remove the partition directory even if it's a shared directory.
     *
     * Persisted so that after FE restart, partitions mid-way through table deletion still
     * have forceRemoveDirectory=true (avoiding missed shared-dir cleanup) and still suppress
     * per-partition WAL logs (avoiding spurious individual erase logs).
     *
     * Marked volatile because it is written in synchronized eraseTable() but read by
     * async delete tasks submitted in erasePartition() on a different thread.
     */
    @SerializedName(value = "fromTableDeletion")
    protected volatile boolean fromTableDeletion = false;

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

    public boolean isForceRemoveDirectory() {
        // `fromTableDeletion` here indicate whether the partition is generated from table-level deletion flow
        // if true, force remove the partition directory even if it's a shared directory
        return fromTableDeletion;
    }

    public boolean isFromTableDeletion() {
        return fromTableDeletion;
    }

    public void setFromTableDeletion(boolean fromTableDeletion) {
        this.fromTableDeletion = fromTableDeletion;
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

    /**
     * Image v3 form: the fields of this class and of RecyclePartitionInfoV2 go into the core message, the kind names
     * the concrete class, and a class with fields of its own fills its variant.
     */
    public RecyclePartitionInfoPB toProto() throws IOException {
        RecyclePartitionInfoCorePB.Builder core = RecyclePartitionInfoCorePB.newBuilder()
                .setDbId(dbId)
                .setTableId(tableId)
                .setReplicationNum(replicationNum)
                .setRecoverable(recoverable)
                .setRetentionPeriod(retentionPeriod)
                .setFromTableDeletion(fromTableDeletion);
        if (partition != null) {
            core.setPartition(partition.toProto());
        }
        if (dataProperty != null) {
            core.setDataProperty(dataProperty.toProto());
        }
        if (getDataCacheInfo() != null) {
            core.setDataCacheInfo(getDataCacheInfo().toProto());
        }
        RecyclePartitionInfoPB.Builder builder = RecyclePartitionInfoPB.newBuilder().setKind(protoKind()).setCore(core);
        fillProtoVariant(builder);
        return builder.build();
    }

    /** The kind naming the concrete class. Every persistable subclass overrides it. */
    protected RecyclePartitionInfoPB.Kind protoKind() {
        throw new IllegalStateException("recycle partition info class " + getClass().getName() + " cannot be persisted");
    }

    /** Adds the variant message of the concrete class; only a class with fields of its own overrides it. */
    protected void fillProtoVariant(RecyclePartitionInfoPB.Builder builder) throws IOException {
    }

    /** Reads the variant message of the concrete class, after the core has been applied. */
    protected void fillFromVariantProto(RecyclePartitionInfoPB pb) throws IOException {
    }

    public static RecyclePartitionInfo fromProto(RecyclePartitionInfoPB pb) throws IOException {
        ProtoKinds.checkKnown(pb.getKind(), pb.getKindValue());
        RecyclePartitionInfoCorePB core = pb.getCore();
        long dbId = core.getDbId();
        long tableId = core.getTableId();
        Partition partition = core.hasPartition() ? Partition.fromProto(core.getPartition()) : null;
        DataProperty dataProperty = core.hasDataProperty() ? DataProperty.fromProto(core.getDataProperty()) : null;
        short replicationNum = (short) core.getReplicationNum();
        DataCacheInfo dataCacheInfo = core.hasDataCacheInfo() ? DataCacheInfo.fromProto(core.getDataCacheInfo()) : null;
        RecyclePartitionInfo info;
        switch (pb.getKind()) {
            case RECYCLE_RANGE_PARTITION_INFO:
                // The range is the variant, read below.
                info = new RecycleRangePartitionInfo(dbId, tableId, partition, null, dataProperty, replicationNum,
                        dataCacheInfo);
                break;
            case RECYCLE_LAKE_RANGE_PARTITION_INFO:
                info = new RecycleLakeRangePartitionInfo(dbId, tableId, partition, null, dataProperty, replicationNum,
                        dataCacheInfo);
                break;
            case RECYCLE_LIST_PARTITION_INFO:
                info = new RecycleListPartitionInfo(dbId, tableId, partition, dataProperty, replicationNum, dataCacheInfo);
                break;
            case RECYCLE_LAKE_LIST_PARTITION_INFO:
                info = new RecycleLakeListPartitionInfo(dbId, tableId, partition, dataProperty, replicationNum,
                        dataCacheInfo);
                break;
            case RECYCLE_UN_PARTITION_INFO:
                info = new RecycleUnPartitionInfo(dbId, tableId, partition, dataProperty, replicationNum, dataCacheInfo);
                break;
            case RECYCLE_LAKE_UN_PARTITION_INFO:
                info = new RecycleLakeUnPartitionInfo(dbId, tableId, partition, dataProperty, replicationNum,
                        dataCacheInfo);
                break;
            default:
                throw new IllegalArgumentException("unknown recycle partition info kind " + pb.getKind());
        }
        // The constructors preset recoverable (false for the list and unpartitioned classes); the persisted flags win.
        info.recoverable = core.getRecoverable();
        info.retentionPeriod = core.getRetentionPeriod();
        info.fromTableDeletion = core.getFromTableDeletion();
        info.fillFromVariantProto(pb);
        return info;
    }
}
