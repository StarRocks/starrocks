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

package com.starrocks.transaction;

import com.google.gson.annotations.SerializedName;
import com.starrocks.load.loadv2.IVMRefreshCommitInfo;
import com.starrocks.persist.proto.InsertTxnCommitAttachmentPB;
import com.starrocks.persist.proto.TxnCommitAttachmentPB;

public class InsertTxnCommitAttachment extends TxnCommitAttachment {
    @SerializedName("loadedRows")
    private long loadedRows;

    @SerializedName("isVersionOverwrite")
    private boolean isVersionOverwrite = false;

    @SerializedName("partitionVersion")
    private long partitionVersion;

    // For a shadow-rewrite transaction: the watershed txn id the converted
    // op_schema_change log must be keyed by, and the alter version it is anchored at.
    @SerializedName("shadowRewriteWatershedTxnId")
    private long shadowRewriteWatershedTxnId = 0;

    @SerializedName("shadowRewriteAlterVersion")
    private long shadowRewriteAlterVersion = 0;

    // Set only by an IVM refresh; null for every other INSERT.
    @SerializedName("ivmRefreshCommitInfo")
    private IVMRefreshCommitInfo ivmRefreshCommitInfo;

    // Set when a column-subset INSERT into a primary key table was planned as a plain insert because
    // every target partition had never been committed to. The commit re-checks that under the
    // transaction manager lock, since another transaction may have committed to those partitions
    // in between, and fails the commit if so (see DatabaseTransactionMgr.commitPreparedTransaction).
    @SerializedName("requireEmptyPartitions")
    private boolean requireEmptyPartitions = false;

    public InsertTxnCommitAttachment() {
        super(TransactionState.LoadJobSourceType.INSERT_STREAMING);
    }

    public InsertTxnCommitAttachment(long loadedRows) {
        super(TransactionState.LoadJobSourceType.INSERT_STREAMING);
        this.loadedRows = loadedRows;
    }

    public InsertTxnCommitAttachment(long loadedRows, long partitionVersion) {
        this(loadedRows);
        this.isVersionOverwrite = true;
        this.partitionVersion = partitionVersion;
    }

    public long getLoadedRows() {
        return loadedRows;
    }

    public boolean getIsVersionOverwrite() {
        return isVersionOverwrite;
    }

    public long getPartitionVersion() {
        return partitionVersion;
    }

    public void setShadowRewriteWatershedTxnId(long shadowRewriteWatershedTxnId) {
        this.shadowRewriteWatershedTxnId = shadowRewriteWatershedTxnId;
    }

    public long getShadowRewriteWatershedTxnId() {
        return shadowRewriteWatershedTxnId;
    }

    public void setShadowRewriteAlterVersion(long shadowRewriteAlterVersion) {
        this.shadowRewriteAlterVersion = shadowRewriteAlterVersion;
    }

    public long getShadowRewriteAlterVersion() {
        return shadowRewriteAlterVersion;
    }

    public void setIvmRefreshCommitInfo(IVMRefreshCommitInfo ivmRefreshCommitInfo) {
        this.ivmRefreshCommitInfo = ivmRefreshCommitInfo;
    }

    public IVMRefreshCommitInfo getIvmRefreshCommitInfo() {
        return ivmRefreshCommitInfo;
    }

    public void setRequireEmptyPartitions(boolean requireEmptyPartitions) {
        this.requireEmptyPartitions = requireEmptyPartitions;
    }

    public boolean getRequireEmptyPartitions() {
        return requireEmptyPartitions;
    }

    @Override
    protected void fillFromVariantProto(TxnCommitAttachmentPB parent) {
        InsertTxnCommitAttachmentPB pb = parent.getInsert();
        if (pb.hasLoadedRows()) {
            loadedRows = pb.getLoadedRows();
        }
        if (pb.hasIsVersionOverwrite()) {
            isVersionOverwrite = pb.getIsVersionOverwrite();
        }
        if (pb.hasPartitionVersion()) {
            partitionVersion = pb.getPartitionVersion();
        }
        if (pb.hasShadowRewriteWatershedTxnId()) {
            shadowRewriteWatershedTxnId = pb.getShadowRewriteWatershedTxnId();
        }
        if (pb.hasShadowRewriteAlterVersion()) {
            shadowRewriteAlterVersion = pb.getShadowRewriteAlterVersion();
        }
        ivmRefreshCommitInfo = pb.hasIvmRefreshCommitInfo()
                ? IVMRefreshCommitInfo.fromProto(pb.getIvmRefreshCommitInfo()) : null;
        requireEmptyPartitions = pb.getRequireEmptyPartitions();
    }

    @Override
    protected void fillProtoVariant(TxnCommitAttachmentPB.Builder parent) {
        InsertTxnCommitAttachmentPB.Builder builder = InsertTxnCommitAttachmentPB.newBuilder();
        builder.setLoadedRows(loadedRows);
        builder.setIsVersionOverwrite(isVersionOverwrite);
        builder.setPartitionVersion(partitionVersion);
        builder.setShadowRewriteWatershedTxnId(shadowRewriteWatershedTxnId);
        builder.setShadowRewriteAlterVersion(shadowRewriteAlterVersion);
        builder.setRequireEmptyPartitions(requireEmptyPartitions);
        if (ivmRefreshCommitInfo != null) {
            builder.setIvmRefreshCommitInfo(ivmRefreshCommitInfo.toProto());
        }
        parent.setKind(TxnCommitAttachmentPB.Kind.INSERT).setInsert(builder);
    }
}
