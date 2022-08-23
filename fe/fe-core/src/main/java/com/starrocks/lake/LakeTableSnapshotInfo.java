// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake;

import com.google.gson.annotations.SerializedName;
import com.starrocks.backup.SnapshotInfo;

public class LakeTableSnapshotInfo extends SnapshotInfo {
    @SerializedName(value = "version")
    private long version;

    public LakeTableSnapshotInfo(long dbId, long tblId, long partitionId, long indexId, long tabletId,
                                 long beId, int schemaHash, long version) {
        super(dbId, tblId, partitionId, indexId, tabletId, beId, schemaHash, "", null);
        this.version = version;
    }

    public long getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }

        if (!(o instanceof LakeTableSnapshotInfo)) {
            return false;
        }

        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        LakeTableSnapshotInfo snapshotInfo = (LakeTableSnapshotInfo) o;
        return this.version == snapshotInfo.version;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
