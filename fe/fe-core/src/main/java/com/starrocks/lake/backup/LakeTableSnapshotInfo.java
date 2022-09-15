// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake.backup;

import com.google.gson.annotations.SerializedName;
import com.starrocks.backup.SnapshotInfo;

import java.util.Objects;

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
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        LakeTableSnapshotInfo that = (LakeTableSnapshotInfo) o;
        return version == that.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), version);
    }
}
