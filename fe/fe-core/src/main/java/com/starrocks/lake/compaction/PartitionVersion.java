// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake.compaction;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class PartitionVersion {
    @SerializedName(value = "version")
    private final long version;
    @SerializedName(value = "createTime")
    private final long createTime;

    public PartitionVersion(long version, long createTime) {
        this.version = version;
        this.createTime = createTime;
    }

    long getVersion() {
        return version;
    }

    long getCreateTime() {
        return createTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionVersion that = (PartitionVersion) o;
        return version == that.version && createTime == that.createTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, createTime);
    }
}
