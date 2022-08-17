// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake.compaction;

import com.google.gson.Gson;

import java.util.Objects;

public class PartitionIdentifier {
    private final long dbId;
    private final long tableId;
    private final long partitionId;

    public PartitionIdentifier(long dbId, long tableId, long partitionId) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionIdentifier that = (PartitionIdentifier) o;
        return dbId == that.dbId && tableId == that.tableId && partitionId == that.partitionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbId, tableId, partitionId);
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
