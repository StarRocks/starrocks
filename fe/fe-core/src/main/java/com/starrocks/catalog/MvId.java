// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class MvId {
    @SerializedName(value = "dbId")
    private final long dbId;
    @SerializedName(value = "id")
    private final long id;

    public MvId(long dbId, long id) {
        this.dbId = dbId;
        this.id = id;
    }

    public long getDbId() {
        return dbId;
    }

    public long getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MvId mvId = (MvId) o;
        return dbId == mvId.dbId && id == mvId.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbId, id);
    }

    @Override
    public String toString() {
        return "MvId{" +
                "dbId=" + dbId +
                ", id=" + id +
                '}';
    }
}