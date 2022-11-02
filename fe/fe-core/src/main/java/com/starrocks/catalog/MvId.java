// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.catalog;

public class MvId {
    private final long dbId;
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
    public String toString() {
        return "MvId{" +
                "dbId=" + dbId +
                ", id=" + id +
                '}';
    }
}