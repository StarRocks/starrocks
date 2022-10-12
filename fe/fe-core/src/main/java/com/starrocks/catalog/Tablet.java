// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;

import java.util.List;
import java.util.Set;

/**
 * This abstract class represents the base olap tablet related metadata.
 */
public abstract class Tablet extends MetaObject implements Writable {
    protected static final String JSON_KEY_ID = "id";

    @SerializedName(value = JSON_KEY_ID)
    protected long id;

    public Tablet(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public abstract long getDataSize(boolean singleReplica);

    public abstract long getRowCount(long version);

    public abstract Set<Long> getBackendIds();

    public abstract void getQueryableReplicas(List<Replica> allQuerableReplicas, List<Replica> localReplicas,
                                              long visibleVersion, long localBeId, int schemaHash);

    @Override
    public String toString() {
        return "id=" + id;
    }

}
