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

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Range;
import com.starrocks.common.io.Writable;
import com.starrocks.warehouse.cngroup.ComputeResource;

import java.util.List;
import java.util.Set;

/**
 * This abstract class represents the base olap tablet related metadata.
 */
public abstract class Tablet extends MetaObject implements Writable {
    protected static final String JSON_KEY_ID = "id";

    @SerializedName(value = JSON_KEY_ID)
    protected long id;

    /*
     * Add the serialization when the feature is almost finished
     * @SerializedName(value = "range")
    */
    protected TabletRange range = new TabletRange(Range.all());

    public Tablet() {
    }

    public Tablet(long id) {
        this.id = id;
    }

    public Tablet(long id, TabletRange range) {
        this.id = id;
        this.range = range;
    }

    public long getId() {
        return id;
    }

    public TabletRange getRange() {
        return range;
    }

    public void setRange(TabletRange range) {
        this.range = range;
    }

    public abstract long getDataSize(boolean singleReplica);

    public abstract long getRowCount(long version);

    public long getFuzzyRowCount() {
        return 1L;
    }

    public abstract Set<Long> getBackendIds();

    public abstract List<Replica> getAllReplicas();

    public abstract void getQueryableReplicas(List<Replica> allQuerableReplicas, List<Replica> localReplicas,
                                              long visibleVersion, long localBeId, int schemaHash);

    public abstract void getQueryableReplicas(List<Replica> allQuerableReplicas, List<Replica> localReplicas,
                                              long visibleVersion, long localBeId, int schemaHash,
                                              ComputeResource computeResource);

    @Override
    public String toString() {
        return "id=" + id;
    }
}
