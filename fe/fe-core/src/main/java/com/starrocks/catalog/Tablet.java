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

    public abstract List<Replica> getAllReplicas();

    public abstract void getQueryableReplicas(List<Replica> allQuerableReplicas, List<Replica> localReplicas,
                                              long visibleVersion, long localBeId, int schemaHash);

    @Override
    public String toString() {
        return "id=" + id;
    }

}
