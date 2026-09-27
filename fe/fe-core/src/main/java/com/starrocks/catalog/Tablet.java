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

    @SerializedName(value = "range")
    protected TabletRange range;

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

    /**
     * This tablet's row count, but only when a stat collection proved it was computed from exactly
     * {@code version}; -1 means "cannot vouch for that version" and is the answer for a tablet kind
     * that cannot prove coverage at all.
     * <p>
     * Row counts are NOT journal-replicated: every FE rebuilds them from its own TabletStatMgr
     * cycle, and a failed, skipped, or stale-snapshot stat RPC silently leaves the previous value in
     * place. So a caller that must be exact (see RewriteSimpleAggToMetaScanRule's COUNT(*) constant
     * fold) asks for the count and its proof together, and falls back to a real meta scan when the
     * tablets cannot produce one. Reading a count and a version separately is exactly the mistake
     * this method exists to prevent: the two are refreshed by different code at different times, so
     * a version fetched next to a count does not necessarily describe it.
     * <p>
     * Deliberately a version and not a timestamp. Versions are assigned by the leader FE and
     * replicated through the journal, so they are comparable across machines; a wall clock is not.
     * visibleVersionTime looks like a timestamp but is really a replicated label -- comparing it
     * against any local System.currentTimeMillis()/UnixMillis() (on a BE, or on a follower whose
     * clock differs from the leader's) would make correctness depend on clock skew.
     */
    public long getRowCountAtVersion(long version) {
        return -1L;
    }

    public long getFuzzyRowCount() {
        return 1L;
    }

    public abstract Set<Long> getBackendIds();

    public abstract List<Replica> getAllReplicas();

    public abstract void getQueryableReplicas(List<Replica> allQuerableReplicas, List<Replica> localReplicas,
                                              long visibleVersion, long localBeId, int schemaHash);

    public abstract void getQueryableReplicas(List<Replica> allQuerableReplicas, List<Replica> localReplicas,
                                              long visibleVersion, long localBeId, int schemaHash,
                                              ComputeResource computeResource, List<Long> locations);

    @Override
    public String toString() {
        return "id=" + id;
    }
}
