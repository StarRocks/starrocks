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

package com.starrocks.persist;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.common.io.Writable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Edit log payload for {@link com.starrocks.catalog.ColocateRangeMgr} set mutations.
 *
 * <p>Only "set" updates are journaled; range mgr removal is not journaled because the parent
 * drop/erase journal (e.g. {@code OP_ERASE_TABLE}) already drives followers through
 * {@code ColocateTableIndex.removeTable}, which performs the in-memory cleanup. Emitting a
 * separate removal journal here would race the parent record and could leave followers with
 * a removed range entry but a still-live table, or vice versa, on a leader crash in between.
 */
public class ColocateRangePersistInfo implements Writable {

    @SerializedName("cg")
    private long colocateGroupId;

    @SerializedName("rs")
    private List<ColocateRange> colocateRanges;

    public ColocateRangePersistInfo() {
    }

    public static ColocateRangePersistInfo create(long colocateGroupId, List<ColocateRange> colocateRanges) {
        Preconditions.checkArgument(colocateRanges != null && !colocateRanges.isEmpty(),
                "colocateRanges must be non-empty");
        return new ColocateRangePersistInfo(colocateGroupId, new ArrayList<>(colocateRanges));
    }

    private ColocateRangePersistInfo(long colocateGroupId, List<ColocateRange> colocateRanges) {
        this.colocateGroupId = colocateGroupId;
        this.colocateRanges = colocateRanges;
    }

    public long getColocateGroupId() {
        return colocateGroupId;
    }

    public List<ColocateRange> getColocateRanges() {
        return colocateRanges;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ColocateRangePersistInfo)) {
            return false;
        }
        ColocateRangePersistInfo other = (ColocateRangePersistInfo) obj;
        return colocateGroupId == other.colocateGroupId
                && Objects.equals(colocateRanges, other.colocateRanges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(colocateGroupId, colocateRanges);
    }

    @Override
    public String toString() {
        return "ColocateRangePersistInfo{colocateGroupId=" + colocateGroupId
                + ", colocateRanges=" + colocateRanges + "}";
    }
}
