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

package com.starrocks.lake.bookmark;

import com.google.gson.annotations.SerializedName;

/**
 * The fixed identity of a physical partition's base materialized index plus
 * its visible version at the moment a bookmark records it. The pair
 * {@code (baseMaterializedIndexId, baseMaterializedIndexMetaId)} together
 * identify the base materialized index instance and change whenever the
 * underlying index is replaced.
 */
public final class PhysicalPartitionMeta {
    @SerializedName("i")
    private final long baseMaterializedIndexId;
    @SerializedName("mi")
    private final long baseMaterializedIndexMetaId;
    @SerializedName("v")
    private final long visibleVersion;
    @SerializedName("vt")
    private final long visibleVersionTimeMs;

    public PhysicalPartitionMeta(long baseMaterializedIndexId, long baseMaterializedIndexMetaId,
                                 long visibleVersion, long visibleVersionTimeMs) {
        this.baseMaterializedIndexId = baseMaterializedIndexId;
        this.baseMaterializedIndexMetaId = baseMaterializedIndexMetaId;
        this.visibleVersion = visibleVersion;
        this.visibleVersionTimeMs = visibleVersionTimeMs;
    }

    public long getBaseMaterializedIndexId() {
        return baseMaterializedIndexId;
    }

    public long getBaseMaterializedIndexMetaId() {
        return baseMaterializedIndexMetaId;
    }

    public long getVisibleVersion() {
        return visibleVersion;
    }

    public long getVisibleVersionTimeMs() {
        return visibleVersionTimeMs;
    }
}
