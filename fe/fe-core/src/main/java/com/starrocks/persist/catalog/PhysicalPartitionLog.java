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
package com.starrocks.persist.catalog;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.PhysicalPartition;

import java.util.Map;

public class PhysicalPartitionLog extends PhysicalPartition {
    @SerializedName(value = "baseIndex")
    private MaterializedIndexLog baseIndex;

    /**
     * Visible rollup indexes are indexes which are visible to user.
     * User can do query on them, show them in related 'show' stmt.
     */
    @SerializedName(value = "idToVisibleRollupIndex")
    private Map<Long, MaterializedIndexLog> idToVisibleRollupIndex = Maps.newHashMap();

    public PhysicalPartitionLog(long id, String name, long parentId, long sharedGroupId, MaterializedIndex baseIndex) {
        super(id, name, parentId, sharedGroupId, baseIndex);
    }
}
