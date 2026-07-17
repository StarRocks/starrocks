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

package com.starrocks.alter.reshard;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.starrocks.persist.gson.GsonUtils.GSON;

public class ReshardingMaterializedIndexTest {

    @Test
    public void testFreshInstanceHasUnallocatedRecycledIds() {
        ReshardingMaterializedIndex index = new ReshardingMaterializedIndex(1001L, null, Lists.newArrayList());
        // The field initializer runs for objects built via the constructor.
        Assertions.assertEquals(-1L, index.getRecycledVirtualPartitionId());
        Assertions.assertEquals(-1L, index.getRecycledVirtualPhysicalPartitionId());
    }

    @Test
    public void testLegacyJsonDeserializesRecycledIdsAsZero() {
        // A reshard job serialized before the recycled-id fields existed omits them. Gson instantiates
        // via Unsafe (no no-arg constructor), so the -1 field initializer does NOT run and the missing
        // primitive longs deserialize as 0 -- which is why TabletReshardJob's allocation guard must
        // treat <= 0 (not just == -1) as "not yet allocated".
        String legacyJson = "{\"materializedIndexId\":1001}";
        ReshardingMaterializedIndex index = GSON.fromJson(legacyJson, ReshardingMaterializedIndex.class);
        Assertions.assertEquals(1001L, index.getMaterializedIndexId());
        Assertions.assertEquals(0L, index.getRecycledVirtualPartitionId());
        Assertions.assertEquals(0L, index.getRecycledVirtualPhysicalPartitionId());
    }
}
