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

import com.starrocks.persist.gson.GsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RecycleMaterializedIndexInfoTest {
    @Test
    public void testNewFieldsAndLegacyDefaults() {
        RecycleMaterializedIndexInfo info = new RecycleMaterializedIndexInfo(1, 2, 3, 4, 5, 42);
        Assertions.assertEquals(3, info.getLogicalPartitionId());
        Assertions.assertEquals(42, info.getSupersededAtVersion());
        RecycleMaterializedIndexInfo reloaded = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(info), RecycleMaterializedIndexInfo.class);
        Assertions.assertEquals(3, reloaded.getLogicalPartitionId());
        Assertions.assertEquals(42, reloaded.getSupersededAtVersion());
        // A pre-upgrade image entry lacks both fields -> 0 -> never bookmark-gated.
        RecycleMaterializedIndexInfo legacy = GsonUtils.GSON.fromJson(
                "{\"dbId\":1,\"tableId\":2,\"physicalPartitionId\":4,\"indexId\":5}",
                RecycleMaterializedIndexInfo.class);
        Assertions.assertEquals(0, legacy.getLogicalPartitionId());
        Assertions.assertEquals(0, legacy.getSupersededAtVersion());
    }
}
