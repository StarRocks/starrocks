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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PhysicalPartitionMetaTest {

    @Test
    public void testValues() {
        PhysicalPartitionMeta m = new PhysicalPartitionMeta(11L, 12L, 5L, 1_700_000_000_000L);
        assertEquals(11L, m.getBaseMaterializedIndexId());
        assertEquals(12L, m.getBaseMaterializedIndexMetaId());
        assertEquals(5L, m.getVisibleVersion());
        assertEquals(1_700_000_000_000L, m.getVisibleVersionTimeMs());
    }
}
