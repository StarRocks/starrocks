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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BookmarkTest {

    @Test
    public void testValues() {
        Map<Long, Map<Long, PhysicalPartitionMeta>> parts = new HashMap<>();
        Map<Long, PhysicalPartitionMeta> inner10 = new HashMap<>();
        inner10.put(100L, new PhysicalPartitionMeta(1L, 1L, 5L, 1000L));
        inner10.put(101L, new PhysicalPartitionMeta(2L, 2L, 7L, 1100L));
        parts.put(10L, inner10);
        Map<Long, PhysicalPartitionMeta> inner20 = new HashMap<>();
        inner20.put(200L, new PhysicalPartitionMeta(3L, 3L, 1L, 1200L));
        parts.put(20L, inner20);

        Bookmark b = new Bookmark(1L, 2L, 30L, 9999L, parts);

        assertEquals(1L, b.getDbId());
        assertEquals(2L, b.getTableId());
        assertEquals(30L, b.getBookmarkId());
        assertEquals(9999L, b.getBookmarkTimeMs());

        Optional<PhysicalPartitionMeta> present = b.getPhysicalPartitionMeta(10L, 100L);
        assertTrue(present.isPresent());
        assertEquals(5L, present.get().getVisibleVersion());

        assertFalse(b.getPhysicalPartitionMeta(99L, 100L).isPresent());
        assertFalse(b.getPhysicalPartitionMeta(10L, 999L).isPresent());

        assertEquals(Optional.of(7L), b.getPhysicalPartitionVersion(10L, 101L));
        assertEquals(Optional.empty(), b.getPhysicalPartitionVersion(10L, 999L));

        assertEquals(2, b.getLogicalPartitionCount());
        assertEquals(3, b.getPhysicalPartitionCount());
    }
}
