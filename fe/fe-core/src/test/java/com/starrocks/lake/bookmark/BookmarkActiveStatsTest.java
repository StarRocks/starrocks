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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BookmarkActiveStatsTest {

    @Test
    public void testBuilder() {
        // Empty build — counts at zero, ages absent.
        BookmarkActiveStats empty = BookmarkActiveStats.newBuilder().build();
        assertEquals(0L, empty.bookmarkCount());
        assertEquals(0L, empty.referenceCount());
        assertEquals(0L, empty.logicalPartitionCount());
        assertEquals(0L, empty.physicalPartitionCount());
        assertFalse(empty.maxBookmarkAgeMs().isPresent());
        assertFalse(empty.maxReferenceAgeMs().isPresent());

        // addBookmark twice with different ages and partition counts —
        // counts sum, max age takes the larger.
        BookmarkActiveStats two = BookmarkActiveStats.newBuilder()
                .addBookmark(100L, 3L, 5L)
                .addBookmark(50L, 2L, 4L)
                .build();
        assertEquals(2L, two.bookmarkCount());
        assertEquals(5L, two.logicalPartitionCount());
        assertEquals(9L, two.physicalPartitionCount());
        assertTrue(two.maxBookmarkAgeMs().isPresent());
        assertEquals(100L, two.maxBookmarkAgeMs().getAsLong());

        // addReference twice — max age takes the larger.
        BookmarkActiveStats refs = BookmarkActiveStats.newBuilder()
                .addReference(10L)
                .addReference(80L)
                .build();
        assertEquals(2L, refs.referenceCount());
        assertTrue(refs.maxReferenceAgeMs().isPresent());
        assertEquals(80L, refs.maxReferenceAgeMs().getAsLong());
    }

}
