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

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BookmarkActiveStatsTest {

    @Test
    public void testBuilder() {
        // Empty build — ages absent, no expired bookmarks.
        BookmarkActiveStats empty = BookmarkActiveStats.newBuilder().build();
        assertFalse(empty.maxBookmarkAgeMs().isPresent());
        assertFalse(empty.maxReferenceAgeMs().isPresent());
        assertTrue(empty.bookmarksWithExpiredReferences().isEmpty());

        // Each max age takes the larger of the values added; the expired-bookmark
        // map is keyed db -> table -> bookmark ids.
        BookmarkActiveStats stats = BookmarkActiveStats.newBuilder()
                .addBookmarkAge(100L)
                .addBookmarkAge(50L)
                .addReferenceAge(10L)
                .addReferenceAge(80L)
                .addBookmarkWithExpiredReference(1L, 2L, 3L)
                .build();
        assertEquals(100L, stats.maxBookmarkAgeMs().getAsLong());
        assertEquals(80L, stats.maxReferenceAgeMs().getAsLong());
        assertEquals(Set.of(3L), stats.bookmarksWithExpiredReferences().get(1L).get(2L));
    }

}
