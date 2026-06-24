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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

/** Immutable snapshot the cleanup daemon takes each cycle to drive the age gauges and the TTL sweep. */
final class BookmarkActiveStats {

    private final OptionalLong maxBookmarkAgeMs;
    private final OptionalLong maxReferenceAgeMs;
    // Bookmarks holding at least one expired reference, keyed db id -> table id -> bookmark ids.
    private final Map<Long, Map<Long, Set<Long>>> bookmarksWithExpiredReferences;

    private BookmarkActiveStats(OptionalLong maxBookmarkAgeMs,
                                OptionalLong maxReferenceAgeMs,
                                Map<Long, Map<Long, Set<Long>>> bookmarksWithExpiredReferences) {
        this.maxBookmarkAgeMs = maxBookmarkAgeMs;
        this.maxReferenceAgeMs = maxReferenceAgeMs;
        this.bookmarksWithExpiredReferences = bookmarksWithExpiredReferences;
    }

    OptionalLong maxBookmarkAgeMs() {
        return maxBookmarkAgeMs;
    }

    OptionalLong maxReferenceAgeMs() {
        return maxReferenceAgeMs;
    }

    Map<Long, Map<Long, Set<Long>>> bookmarksWithExpiredReferences() {
        return bookmarksWithExpiredReferences;
    }

    static Builder newBuilder() {
        return new Builder();
    }

    static final class Builder {
        private long maxBookmarkAgeMs = -1L;
        private long maxReferenceAgeMs = -1L;
        private final Map<Long, Map<Long, Set<Long>>> bookmarksWithExpiredReferences = new HashMap<>();

        Builder addBookmarkAge(long ageMs) {
            if (ageMs > maxBookmarkAgeMs) {
                maxBookmarkAgeMs = ageMs;
            }
            return this;
        }

        Builder addReferenceAge(long ageMs) {
            if (ageMs > maxReferenceAgeMs) {
                maxReferenceAgeMs = ageMs;
            }
            return this;
        }

        Builder addBookmarkWithExpiredReference(long dbId, long tableId, long bookmarkId) {
            bookmarksWithExpiredReferences
                    .computeIfAbsent(dbId, k -> new HashMap<>())
                    .computeIfAbsent(tableId, k -> new HashSet<>())
                    .add(bookmarkId);
            return this;
        }

        BookmarkActiveStats build() {
            return new BookmarkActiveStats(
                    maxBookmarkAgeMs < 0L ? OptionalLong.empty() : OptionalLong.of(maxBookmarkAgeMs),
                    maxReferenceAgeMs < 0L ? OptionalLong.empty() : OptionalLong.of(maxReferenceAgeMs),
                    bookmarksWithExpiredReferences);
        }
    }
}
