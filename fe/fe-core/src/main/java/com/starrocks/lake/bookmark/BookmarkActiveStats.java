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

import java.util.OptionalLong;

/** Immutable snapshot of the bookmark module's currently-active state. */
final class BookmarkActiveStats {

    private final long bookmarkCount;
    private final long referenceCount;
    private final long logicalPartitionCount;
    private final long physicalPartitionCount;
    private final OptionalLong maxBookmarkAgeMs;
    private final OptionalLong maxReferenceAgeMs;

    private BookmarkActiveStats(long bookmarkCount,
                                long referenceCount,
                                long logicalPartitionCount,
                                long physicalPartitionCount,
                                OptionalLong maxBookmarkAgeMs,
                                OptionalLong maxReferenceAgeMs) {
        this.bookmarkCount = bookmarkCount;
        this.referenceCount = referenceCount;
        this.logicalPartitionCount = logicalPartitionCount;
        this.physicalPartitionCount = physicalPartitionCount;
        this.maxBookmarkAgeMs = maxBookmarkAgeMs;
        this.maxReferenceAgeMs = maxReferenceAgeMs;
    }

    long bookmarkCount() {
        return bookmarkCount;
    }

    long referenceCount() {
        return referenceCount;
    }

    long logicalPartitionCount() {
        return logicalPartitionCount;
    }

    long physicalPartitionCount() {
        return physicalPartitionCount;
    }

    OptionalLong maxBookmarkAgeMs() {
        return maxBookmarkAgeMs;
    }

    OptionalLong maxReferenceAgeMs() {
        return maxReferenceAgeMs;
    }

    static Builder newBuilder() {
        return new Builder();
    }

    static final class Builder {
        private long bookmarkCount;
        private long referenceCount;
        private long logicalPartitionCount;
        private long physicalPartitionCount;
        private long maxBookmarkAgeMs = -1L;
        private long maxReferenceAgeMs = -1L;

        Builder addBookmark(long ageMs, long logicalPartitionCount, long physicalPartitionCount) {
            bookmarkCount++;
            this.logicalPartitionCount += logicalPartitionCount;
            this.physicalPartitionCount += physicalPartitionCount;
            if (ageMs > maxBookmarkAgeMs) {
                maxBookmarkAgeMs = ageMs;
            }
            return this;
        }

        Builder addReference(long ageMs) {
            referenceCount++;
            if (ageMs > maxReferenceAgeMs) {
                maxReferenceAgeMs = ageMs;
            }
            return this;
        }

        BookmarkActiveStats build() {
            return new BookmarkActiveStats(
                    bookmarkCount,
                    referenceCount,
                    logicalPartitionCount,
                    physicalPartitionCount,
                    maxBookmarkAgeMs < 0L ? OptionalLong.empty() : OptionalLong.of(maxBookmarkAgeMs),
                    maxReferenceAgeMs < 0L ? OptionalLong.empty() : OptionalLong.of(maxReferenceAgeMs));
        }
    }
}
