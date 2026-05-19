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

package com.starrocks.lake.ivm;

import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableDeltaTrait;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkChange;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Adapter that exposes a BookmarkChange between two Lake-table bookmarks to
 * IVM as a single TvrTableDeltaTrait covering the whole {@code (base, head]}
 * range. The trait is MONOTONIC when every per-partition diff is {@code ADDED}
 * or {@code DATA_CHANGED} (including the vacuous "no diff" case where
 * endpoints have equivalent partition meta but bookmark ids still advance);
 * RETRACTABLE otherwise so IVM falls back to a non-incremental refresh.
 */
public final class BookmarkChangeTvrAdapter {

    private BookmarkChangeTvrAdapter() {
    }

    /**
     * @param base the prior bookmark, or {@code null} for first refresh
     *             (every physical partition in {@code head} is {@code ADDED})
     * @param head the current bookmark; must be non-null
     * @return single-element list with the trait for the whole {@code (base, head]}
     *         range
     */
    public static List<TvrTableDeltaTrait> toTvrTraits(@Nullable Bookmark base, Bookmark head) {
        BookmarkChange change = BookmarkChange.computeChanges(base, head);
        TvrTableDelta delta = TvrTableDelta.of(
                base == null ? Optional.empty() : Optional.of(base.getBookmarkId()),
                Optional.of(head.getBookmarkId()));
        // isTrackable() is vacuously true when the change set is empty, so the
        // "equivalent endpoints but different bookmark ids" case (e.g. ADD then
        // DROP of the same partition between base and head — bookmark dedup
        // only consults the latest active, so older equivalent states get
        // fresh ids) naturally maps to MONOTONIC. IVM treats it as a no-op
        // refresh that advances the version pointer.
        return List.of(change.isTrackable()
                ? TvrTableDeltaTrait.ofMonotonic(delta)
                : TvrTableDeltaTrait.ofRetractable(delta));
    }
}
