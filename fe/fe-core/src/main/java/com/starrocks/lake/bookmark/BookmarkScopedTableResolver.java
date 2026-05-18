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

import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.lake.LakeMaterializedView;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.bookmark.BookmarkChange.IndexReplaced;
import com.starrocks.lake.bookmark.BookmarkChange.PartitionDropped;
import com.starrocks.lake.bookmark.BookmarkChange.PhysicalPartitionChange;
import com.starrocks.lake.bookmark.BookmarkChange.TabletReshard;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Builds an isolated, query-only OlapTable whose physical partitions carry the
 * versions captured in a Bookmark. The caller opts in via the
 * {@code [_BOOKMARK_<id>_]} bracket hint on the table reference; cloud-native
 * OlapTables have no other AS-OF entry point.
 */
public final class BookmarkScopedTableResolver {

    /**
     * Returns a scoped OlapTable for the Bookmark with id {@code bookmarkId}.
     * Throws SemanticException if no Bookmark with that id is registered for
     * the live table.
     */
    public static OlapTable resolveById(OlapTable live, long bookmarkId) {
        long dbId = live.mayGetDatabaseId().orElseThrow(
                () -> new IllegalStateException(String.format("dbId missing on %s", live.getName())));
        Bookmark bookmark = GlobalStateMgr.getCurrentState().getBookmarkManager()
                .findBookmarkById(dbId, live.getId(), bookmarkId)
                .orElseThrow(() -> new SemanticException(String.format(
                        "bookmark %d not found on table '%s'", bookmarkId, live.getName())));
        return resolve(live, bookmark);
    }

    /**
     * Returns a scoped OlapTable for the latest Bookmark on the live table whose
     * creation time is at or before {@code timestampMs}. Throws SemanticException
     * if no such Bookmark exists. {@code timestampMs} is a Bookmark creation time,
     * not a DML time.
     *
     * <p>Not reached from SQL today (the {@code [_BOOKMARK_<id>_]} hint only
     * carries a numeric id); kept as an internal entry point for future
     * timestamp-keyed bookmark resolution.
     */
    public static OlapTable resolveByTimestamp(OlapTable live, long timestampMs) {
        long dbId = live.mayGetDatabaseId().orElseThrow(
                () -> new IllegalStateException(String.format("dbId missing on %s", live.getName())));
        Bookmark bookmark = GlobalStateMgr.getCurrentState().getBookmarkManager()
                .findByTimestamp(dbId, live.getId(), timestampMs)
                .orElseThrow(() -> new SemanticException(String.format(
                        "no bookmark for table '%s' at or before %d", live.getName(), timestampMs)));
        return resolve(live, bookmark);
    }

    private static OlapTable resolve(OlapTable live, Bookmark bookmark) {
        OlapTable shadow;
        if (live instanceof LakeMaterializedView) {
            shadow = new LakeMaterializedView();
        } else if (live instanceof LakeTable) {
            shadow = new LakeTable();
        } else {
            throw new IllegalStateException(String.format(
                    "bookmark resolution requires a cloud-native table, got %s on '%s'",
                    live.getClass().getSimpleName(), live.getName()));
        }

        BookmarkScopedTableRewriter rewriter = new BookmarkScopedTableRewriter(bookmark);
        live.buildBookmarkScopedTable(shadow, rewriter);

        long bookmarkId = bookmark.getBookmarkId();
        String tableName = live.getName();
        BookmarkChange nonTrackableChanges = rewriter.collectNonTrackableChanges();
        Map<Long, List<PhysicalPartitionChange>> changesByLogical = nonTrackableChanges.getChanges();

        for (Map.Entry<Long, List<PhysicalPartitionChange>> logicalEntry : changesByLogical.entrySet()) {
            List<PhysicalPartitionChange> changesPerLogical = logicalEntry.getValue();
            for (PhysicalPartitionChange change : changesPerLogical) {
                long logicalId = change.getLogicalPartitionId();
                long physicalId = change.getPhysicalPartitionId();
                if (change instanceof PartitionDropped) {
                    throw new SemanticException(String.format(
                            "physical partition %d in bookmark %d no longer exists on table '%s'",
                            physicalId, bookmarkId, tableName));
                }
                String partitionName = live.getPartition(logicalId).getName();
                if (change instanceof IndexReplaced) {
                    throw new SemanticException(String.format(
                            "bookmark %d is no longer queryable: partition '%s' has been modified in a way that rewrote its data",
                            bookmarkId, partitionName));
                }
                if (change instanceof TabletReshard) {
                    throw new SemanticException(String.format(
                            "bookmark %d is no longer queryable: partition '%s' has been redistributed",
                            bookmarkId, partitionName));
                }
            }
        }
        return shadow;
    }

    /**
     * Rewriter for a single resolve call. Returns a version-stamped copy when
     * the bookmark records the physical with matching base-index identity,
     * otherwise empty.
     *
     * <p>Physicals added since the bookmark are dropped silently (trackable).
     * Base-index drifts — {@link IndexReplaced} (the index was replaced under
     * the same logical) or {@link TabletReshard} (its tablets were resharded)
     * — are dropped AND recorded as non-trackable changes;
     * {@code collectNonTrackableChanges()} then appends one
     * {@link PartitionDropped} per bookmark physical the rewriter never saw,
     * producing the full bookmark-to-live diff for the resolver to translate
     * into errors.
     */
    private static final class BookmarkScopedTableRewriter implements BookmarkPartitionRewriter {
        private final Bookmark bookmark;
        private final Map<Long, Set<Long>> rewrittenPhysicalPartitionIdsByLogicalId = new HashMap<>();
        private final List<PhysicalPartitionChange> nonTrackableChangesSeenDuringRewrite = new ArrayList<>();

        BookmarkScopedTableRewriter(Bookmark bookmark) {
            this.bookmark = bookmark;
        }

        @Override
        public Optional<PhysicalPartition> rewrite(Partition partition, PhysicalPartition physical) {
            long logicalId = partition.getId();
            long physicalId = physical.getId();

            Map<Long, PhysicalPartitionMeta> physicalMetas =
                    bookmark.getPartitionsMeta().get(logicalId);
            PhysicalPartitionMeta bookmarkPartitionMeta =
                    physicalMetas == null ? null : physicalMetas.get(physicalId);
            if (bookmarkPartitionMeta == null) {
                return Optional.empty();
            }

            rewrittenPhysicalPartitionIdsByLogicalId
                    .computeIfAbsent(logicalId, k -> new HashSet<>())
                    .add(physicalId);

            MaterializedIndex liveBaseIndex = physical.getLatestBaseIndex();
            PhysicalPartitionMeta livePartitionMeta = livePhysicalMeta(physical, liveBaseIndex);
            if (bookmarkPartitionMeta.getBaseMaterializedIndexMetaId() != liveBaseIndex.getMetaId()) {
                nonTrackableChangesSeenDuringRewrite.add(new IndexReplaced(
                        logicalId, physicalId, bookmarkPartitionMeta, livePartitionMeta));
                return Optional.empty();
            }
            if (bookmarkPartitionMeta.getBaseMaterializedIndexId() != liveBaseIndex.getId()) {
                nonTrackableChangesSeenDuringRewrite.add(new TabletReshard(
                        logicalId, physicalId, bookmarkPartitionMeta, livePartitionMeta));
                return Optional.empty();
            }
            return Optional.of(physical.copyForBookmark(
                    bookmarkPartitionMeta.getVisibleVersion(),
                    bookmarkPartitionMeta.getVisibleVersionTimeMs()));
        }

        /**
         * Returns the bookmark→live diff containing all non-trackable changes:
         * the drifts recorded during rewrite plus a {@link PartitionDropped}
         * per bookmark physical id that was never rewritten (the physical was
         * removed from live since the bookmark was taken).
         */
        BookmarkChange collectNonTrackableChanges() {
            Map<Long, List<PhysicalPartitionChange>> changesByLogical = new HashMap<>();
            for (PhysicalPartitionChange change : nonTrackableChangesSeenDuringRewrite) {
                changesByLogical
                        .computeIfAbsent(change.getLogicalPartitionId(), k -> new ArrayList<>())
                        .add(change);
            }
            Map<Long, Map<Long, PhysicalPartitionMeta>> partitionsMeta = bookmark.getPartitionsMeta();
            for (Map.Entry<Long, Map<Long, PhysicalPartitionMeta>> logicalEntry : partitionsMeta.entrySet()) {
                long logicalId = logicalEntry.getKey();
                Set<Long> rewritten = rewrittenPhysicalPartitionIdsByLogicalId
                        .getOrDefault(logicalId, Collections.emptySet());
                Map<Long, PhysicalPartitionMeta> physicalMetas = logicalEntry.getValue();
                for (Map.Entry<Long, PhysicalPartitionMeta> physicalEntry : physicalMetas.entrySet()) {
                    long physicalId = physicalEntry.getKey();
                    if (!rewritten.contains(physicalId)) {
                        changesByLogical
                                .computeIfAbsent(logicalId, k -> new ArrayList<>())
                                .add(new PartitionDropped(logicalId, physicalId, physicalEntry.getValue()));
                    }
                }
            }
            return new BookmarkChange(changesByLogical);
        }

        private static PhysicalPartitionMeta livePhysicalMeta(PhysicalPartition physical, MaterializedIndex baseIndex) {
            return new PhysicalPartitionMeta(
                    baseIndex.getId(),
                    baseIndex.getMetaId(),
                    physical.getVisibleVersion(),
                    physical.getVisibleVersionTime());
        }
    }
}
