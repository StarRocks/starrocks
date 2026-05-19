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
import com.starrocks.lake.bookmark.BookmarkChange.DataChanged;
import com.starrocks.lake.bookmark.BookmarkChange.IndexReplaced;
import com.starrocks.lake.bookmark.BookmarkChange.PartitionAdded;
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
import java.util.OptionalLong;
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
        return resolveByBookmark(live, bookmark);
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
        return resolveByBookmark(live, bookmark);
    }

    /**
     * Returns a scoped OlapTable that exposes only the physical partitions touched
     * by {@code delta}. Each surviving physical partition is stamped with the
     * head bookmark's visibleVersion / visibleVersionTimeMs from the change
     * record, so a downstream scan sees the head's data. Full schema
     * (columns, indexes) is preserved.
     *
     * <p>Throws {@link SemanticException} when {@code delta} is not applicable:
     * <ul>
     *   <li>base→head not trackable: the delta itself records a
     *       {@link PartitionDropped} / {@link IndexReplaced} / {@link TabletReshard};</li>
     *   <li>head→live not trackable: a head-recorded physical partition has
     *       been dropped, schema-replaced, or resharded in live since the head
     *       bookmark was taken.</li>
     * </ul>
     *
     * @throws SemanticException on either kind of non-trackability above
     */
    public static OlapTable resolveByChange(OlapTable live, BookmarkChange delta) {
        String tableName = live.getName();
        checkChangesTrackable(delta, tableName);

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
        ChangesScopedRewriter rewriter = new ChangesScopedRewriter(delta);
        live.buildBookmarkScopedTable(shadow, rewriter);

        long headBookmarkId = delta.getHeadBookmarkId();
        BookmarkChange nonTrackable = rewriter.collectNonTrackableChanges();
        for (Map.Entry<Long, List<PhysicalPartitionChange>> entry : nonTrackable.getChanges().entrySet()) {
            for (PhysicalPartitionChange change : entry.getValue()) {
                long logicalId = change.getLogicalPartitionId();
                long physicalId = change.getPhysicalPartitionId();
                if (change instanceof PartitionDropped) {
                    throw new SemanticException(String.format(
                            "CHANGES not trackable: physical partition %d on table '%s' dropped from live since bookmark %d",
                            physicalId, tableName, headBookmarkId));
                }
                String partitionName = live.getPartition(logicalId).getName();
                if (change instanceof IndexReplaced) {
                    throw new SemanticException(String.format(
                            "CHANGES not trackable: partition '%s' on table '%s' rewritten in live since bookmark %d",
                            partitionName, tableName, headBookmarkId));
                }
                if (change instanceof TabletReshard) {
                    throw new SemanticException(String.format(
                            "CHANGES not trackable: partition '%s' on table '%s' resharded in live since bookmark %d",
                            partitionName, tableName, headBookmarkId));
                }
            }
        }
        return shadow;
    }

    /** Reject the first non-trackable entry in {@code delta} (base→head not applicable). */
    private static void checkChangesTrackable(BookmarkChange delta, String tableName) {
        long headId = delta.getHeadBookmarkId();
        for (List<PhysicalPartitionChange> changes : delta.getChanges().values()) {
            for (PhysicalPartitionChange change : changes) {
                String reason;
                if (change instanceof PartitionDropped) {
                    reason = "dropped";
                } else if (change instanceof IndexReplaced) {
                    reason = "rewritten";
                } else if (change instanceof TabletReshard) {
                    reason = "resharded";
                } else {
                    continue;
                }
                long baseId = delta.getBaseBookmarkId().orElseThrow(() ->
                        new IllegalStateException("non-trackable delta must carry a base bookmark id"));
                throw new SemanticException(String.format(
                        "CHANGES from bookmark %d to %d on table '%s' not trackable: physical partition %d %s",
                        baseId, headId, tableName, change.getPhysicalPartitionId(), reason));
            }
        }
    }

    private static OlapTable resolveByBookmark(OlapTable live, Bookmark bookmark) {
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

        BookmarkScopedRewriter rewriter = new BookmarkScopedRewriter(bookmark);
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
     * Shared shape for both rewriter paths: walk live's partitions, look up
     * each physical against the bookmark-side partition meta, stamp the
     * matched ones with the bookmark-recorded version, and record metaId /
     * indexId drift plus bookmark-only physicals as non-trackable changes.
     * Subclasses supply the partition meta (a delta's PartitionAdded /
     * DataChanged entries, or a bookmark's full partition meta) and the
     * bookmark id this rewriter is anchored to.
     */
    private abstract static class AbstractPartitionRewriter implements BookmarkPartitionRewriter {
        private final long bookmarkId;
        private final Map<Long, Set<Long>> rewrittenPhysicalPartitionIdsByLogicalId = new HashMap<>();
        private final List<PhysicalPartitionChange> nonTrackableChangesSeenDuringRewrite = new ArrayList<>();

        protected AbstractPartitionRewriter(long bookmarkId) {
            this.bookmarkId = bookmarkId;
        }

        /** Bookmark-side partition meta the rewriter matches live against: logicalId → physicalId → meta. */
        protected abstract Map<Long, Map<Long, PhysicalPartitionMeta>> partitionsMeta();

        @Override
        public final Optional<PhysicalPartition> rewrite(Partition partition, PhysicalPartition physical) {
            long logicalId = partition.getId();
            long physicalId = physical.getId();
            Map<Long, PhysicalPartitionMeta> physicals = partitionsMeta().get(logicalId);
            if (physicals == null) {
                return Optional.empty();
            }
            PhysicalPartitionMeta bookmarkPartitionMeta = physicals.get(physicalId);
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
         * per bookmark-recorded physical id that was never rewritten (live no
         * longer has it).
         */
        BookmarkChange collectNonTrackableChanges() {
            Map<Long, List<PhysicalPartitionChange>> changesByLogical = new HashMap<>();
            for (PhysicalPartitionChange change : nonTrackableChangesSeenDuringRewrite) {
                changesByLogical
                        .computeIfAbsent(change.getLogicalPartitionId(), k -> new ArrayList<>())
                        .add(change);
            }
            for (Map.Entry<Long, Map<Long, PhysicalPartitionMeta>> logicalEntry : partitionsMeta().entrySet()) {
                long logicalId = logicalEntry.getKey();
                Set<Long> rewritten = rewrittenPhysicalPartitionIdsByLogicalId
                        .getOrDefault(logicalId, Collections.emptySet());
                for (Map.Entry<Long, PhysicalPartitionMeta> physicalEntry : logicalEntry.getValue().entrySet()) {
                    long physicalId = physicalEntry.getKey();
                    if (!rewritten.contains(physicalId)) {
                        changesByLogical
                                .computeIfAbsent(logicalId, k -> new ArrayList<>())
                                .add(new PartitionDropped(logicalId, physicalId, physicalEntry.getValue()));
                    }
                }
            }
            return new BookmarkChange(OptionalLong.empty(), bookmarkId, changesByLogical);
        }

        private static PhysicalPartitionMeta livePhysicalMeta(PhysicalPartition physical, MaterializedIndex baseIndex) {
            return new PhysicalPartitionMeta(
                    baseIndex.getId(), baseIndex.getMetaId(),
                    physical.getVisibleVersion(), physical.getVisibleVersionTime());
        }
    }

    /**
     * {@link AbstractPartitionRewriter} sourced from a {@link BookmarkChange} delta:
     * the partition meta is taken from each {@link PartitionAdded} /
     * {@link DataChanged} entry's head-bookmark record. {@link IndexReplaced},
     * {@link TabletReshard}, {@link PartitionDropped} entries are skipped (they
     * carry no head record and are rejected upstream as non-trackable).
     */
    private static final class ChangesScopedRewriter extends AbstractPartitionRewriter {
        private final Map<Long, Map<Long, PhysicalPartitionMeta>> headMetaByLogicalAndPhysical;

        ChangesScopedRewriter(BookmarkChange delta) {
            super(delta.getHeadBookmarkId());
            this.headMetaByLogicalAndPhysical = new HashMap<>();
            for (Map.Entry<Long, List<PhysicalPartitionChange>> logicalEntry : delta.getChanges().entrySet()) {
                long logicalId = logicalEntry.getKey();
                Map<Long, PhysicalPartitionMeta> inner = new HashMap<>();
                for (PhysicalPartitionChange change : logicalEntry.getValue()) {
                    PhysicalPartitionMeta head = headMetaOf(change);
                    if (head != null) {
                        inner.put(change.getPhysicalPartitionId(), head);
                    }
                }
                if (!inner.isEmpty()) {
                    headMetaByLogicalAndPhysical.put(logicalId, inner);
                }
            }
        }

        @Override
        protected Map<Long, Map<Long, PhysicalPartitionMeta>> partitionsMeta() {
            return headMetaByLogicalAndPhysical;
        }

        private static PhysicalPartitionMeta headMetaOf(PhysicalPartitionChange change) {
            if (change instanceof PartitionAdded) {
                return ((PartitionAdded) change).getHeadPartition();
            }
            if (change instanceof DataChanged) {
                return ((DataChanged) change).getHeadPartition();
            }
            return null;
        }
    }

    /**
     * {@link AbstractPartitionRewriter} sourced from a single {@link Bookmark}:
     * the partition meta is the bookmark's own. Used by the single-bookmark
     * resolution paths ({@link #resolveById}, {@link #resolveByTimestamp}).
     */
    private static final class BookmarkScopedRewriter extends AbstractPartitionRewriter {
        private final Bookmark bookmark;

        BookmarkScopedRewriter(Bookmark bookmark) {
            super(bookmark.getBookmarkId());
            this.bookmark = bookmark;
        }

        @Override
        protected Map<Long, Map<Long, PhysicalPartitionMeta>> partitionsMeta() {
            return bookmark.getPartitionsMeta();
        }
    }
}
