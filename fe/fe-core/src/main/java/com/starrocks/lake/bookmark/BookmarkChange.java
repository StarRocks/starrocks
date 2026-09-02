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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/** How a table's physical-partition layout moved between two bookmarks. */
public final class BookmarkChange {

    public enum ChangeType {
        /** Present at head but not at base. */
        ADDED,
        /** Present at base but not at head. */
        DROPPED,
        /** Base materialized index meta id changed (data-rewrite schema change). */
        INDEX_REPLACED,
        /** Base index id changed while meta id stayed the same (tablet split/merge). */
        TABLET_RESHARD,
        /** Same base index identity, visible version advanced (data write). */
        DATA_CHANGED,
        /** Base index id changed by a resolvable tablet split/merge chain; carries per-generation
         * version sub-ranges and is trackable. */
        RESHARDED_DATA_CHANGED
    }

    public abstract static class PhysicalPartitionChange {
        protected final long logicalPartitionId;
        protected final long physicalPartitionId;

        protected PhysicalPartitionChange(long logicalPartitionId, long physicalPartitionId) {
            this.logicalPartitionId = logicalPartitionId;
            this.physicalPartitionId = physicalPartitionId;
        }

        public long getLogicalPartitionId() {
            return logicalPartitionId;
        }

        public long getPhysicalPartitionId() {
            return physicalPartitionId;
        }

        public abstract ChangeType getChangeType();
    }

    /** Physical partition present at head but not at base. */
    public static final class PartitionAdded extends PhysicalPartitionChange {
        private final PhysicalPartitionMeta headPartition;

        public PartitionAdded(long logicalPartitionId, long physicalPartitionId,
                              PhysicalPartitionMeta headPartition) {
            super(logicalPartitionId, physicalPartitionId);
            this.headPartition = headPartition;
        }

        public PhysicalPartitionMeta getHeadPartition() {
            return headPartition;
        }

        @Override
        public ChangeType getChangeType() {
            return ChangeType.ADDED;
        }
    }

    /** Physical partition present at base but not at head. */
    public static final class PartitionDropped extends PhysicalPartitionChange {
        private final PhysicalPartitionMeta basePartition;

        public PartitionDropped(long logicalPartitionId, long physicalPartitionId,
                                PhysicalPartitionMeta basePartition) {
            super(logicalPartitionId, physicalPartitionId);
            this.basePartition = basePartition;
        }

        public PhysicalPartitionMeta getBasePartition() {
            return basePartition;
        }

        @Override
        public ChangeType getChangeType() {
            return ChangeType.DROPPED;
        }
    }

    /** Base materialized index meta id changed (data-rewrite schema change). */
    public static final class IndexReplaced extends PhysicalPartitionChange {
        private final PhysicalPartitionMeta basePartition;
        private final PhysicalPartitionMeta headPartition;

        public IndexReplaced(long logicalPartitionId, long physicalPartitionId,
                             PhysicalPartitionMeta basePartition,
                             PhysicalPartitionMeta headPartition) {
            super(logicalPartitionId, physicalPartitionId);
            this.basePartition = basePartition;
            this.headPartition = headPartition;
        }

        public PhysicalPartitionMeta getBasePartition() {
            return basePartition;
        }

        public PhysicalPartitionMeta getHeadPartition() {
            return headPartition;
        }

        @Override
        public ChangeType getChangeType() {
            return ChangeType.INDEX_REPLACED;
        }
    }

    /** Base index id changed while meta id stayed the same (tablet split/merge). */
    public static final class TabletReshard extends PhysicalPartitionChange {
        private final PhysicalPartitionMeta basePartition;
        private final PhysicalPartitionMeta headPartition;

        public TabletReshard(long logicalPartitionId, long physicalPartitionId,
                             PhysicalPartitionMeta basePartition,
                             PhysicalPartitionMeta headPartition) {
            super(logicalPartitionId, physicalPartitionId);
            this.basePartition = basePartition;
            this.headPartition = headPartition;
        }

        public PhysicalPartitionMeta getBasePartition() {
            return basePartition;
        }

        public PhysicalPartitionMeta getHeadPartition() {
            return headPartition;
        }

        @Override
        public ChangeType getChangeType() {
            return ChangeType.TABLET_RESHARD;
        }
    }

    /** A {@link TabletReshard} resolved against the live generation chain: base index id changed
     * by a resolvable tablet split/merge, carrying the per-generation version sub-ranges. */
    public static final class ReshardedDataChanged extends PhysicalPartitionChange {
        private final PhysicalPartitionMeta basePartition;
        private final PhysicalPartitionMeta headPartition;
        private final List<IndexEpoch> epochs;

        public ReshardedDataChanged(long logicalPartitionId, long physicalPartitionId,
                                    PhysicalPartitionMeta basePartition,
                                    PhysicalPartitionMeta headPartition,
                                    List<IndexEpoch> epochs) {
            super(logicalPartitionId, physicalPartitionId);
            this.basePartition = basePartition;
            this.headPartition = headPartition;
            this.epochs = List.copyOf(epochs);
        }

        public PhysicalPartitionMeta getBasePartition() {
            return basePartition;
        }

        public PhysicalPartitionMeta getHeadPartition() {
            return headPartition;
        }

        public List<IndexEpoch> getEpochs() {
            return epochs;
        }

        @Override
        public ChangeType getChangeType() {
            return ChangeType.RESHARDED_DATA_CHANGED;
        }
    }

    /** Same base index identity, visible version advanced (data write). */
    public static final class DataChanged extends PhysicalPartitionChange {
        private final PhysicalPartitionMeta basePartition;
        private final PhysicalPartitionMeta headPartition;

        public DataChanged(long logicalPartitionId, long physicalPartitionId,
                           PhysicalPartitionMeta basePartition,
                           PhysicalPartitionMeta headPartition) {
            super(logicalPartitionId, physicalPartitionId);
            this.basePartition = basePartition;
            this.headPartition = headPartition;
        }

        public PhysicalPartitionMeta getBasePartition() {
            return basePartition;
        }

        public PhysicalPartitionMeta getHeadPartition() {
            return headPartition;
        }

        @Override
        public ChangeType getChangeType() {
            return ChangeType.DATA_CHANGED;
        }
    }

    private final OptionalLong baseBookmarkId;
    private final long headBookmarkId;
    private final Map<Long, List<PhysicalPartitionChange>> changesByLogicalPartition;

    public BookmarkChange(OptionalLong baseBookmarkId, long headBookmarkId,
                          Map<Long, List<PhysicalPartitionChange>> changes) {
        this.baseBookmarkId = baseBookmarkId;
        this.headBookmarkId = headBookmarkId;
        this.changesByLogicalPartition = changes;
    }

    public OptionalLong getBaseBookmarkId() {
        return baseBookmarkId;
    }

    public long getHeadBookmarkId() {
        return headBookmarkId;
    }

    public Map<Long, List<PhysicalPartitionChange>> getChanges() {
        return Collections.unmodifiableMap(changesByLogicalPartition);
    }

    public boolean isNoChange() {
        return changesByLogicalPartition.isEmpty();
    }

    /**
     * True iff every per-partition change is one whose underlying data is
     * protected from vacuum: {@code ADDED}, {@code DATA_CHANGED}, or
     * {@code RESHARDED_DATA_CHANGED}. Other change types may rewrite or remove
     * data the bookmark anchors. Future change types may extend this set as
     * vacuum coverage grows.
     */
    public boolean isTrackable() {
        for (List<PhysicalPartitionChange> row : changesByLogicalPartition.values()) {
            for (PhysicalPartitionChange c : row) {
                ChangeType t = c.getChangeType();
                if (t != ChangeType.ADDED && t != ChangeType.DATA_CHANGED && t != ChangeType.RESHARDED_DATA_CHANGED) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * True iff any per-partition change is a resolved {@code RESHARDED_DATA_CHANGED} that actually
     * spans generations, i.e. carries at least one epoch.
     *
     * <p>The empty-epoch case is a resolved reshard that reads nothing: base = S-1 and head = S for
     * a split with no load in between collapses both sub-ranges. Callers use this to decide whether
     * to drop to ANY distribution, veto colocate across the fragment, and reject a TABLET hint --
     * all of which exist to protect a scan that touches old-generation tablets. A scan with no
     * ranges touches none, so treating it as crossing would reject a hint for a range that crosses
     * nothing.
     */
    public boolean hasReshardedChanges() {
        return changesByLogicalPartition.values().stream().flatMap(List::stream)
                .anyMatch(c -> c.getChangeType() == ChangeType.RESHARDED_DATA_CHANGED
                        && !((ReshardedDataChanged) c).getEpochs().isEmpty());
    }

    /** The per-partition change for {@code (logicalPartitionId, physicalPartitionId)}, if any. */
    public Optional<PhysicalPartitionChange> getChange(long logicalPartitionId, long physicalPartitionId) {
        List<PhysicalPartitionChange> row = changesByLogicalPartition.get(logicalPartitionId);
        if (row == null) {
            return Optional.empty();
        }
        for (PhysicalPartitionChange c : row) {
            if (c.getPhysicalPartitionId() == physicalPartitionId) {
                return Optional.of(c);
            }
        }
        return Optional.empty();
    }

    /**
     * Resolve TABLET_RESHARD entries against the live table's generation chains. Each entry whose
     * chain from base index to head index is fully resolvable (every generation still installed,
     * every successor stamped with a reshard takeover version) becomes a trackable
     * RESHARDED_DATA_CHANGED carrying the per-generation sub-ranges of (base, head]. Unresolvable
     * entries are kept, so the change set degrades exactly as before this feature. Returns
     * {@code this} when there is nothing to resolve. The caller must hold a table read lock or the
     * planner meta lock (generation maps are read live).
     */
    @VisibleForTesting
    BookmarkChange resolveReshards(OlapTable table) {
        boolean anyReshard = changesByLogicalPartition.values().stream()
                .flatMap(List::stream).anyMatch(c -> c.getChangeType() == ChangeType.TABLET_RESHARD);
        if (!anyReshard) {
            return this;
        }
        Map<Long, List<PhysicalPartitionChange>> resolved = new HashMap<>();
        for (Map.Entry<Long, List<PhysicalPartitionChange>> e : changesByLogicalPartition.entrySet()) {
            List<PhysicalPartitionChange> row = new ArrayList<>(e.getValue().size());
            for (PhysicalPartitionChange c : e.getValue()) {
                row.add(c instanceof TabletReshard reshard ? resolveOne(table, reshard) : c);
            }
            resolved.put(e.getKey(), row);
        }
        return new BookmarkChange(baseBookmarkId, headBookmarkId, resolved);
    }

    private static PhysicalPartitionChange resolveOne(OlapTable table, TabletReshard reshard) {
        PhysicalPartition partition = table.getPhysicalPartition(reshard.getPhysicalPartitionId());
        if (partition == null) {
            return reshard;
        }
        PhysicalPartitionMeta base = reshard.getBasePartition();
        PhysicalPartitionMeta head = reshard.getHeadPartition();
        return ReshardEpochResolver.resolveEpochs(partition, head.getBaseMaterializedIndexMetaId(),
                        base.getBaseMaterializedIndexId(), base.getVisibleVersion(),
                        head.getBaseMaterializedIndexId(), head.getVisibleVersion())
                .<PhysicalPartitionChange>map(epochs -> new ReshardedDataChanged(
                        reshard.getLogicalPartitionId(), reshard.getPhysicalPartitionId(), base, head, epochs))
                .orElse(reshard);
    }

    /**
     * Compute the change between two bookmarks of the same table.
     *
     * @param base the earlier bookmark, or {@link Optional#empty()} for "no prior
     *             bookmark" (every physical partition in {@code head} is ADDED).
     * @param head the later bookmark; must be non-null.
     * @throws IllegalArgumentException if base and head belong to different tables
     *         or base is newer than head.
     */
    public static BookmarkChange computeChanges(Optional<Bookmark> base, Bookmark head) {
        Preconditions.checkNotNull(base, "base must not be null (use Optional.empty())");
        Preconditions.checkNotNull(head, "head must not be null");
        if (base.isEmpty()) {
            return computeChanges(OptionalLong.empty(), head.getBookmarkId(),
                    Collections.emptyMap(), head.getPartitionsMeta(), head.getDbId(), head.getTableId());
        }
        Bookmark b = base.get();
        Preconditions.checkArgument(b.getTableId() == head.getTableId(),
                "base.tableId(%s) != head.tableId(%s)", b.getTableId(), head.getTableId());
        Preconditions.checkArgument(b.getBookmarkId() <= head.getBookmarkId(),
                "base.bookmarkId(%s) > head.bookmarkId(%s)", b.getBookmarkId(), head.getBookmarkId());
        return computeChanges(OptionalLong.of(b.getBookmarkId()), head.getBookmarkId(),
                b.getPartitionsMeta(), head.getPartitionsMeta(), head.getDbId(), head.getTableId());
    }

    private static BookmarkChange computeChanges(
            OptionalLong baseBookmarkId, long headBookmarkId,
            Map<Long, Map<Long, PhysicalPartitionMeta>> baseParts,
            Map<Long, Map<Long, PhysicalPartitionMeta>> headParts,
            long dbId, long tableId) {
        Map<Long, List<PhysicalPartitionChange>> changes = new HashMap<>();
        // Whether classification produced any TABLET_RESHARD. This branch is its only producer, so
        // the flag tells us whether the (table-touching, lock-taking) resolution pass is needed at
        // all -- the common case never looks the table up.
        boolean anyReshard = false;

        // Pass 1: iterate head; classify each physical partition relative to base.
        for (Map.Entry<Long, Map<Long, PhysicalPartitionMeta>> e : headParts.entrySet()) {
            long logicalId = e.getKey();
            Map<Long, PhysicalPartitionMeta> headPPs = e.getValue();
            Map<Long, PhysicalPartitionMeta> basePPs = baseParts.get(logicalId);
            List<PhysicalPartitionChange> row = new ArrayList<>();

            if (basePPs == null) {
                for (Map.Entry<Long, PhysicalPartitionMeta> ppe : headPPs.entrySet()) {
                    row.add(new PartitionAdded(logicalId, ppe.getKey(), ppe.getValue()));
                }
            } else {
                for (Map.Entry<Long, PhysicalPartitionMeta> ppe : headPPs.entrySet()) {
                    long ppId = ppe.getKey();
                    PhysicalPartitionMeta hp = ppe.getValue();
                    PhysicalPartitionMeta bp = basePPs.get(ppId);
                    if (bp == null) {
                        // Same logical partition, brand-new physical partition.
                        row.add(new PartitionAdded(logicalId, ppId, hp));
                        continue;
                    }
                    boolean metaIdChanged =
                            bp.getBaseMaterializedIndexMetaId() != hp.getBaseMaterializedIndexMetaId();
                    boolean indexIdChanged = !metaIdChanged
                            && bp.getBaseMaterializedIndexId() != hp.getBaseMaterializedIndexId();
                    boolean versionChanged = !metaIdChanged && !indexIdChanged
                            && bp.getVisibleVersion() != hp.getVisibleVersion();

                    if (metaIdChanged) {
                        row.add(new IndexReplaced(logicalId, ppId, bp, hp));
                    } else if (indexIdChanged) {
                        row.add(new TabletReshard(logicalId, ppId, bp, hp));
                        anyReshard = true;
                    } else if (versionChanged) {
                        row.add(new DataChanged(logicalId, ppId, bp, hp));
                    }
                }
                // Physicals present in base but not head (within the same logical partition) → DROP.
                for (Map.Entry<Long, PhysicalPartitionMeta> bpe : basePPs.entrySet()) {
                    if (!headPPs.containsKey(bpe.getKey())) {
                        row.add(new PartitionDropped(logicalId, bpe.getKey(), bpe.getValue()));
                    }
                }
            }

            if (!row.isEmpty()) {
                changes.put(logicalId, row);
            }
        }

        // Pass 2: logical partitions present in base but absent in head → every physical partition is DROP.
        for (Map.Entry<Long, Map<Long, PhysicalPartitionMeta>> e : baseParts.entrySet()) {
            long logicalId = e.getKey();
            if (headParts.containsKey(logicalId)) {
                continue;
            }
            List<PhysicalPartitionChange> row = new ArrayList<>();
            for (Map.Entry<Long, PhysicalPartitionMeta> ppe : e.getValue().entrySet()) {
                row.add(new PartitionDropped(logicalId, ppe.getKey(), ppe.getValue()));
            }
            changes.put(logicalId, row);
        }

        BookmarkChange change = new BookmarkChange(baseBookmarkId, headBookmarkId, changes);
        return anyReshard ? change.resolveReshards(dbId, tableId) : change;
    }

    /**
     * Resolve the TABLET_RESHARD entries produced above, under the table read lock. Only reached
     * when classification actually produced one, so an unresharded table pays neither the lookup
     * nor the lock. Callers may or may not already hold the table read lock -- {@code
     * StatementPlanner} releases the planner meta lock before planning in its lock-free mode but
     * keeps it in the other -- so this takes the lock only when the calling thread does not already
     * have it, rather than relying on a convention callers would have to know about.
     */
    private BookmarkChange resolveReshards(long dbId, long tableId) {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
        if (!(table instanceof OlapTable olapTable)) {
            return this;
        }
        // Deliberately not gated on the distribution type. What makes a generation chain walkable is
        // the reshard-stamped lineage, which ReshardEpochResolver verifies and which nothing else
        // sets; keying off isRangeDistribution() would be a second, weaker proxy for the same thing
        // and would silently stop resolving for any table whose distribution changes meaning later.
        // The anyReshard flag above is what keeps the lookup and the lock off the common path.
        // Read directly when this thread already holds the table read lock. StatementPlanner keeps
        // it across planning whenever needWholePhaseLock is set -- a session with cbo_use_lock_db,
        // or a query joining a non-OLAP table -- and InsertPlanner does the same, which is the path
        // an IVM refresh takes. Acquiring a second time there would deadlock the thread against
        // itself: the second request uses a different Locker, so it misses MultiUserLock's
        // same-locker refCount path and queues behind any waiting writer, while that writer waits
        // for the first acquisition this same thread still holds. The recycle bin's erase is
        // exactly such a writer, and this feature is what schedules it on these tables.
        if (GlobalStateMgr.getCurrentState().getLockManager()
                .isOwnedByCurrentThread(tableId, LockType.READ)) {
            return resolveReshards(olapTable);
        }
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(dbId, tableId, LockType.READ);
        try {
            return resolveReshards(olapTable);
        } finally {
            locker.unLockTableWithIntensiveDbLock(dbId, tableId, LockType.READ);
        }
    }
}
