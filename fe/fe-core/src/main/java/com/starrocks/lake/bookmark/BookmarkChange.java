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

import com.google.common.base.Preconditions;

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
        DATA_CHANGED
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
     * protected from vacuum: {@code ADDED} or {@code DATA_CHANGED}. Other
     * change types may rewrite or remove data the bookmark anchors. Future
     * change types may extend this set as vacuum coverage grows.
     */
    public boolean isTrackable() {
        for (List<PhysicalPartitionChange> row : changesByLogicalPartition.values()) {
            for (PhysicalPartitionChange c : row) {
                ChangeType t = c.getChangeType();
                if (t != ChangeType.ADDED && t != ChangeType.DATA_CHANGED) {
                    return false;
                }
            }
        }
        return true;
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
                    Collections.emptyMap(), head.getPartitionsMeta());
        }
        Bookmark b = base.get();
        Preconditions.checkArgument(b.getTableId() == head.getTableId(),
                "base.tableId(%s) != head.tableId(%s)", b.getTableId(), head.getTableId());
        Preconditions.checkArgument(b.getBookmarkId() <= head.getBookmarkId(),
                "base.bookmarkId(%s) > head.bookmarkId(%s)", b.getBookmarkId(), head.getBookmarkId());
        return computeChanges(OptionalLong.of(b.getBookmarkId()), head.getBookmarkId(),
                b.getPartitionsMeta(), head.getPartitionsMeta());
    }

    private static BookmarkChange computeChanges(
            OptionalLong baseBookmarkId, long headBookmarkId,
            Map<Long, Map<Long, PhysicalPartitionMeta>> baseParts,
            Map<Long, Map<Long, PhysicalPartitionMeta>> headParts) {
        Map<Long, List<PhysicalPartitionChange>> changes = new HashMap<>();

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

        return new BookmarkChange(baseBookmarkId, headBookmarkId, changes);
    }
}
