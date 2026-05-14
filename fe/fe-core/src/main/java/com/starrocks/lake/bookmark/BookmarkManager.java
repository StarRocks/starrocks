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
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.epack.persist.SRMetaBlockIDEPack;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Cluster-wide owner of bookmarks. Routes create / acquire / release / lookup
 * calls to the per-table tracker, persists state through the edit log and the
 * meta-image, and reclaims trackers that hold nothing.
 */
public class BookmarkManager extends FrontendDaemon {

    private static final Logger LOG = LogManager.getLogger(BookmarkManager.class);

    private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, TableBookmarkTracker>> trackers =
            new ConcurrentHashMap<>();

    private final ReentrantReadWriteLock trackerMapLock = new ReentrantReadWriteLock();

    public BookmarkManager() {
        super("BookmarkManager", Config.bookmark_cleanup_interval_sec * 1000L);
    }

    @Override
    protected void runAfterCatalogReady() {
        // TODO: implement TTL-based sweep of stale references.
        setInterval(Config.bookmark_cleanup_interval_sec * 1000L);
    }

    /* ---------- bookmark lifecycle ---------- */

    public Bookmark create(long dbId, long tableId, BookmarkHolder holder)
            throws AlreadyAtLatestException, LockTimeoutException {
        Preconditions.checkNotNull(holder);
        trackerMapLock.readLock().lock();
        boolean success = false;
        try {
            TableBookmarkTracker tracker = getTrackerLocked(dbId, tableId, true).get();
            Bookmark bookmark = tracker.create(holder);
            success = true;
            return bookmark;
        } finally {
            trackerMapLock.readLock().unlock();
            if (!success) {
                removeEmptyTracker(dbId, tableId);
            }
        }
    }

    public Bookmark acquireReference(long dbId, long tableId, long bookmarkId, BookmarkHolder holder)
            throws AlreadyReferencedException, BookmarkNotFoundException {
        Preconditions.checkNotNull(holder);
        trackerMapLock.readLock().lock();
        try {
            TableBookmarkTracker tr = getTrackerLocked(dbId, tableId, false)
                    .orElseThrow(() -> {
                        LOG.debug("bookmark not found on acquire (no tracker): db={}, table={}, bookmarkId={}, holder={}",
                                dbId, tableId, bookmarkId, holder.getHolderId());
                        return new BookmarkNotFoundException(dbId, tableId, bookmarkId, holder.getHolderId());
                    });
            return tr.acquireReference(bookmarkId, holder);
        } finally {
            trackerMapLock.readLock().unlock();
        }
    }

    public void releaseReference(long dbId, long tableId, long bookmarkId, HolderId holderId)
            throws BookmarkNotFoundException, ReferenceNotFoundException {
        Preconditions.checkNotNull(holderId);
        boolean nowEmpty;
        trackerMapLock.readLock().lock();
        try {
            TableBookmarkTracker tr = getTrackerLocked(dbId, tableId, false)
                    .orElseThrow(() -> {
                        LOG.debug("bookmark not found on release (no tracker): db={}, table={}, bookmarkId={}, holder={}",
                                dbId, tableId, bookmarkId, holderId);
                        return new BookmarkNotFoundException(dbId, tableId, bookmarkId, holderId);
                    });
            tr.releaseReference(bookmarkId, holderId);
            nowEmpty = tr.isEmpty();
        } finally {
            trackerMapLock.readLock().unlock();
        }
        if (nowEmpty) {
            removeEmptyTracker(dbId, tableId);
        }
    }

    /* ---------- queries ---------- */

    public Optional<Bookmark> findBookmarkById(long dbId, long tableId, long bookmarkId) {
        trackerMapLock.readLock().lock();
        try {
            return getTrackerLocked(dbId, tableId, false).flatMap(tr -> tr.findByBookmarkId(bookmarkId));
        } finally {
            trackerMapLock.readLock().unlock();
        }
    }

    public Optional<Bookmark> findByTimestamp(long dbId, long tableId, long ts) {
        trackerMapLock.readLock().lock();
        try {
            return getTrackerLocked(dbId, tableId, false).flatMap(tr -> tr.findByTimestamp(ts));
        } finally {
            trackerMapLock.readLock().unlock();
        }
    }

    public Optional<Long> getPhysicalPartitionFenceVersion(long dbId, long tableId,
                                                           long logicalPartitionId, long physicalPartitionId) {
        trackerMapLock.readLock().lock();
        try {
            return getTrackerLocked(dbId, tableId, false)
                    .flatMap(tr -> tr.getPhysicalPartitionFenceVersion(logicalPartitionId, physicalPartitionId));
        } finally {
            trackerMapLock.readLock().unlock();
        }
    }

    @VisibleForTesting
    public int activeBookmarkCount(long dbId, long tableId) {
        trackerMapLock.readLock().lock();
        try {
            return getTrackerLocked(dbId, tableId, false)
                    .map(TableBookmarkTracker::activeBookmarkCount)
                    .orElse(0);
        } finally {
            trackerMapLock.readLock().unlock();
        }
    }

    @VisibleForTesting
    public int referenceCount(long dbId, long tableId, long bookmarkId) {
        trackerMapLock.readLock().lock();
        try {
            return getTrackerLocked(dbId, tableId, false)
                    .map(tr -> tr.referenceCount(bookmarkId))
                    .orElse(0);
        } finally {
            trackerMapLock.readLock().unlock();
        }
    }

    /* ---------- journal / image ---------- */

    public void replay(BookmarkLogEntry entry) {
        if (entry == null) {
            LOG.warn("ignoring null bookmark log entry");
            return;
        }
        long dbId = entry.getDbId();
        long tableId = entry.getTableId();
        if (entry instanceof BookmarkLogEntry.AddBookmark
                || entry instanceof BookmarkLogEntry.AcquireReference) {
            trackerMapLock.readLock().lock();
            try {
                getTrackerLocked(dbId, tableId, true).get().replayLogEntry(entry);
            } finally {
                trackerMapLock.readLock().unlock();
            }
        } else if (entry instanceof BookmarkLogEntry.ReleaseReference) {
            boolean nowEmpty;
            trackerMapLock.readLock().lock();
            try {
                Optional<TableBookmarkTracker> trOpt = getTrackerLocked(dbId, tableId, false);
                if (trOpt.isEmpty()) {
                    return;
                }
                TableBookmarkTracker tr = trOpt.get();
                tr.replayLogEntry(entry);
                nowEmpty = tr.isEmpty();
            } finally {
                trackerMapLock.readLock().unlock();
            }
            if (nowEmpty) {
                removeEmptyTracker(dbId, tableId);
            }
        } else {
            LOG.warn("unknown bookmark log entry type: {}", entry.getClass().getName());
        }
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        trackerMapLock.readLock().lock();
        try {
            int totalTrackerCount = trackers.values().stream().mapToInt(Map::size).sum();
            SRMetaBlockWriter writer = imageWriter.getBlockWriter(
                    SRMetaBlockIDEPack.BOOKMARK_MANAGER, 1 + totalTrackerCount);
            writer.writeJson(new BookmarkManagerImageHeader(totalTrackerCount));
            for (Map<Long, TableBookmarkTracker> dbMap : trackers.values()) {
                for (TableBookmarkTracker tr : dbMap.values()) {
                    tr.writeJsonTo(writer);
                }
            }
            writer.close();
        } finally {
            trackerMapLock.readLock().unlock();
        }
    }

    public void load(SRMetaBlockReader reader)
            throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        BookmarkManagerImageHeader header = reader.readJson(BookmarkManagerImageHeader.class);
        for (int i = 0; i < header.totalTrackerCount; i++) {
            TableBookmarkTracker tr = reader.readJson(TableBookmarkTracker.class);
            trackers.computeIfAbsent(tr.getDbId(), k -> new ConcurrentHashMap<>())
                    .put(tr.getTableId(), tr);
        }
    }

    /** Header block; @SerializedName is required for Gson's HiddenAnnotationExclusionStrategy. */
    static final class BookmarkManagerImageHeader {
        @SerializedName("n")
        int totalTrackerCount;

        BookmarkManagerImageHeader() {
        }

        BookmarkManagerImageHeader(int n) {
            this.totalTrackerCount = n;
        }
    }

    /* ---------- internals ---------- */

    /**
     * Returns the tracker for {@code (dbId, tableId)} under {@code trackerMapLock}.
     * When {@code createIfMissing} is true the slot is materialised and the result
     * is always present.
     */
    private Optional<TableBookmarkTracker> getTrackerLocked(long dbId, long tableId, boolean createIfMissing) {
        if (createIfMissing) {
            TableBookmarkTracker tr = trackers
                    .computeIfAbsent(dbId, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(tableId, k -> createTracker(dbId, tableId));
            return Optional.of(tr);
        }
        ConcurrentHashMap<Long, TableBookmarkTracker> dbMap = trackers.get(dbId);
        return dbMap == null ? Optional.empty() : Optional.ofNullable(dbMap.get(tableId));
    }

    /** Overridable so tests can inject a tracker subclass with extra hooks. */
    protected TableBookmarkTracker createTracker(long dbId, long tableId) {
        return new TableBookmarkTracker(dbId, tableId);
    }

    // Holding the write lock here keeps a concurrent create from re-growing the
    // tracker between the empty check and the remove.
    private void removeEmptyTracker(long dbId, long tableId) {
        trackerMapLock.writeLock().lock();
        try {
            ConcurrentHashMap<Long, TableBookmarkTracker> dbMap = trackers.get(dbId);
            if (dbMap == null) {
                return;
            }
            TableBookmarkTracker tracker = dbMap.get(tableId);
            if (tracker != null && tracker.isEmpty()) {
                dbMap.remove(tableId);
            }
            if (dbMap.isEmpty()) {
                trackers.remove(dbId, dbMap);
            }
        } finally {
            trackerMapLock.writeLock().unlock();
        }
    }

}
