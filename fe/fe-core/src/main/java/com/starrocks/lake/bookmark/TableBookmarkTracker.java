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
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Owns every bookmark for one OlapTable and the references that reach each
 * bookmark. Mutating calls go through the same path on leader and follower:
 * the change is journalled first, then applied in memory.
 */
public class TableBookmarkTracker {
    private static final Logger LOG = LogManager.getLogger(TableBookmarkTracker.class);

    @SerializedName("db")
    private long dbId;
    @SerializedName("t")
    private long tableId;
    @SerializedName("m")
    private final TreeMap<Long, Bookmark> activeBookmarks = new TreeMap<>();
    @SerializedName("r")
    private final TreeMap<Long, ReferenceSet> referencesByBookmark = new TreeMap<>();

    private transient ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    // Serialises create() so bookmarkId order matches state freshness — a larger
    // bookmarkId always captures a state at least as new as a smaller one. Also
    // keeps the in-flight slot below to one writer at a time.
    private final transient ReentrantLock createLock = new ReentrantLock();

    // The bookmark currently being created — set after partition-meta is read
    // and cleared once it is added to activeBookmarks. Volatile so retention
    // checks observe it without taking the lock.
    private transient volatile Bookmark creating;

    private TableBookmarkTracker() {
    }

    public TableBookmarkTracker(long dbId, long tableId) {
        this.dbId = dbId;
        this.tableId = tableId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    /**
     * Create a bookmark of the table's current state and acquire a reference
     * for {@code holder}. If the latest active bookmark has the same partition
     * meta, that bookmark is reused and only the new reference is added;
     * older equivalent bookmarks are not consulted.
     *
     * @throws AlreadyAtLatestException   if {@code holder} already references the
     *                                    bookmark this call would reuse
     * @throws LockTimeoutException       if the db+table read lock cannot be obtained
     * @throws IllegalStateException      if the table is missing from the catalog
     */
    public Bookmark create(BookmarkHolder holder)
            throws AlreadyAtLatestException, LockTimeoutException {
        Objects.requireNonNull(holder, "holder");

        createLock.lock();
        try {
            Locker locker = new Locker();
            if (!locker.tryLockTableWithIntensiveDbLock(dbId, tableId, LockType.READ,
                    Config.bookmark_table_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
                throw new LockTimeoutException(String.format(
                        "timed out acquiring db+table read lock for bookmark create "
                                + "(db=%d, table=%d, timeoutMs=%d)",
                        dbId, tableId, Config.bookmark_table_lock_timeout_ms));
            }
            Bookmark candidate;
            try {
                OlapTable table = resolveTable();
                candidate = Bookmark.fromTable(dbId, table);
                // Publish the in-flight bookmark before releasing the table
                // read-lock so any retention check that observes it sees
                // partition versions consistent with the bookmark just read.
                creating = candidate;
            } finally {
                locker.unLockTableWithIntensiveDbLock(dbId, tableId, LockType.READ);
            }

            onCopyTableStateInBookmarkCreation();

            rwLock.writeLock().lock();
            try {
                Optional<Bookmark> existing = findLatestEquivalent(candidate.getPartitionsMeta());
                if (existing.isPresent()) {
                    Bookmark b = existing.get();
                    ReferenceSet refSet = referencesByBookmark.get(b.getBookmarkId());
                    if (refSet.get(holder.getHolderId()) != null) {
                        LOG.debug("bookmark already at latest: db={}, table={}, bookmarkId={}, holder={}",
                                dbId, tableId, b.getBookmarkId(), holder.getHolderId());
                        throw new AlreadyAtLatestException(dbId, tableId, b.getBookmarkId(), holder.getHolderId());
                    }
                    journalAndApply(BookmarkLogEntry.AcquireReference.of(
                            dbId, tableId, b.getBookmarkId(), holder, System.currentTimeMillis()));
                    LOG.info("bookmark reused: db={}, table={}, bookmarkId={}, holder={}",
                            dbId, tableId, b.getBookmarkId(), holder.getHolderId());
                    return b;
                }
                journalAndApply(BookmarkLogEntry.AddBookmark.of(candidate, holder, candidate.getBookmarkTimeMs()));
                return candidate;
            } finally {
                // Clear the in-flight slot inside the write lock so an observer
                // never sees the same bookmarkId in both the in-flight slot and
                // the active map.
                creating = null;
                rwLock.writeLock().unlock();
            }
        } finally {
            createLock.unlock();
        }
    }

    /**
     * Acquire a reference on an already-tracked bookmark for {@code holder}.
     *
     * @throws BookmarkNotFoundException  if {@code bookmarkId} is not currently tracked
     * @throws AlreadyReferencedException if {@code holder} already references this bookmark
     */
    public Bookmark acquireReference(long bookmarkId, BookmarkHolder holder)
            throws AlreadyReferencedException, BookmarkNotFoundException {
        Objects.requireNonNull(holder, "holder");
        rwLock.writeLock().lock();
        try {
            Bookmark b = activeBookmarks.get(bookmarkId);
            if (b == null) {
                LOG.debug("bookmark not found on acquire: db={}, table={}, bookmarkId={}, holder={}",
                        dbId, tableId, bookmarkId, holder.getHolderId());
                throw new BookmarkNotFoundException(dbId, tableId, bookmarkId, holder.getHolderId());
            }
            ReferenceSet refSet = referencesByBookmark.get(bookmarkId);
            if (refSet.get(holder.getHolderId()) != null) {
                LOG.debug("bookmark already referenced: db={}, table={}, bookmarkId={}, holder={}",
                        dbId, tableId, bookmarkId, holder.getHolderId());
                throw new AlreadyReferencedException(dbId, tableId, bookmarkId, holder.getHolderId());
            }
            journalAndApply(BookmarkLogEntry.AcquireReference.of(
                    dbId, tableId, bookmarkId, holder, System.currentTimeMillis()));
            return b;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Release the holder's reference on the given bookmark. The bookmark is
     * reclaimed once its last holder releases it.
     *
     * @throws BookmarkNotFoundException  if no tracked bookmark has that id
     * @throws ReferenceNotFoundException if the holder does not reference it
     */
    public void releaseReference(long bookmarkId, HolderId holderId)
            throws BookmarkNotFoundException, ReferenceNotFoundException {
        Objects.requireNonNull(holderId, "holderId");
        rwLock.writeLock().lock();
        try {
            Bookmark b = activeBookmarks.get(bookmarkId);
            if (b == null) {
                LOG.debug("bookmark not found on release: db={}, table={}, bookmarkId={}, holder={}",
                        dbId, tableId, bookmarkId, holderId);
                throw new BookmarkNotFoundException(dbId, tableId, bookmarkId, holderId);
            }
            ReferenceSet refSet = referencesByBookmark.get(bookmarkId);
            Reference removed = refSet.get(holderId);
            if (removed == null) {
                LOG.debug("reference not found on release: db={}, table={}, bookmarkId={}, holder={}",
                        dbId, tableId, bookmarkId, holderId);
                throw new ReferenceNotFoundException(dbId, tableId, bookmarkId, holderId);
            }
            journalAndApply(BookmarkLogEntry.ReleaseReference.of(dbId, tableId, bookmarkId, holderId, removed));
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /** Look up a tracked bookmark by id. */
    public Optional<Bookmark> findByBookmarkId(long bookmarkId) {
        rwLock.readLock().lock();
        try {
            return Optional.ofNullable(activeBookmarks.get(bookmarkId));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Latest tracked bookmark whose creation time is at or before {@code ts},
     * or empty if none exists.
     */
    public Optional<Bookmark> findByTimestamp(long ts) {
        rwLock.readLock().lock();
        try {
            for (Bookmark b : activeBookmarks.descendingMap().values()) {
                if (b.getBookmarkTimeMs() <= ts) {
                    return Optional.of(b);
                }
            }
            return Optional.empty();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /** Ids of bookmarks {@code holderId} references in this tracker, ascending. */
    public List<Long> listBookmarkIdsByHolder(HolderId holderId) {
        Objects.requireNonNull(holderId, "holderId");
        rwLock.readLock().lock();
        try {
            List<Long> ids = new ArrayList<>();
            for (Map.Entry<Long, ReferenceSet> e : referencesByBookmark.entrySet()) {
                if (e.getValue().get(holderId) != null) {
                    ids.add(e.getKey());
                }
            }
            return ids;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Oldest version of {@code (logicalPartitionId, physicalPartitionId)} that is
     * still referenced by any tracked bookmark — including any in-flight create —
     * so vacuum keeps it. Empty when no tracked bookmark captured this partition.
     */
    public Optional<Long> getPhysicalPartitionFenceVersion(long logicalPartitionId, long physicalPartitionId) {
        // Read the in-flight slot before acquiring the tracker's read lock:
        // create() publishes the slot before it acquires the tracker's write
        // lock, so a read taken inside the tracker's read lock could miss a
        // bookmark about to be committed.
        Bookmark inFlight = creating;
        rwLock.readLock().lock();
        try {
            // activeBookmarks is keyed by bookmarkId (monotonic via getNextId()) and
            // PhysicalPartition.visibleVersion is monotonic, so the smallest-id bookmark
            // that captured this partition holds the smallest version of it across all
            // active bookmarks. Iterate ascending and stop at the first match.
            for (Bookmark b : activeBookmarks.values()) {
                Optional<Long> v = b.getPhysicalPartitionVersion(logicalPartitionId, physicalPartitionId);
                LOG.debug("fence version candidate: db={}, table={}, logicalPartitionId={}, physicalPartitionId={}, "
                        + "bookmarkId={}, version={}",
                        dbId, tableId, logicalPartitionId, physicalPartitionId, b.getBookmarkId(), v.orElse(null));
                if (v.isPresent()) {
                    return v;
                }
            }
            // No active bookmark captured this partition. The in-flight bookmark holds a
            // larger bookmarkId than any active one (createLock + monotonic getNextId()),
            // and it may have just observed a newly added partition; it is the only
            // remaining candidate.
            if (inFlight == null) {
                return Optional.empty();
            }
            Optional<Long> v = inFlight.getPhysicalPartitionVersion(logicalPartitionId, physicalPartitionId);
            LOG.debug("fence version candidate (creating): db={}, table={}, logicalPartitionId={}, physicalPartitionId={}, "
                    + "bookmarkId={}, version={}",
                    dbId, tableId, logicalPartitionId, physicalPartitionId, inFlight.getBookmarkId(), v.orElse(null));
            return v;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Read-only listing of every active bookmark in this tracker, sorted by
     * bookmarkId ascending.
     */
    public List<Bookmark.View> listAllBookmarks() {
        rwLock.readLock().lock();
        try {
            List<Bookmark.View> out = new ArrayList<>(activeBookmarks.size());
            for (Map.Entry<Long, Bookmark> e : activeBookmarks.entrySet()) {
                out.add(new Bookmark.View(e.getValue(), collectReferenceViews(e.getKey())));
            }
            return out;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Read-only lookup of a single tracked bookmark with its references.
     */
    public Optional<Bookmark.View> findBookmarkView(long bookmarkId) {
        rwLock.readLock().lock();
        try {
            Bookmark b = activeBookmarks.get(bookmarkId);
            if (b == null) {
                return Optional.empty();
            }
            return Optional.of(new Bookmark.View(b, collectReferenceViews(bookmarkId)));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private List<Reference.View> collectReferenceViews(long bookmarkId) {
        ReferenceSet refSet = referencesByBookmark.get(bookmarkId);
        List<Reference.View> refs = new ArrayList<>();
        if (refSet != null) {
            for (Map.Entry<HolderId, Reference> r : refSet.entries().entrySet()) {
                refs.add(new Reference.View(r.getKey().getId(), r.getValue().getAcquiredAtMs()));
            }
        }
        return refs;
    }

    /** Apply a previously persisted entry on this tracker. */
    public void replayLogEntry(BookmarkLogEntry entry) {
        rwLock.writeLock().lock();
        try {
            apply(entry, true);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public boolean isEmpty() {
        rwLock.readLock().lock();
        try {
            return activeBookmarks.isEmpty();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /* ---------- read accessors for image / tests ---------- */

    @VisibleForTesting
    NavigableMap<Long, Bookmark> activeBookmarks() {
        return activeBookmarks;
    }

    @VisibleForTesting
    NavigableMap<Long, ReferenceSet> referencesByBookmark() {
        return referencesByBookmark;
    }

    public void writeJsonTo(SRMetaBlockWriter writer) throws IOException, SRMetaBlockException {
        rwLock.readLock().lock();
        try {
            writer.writeJson(this);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @VisibleForTesting
    int activeBookmarkCount() {
        rwLock.readLock().lock();
        try {
            return activeBookmarks.size();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @VisibleForTesting
    int referenceCount(long bookmarkId) {
        rwLock.readLock().lock();
        try {
            ReferenceSet refs = referencesByBookmark.get(bookmarkId);
            return refs == null ? 0 : refs.size();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /* ---------- internals ---------- */

    private OlapTable resolveTable() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new IllegalStateException("db " + dbId + " not found");
        }
        Table t = db.getTable(tableId);
        if (!(t instanceof OlapTable)) {
            throw new IllegalStateException("table " + tableId + " is not an OlapTable");
        }
        return (OlapTable) t;
    }

    /**
     * Returns the latest active bookmark if its partition meta matches the
     * candidate; older equivalent bookmarks are not consulted. This keeps
     * the bookmarkId returned by {@link #create} monotonic per holder, which
     * {@link BookmarkChange#computeChanges} relies on.
     */
    private Optional<Bookmark> findLatestEquivalent(Map<Long, Map<Long, PhysicalPartitionMeta>> partitionsMeta) {
        Map.Entry<Long, Bookmark> last = activeBookmarks.lastEntry();
        if (last == null) {
            return Optional.empty();
        }
        Bookmark latest = last.getValue();
        return partitionsMetaEquals(latest.getPartitionsMeta(), partitionsMeta) ? Optional.of(latest) : Optional.empty();
    }

    /** Two partition maps are equivalent for dedup. */
    private static boolean partitionsMetaEquals(
            Map<Long, Map<Long, PhysicalPartitionMeta>> a,
            Map<Long, Map<Long, PhysicalPartitionMeta>> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (Map.Entry<Long, Map<Long, PhysicalPartitionMeta>> e : a.entrySet()) {
            Map<Long, PhysicalPartitionMeta> bInner = b.get(e.getKey());
            Map<Long, PhysicalPartitionMeta> aInner = e.getValue();
            if (bInner == null || bInner.size() != aInner.size()) {
                return false;
            }
            for (Map.Entry<Long, PhysicalPartitionMeta> pe : aInner.entrySet()) {
                PhysicalPartitionMeta bm = bInner.get(pe.getKey());
                PhysicalPartitionMeta am = pe.getValue();
                if (bm == null) {
                    return false;
                }
                if (am.getBaseMaterializedIndexId() != bm.getBaseMaterializedIndexId()) {
                    return false;
                }
                if (am.getBaseMaterializedIndexMetaId() != bm.getBaseMaterializedIndexMetaId()) {
                    return false;
                }
                if (am.getVisibleVersion() != bm.getVisibleVersion()) {
                    return false;
                }
            }
        }
        return true;
    }

    // Caller holds rwLock.writeLock(); the apply runs under the same lock so
    // commit and visibility are atomic.
    private void journalAndApply(BookmarkLogEntry entry) {
        GlobalStateMgr.getCurrentState().getEditLog()
                .logBookmarkEntry(entry, o -> apply((BookmarkLogEntry) o, false));
    }

    /** Test hook fired after create() copies the table state into the in-flight bookmark. */
    protected void onCopyTableStateInBookmarkCreation() {
    }

    /** Caller must hold {@code rwLock.writeLock()}. */
    private void apply(BookmarkLogEntry entry, boolean isReplay) {
        if (entry instanceof BookmarkLogEntry.AddBookmark) {
            applyAddBookmark((BookmarkLogEntry.AddBookmark) entry, isReplay);
        } else if (entry instanceof BookmarkLogEntry.AcquireReference) {
            applyAcquireReference((BookmarkLogEntry.AcquireReference) entry, isReplay);
        } else if (entry instanceof BookmarkLogEntry.ReleaseReference) {
            applyReleaseReferences((BookmarkLogEntry.ReleaseReference) entry, isReplay);
        }
    }

    private void applyAddBookmark(BookmarkLogEntry.AddBookmark entry, boolean isReplay) {
        long bookmarkId = entry.getBookmark().getBookmarkId();
        if (activeBookmarks.containsKey(bookmarkId)) {
            // Idempotent replay path: image load already restored this bookmark.
            return;
        }
        Map<HolderId, Reference> refs = entry.getInitialReferences();
        if (refs.isEmpty()) {
            // The leader writes AddBookmark only with at least one reference;
            // an empty map here means a corrupted journal entry.
            throw new IllegalStateException(
                    "AddBookmark entry must carry at least one initial reference, bookmarkId=" + bookmarkId);
        }
        long earliestAcquired = Long.MAX_VALUE;
        for (Reference ref : refs.values()) {
            if (ref.getAcquiredAtMs() < earliestAcquired) {
                earliestAcquired = ref.getAcquiredAtMs();
            }
        }
        ReferenceSet refSet = new ReferenceSet(earliestAcquired);
        for (Map.Entry<HolderId, Reference> e : refs.entrySet()) {
            refSet.put(e.getKey(), e.getValue());
        }
        activeBookmarks.put(bookmarkId, entry.getBookmark());
        referencesByBookmark.put(bookmarkId, refSet);
        Level level = isReplay ? Level.DEBUG : Level.INFO;
        LOG.log(level, "bookmark created: db={}, table={}, bookmarkId={}", dbId, tableId, bookmarkId);
        for (HolderId holderId : refs.keySet()) {
            LOG.log(level, "bookmark reference added: db={}, table={}, bookmarkId={}, holder={}",
                    dbId, tableId, bookmarkId, holderId);
        }
    }

    private void applyAcquireReference(BookmarkLogEntry.AcquireReference entry, boolean isReplay) {
        ReferenceSet refSet = referencesByBookmark.get(entry.getBookmarkId());
        if (refSet == null) {
            // Bookmark was already released; the matching ReleaseReference
            // entry was applied first. Drop the acquire silently.
            return;
        }
        Level level = isReplay ? Level.DEBUG : Level.INFO;
        for (Map.Entry<HolderId, Reference> e : entry.getReferences().entrySet()) {
            refSet.put(e.getKey(), e.getValue());
            LOG.log(level, "bookmark reference added: db={}, table={}, bookmarkId={}, holder={}",
                    dbId, tableId, entry.getBookmarkId(), e.getKey());
        }
    }

    private void applyReleaseReferences(BookmarkLogEntry.ReleaseReference entry, boolean isReplay) {
        long bookmarkId = entry.getBookmarkId();
        ReferenceSet refSet = referencesByBookmark.get(bookmarkId);
        if (refSet == null) {
            // Idempotent replay or duplicate entry: the bookmark was already
            // reclaimed when its last reference left.
            return;
        }
        Level level = isReplay ? Level.DEBUG : Level.INFO;
        for (HolderId holderId : entry.getReferences().keySet()) {
            refSet.remove(holderId);
            LOG.log(level, "bookmark reference released: db={}, table={}, bookmarkId={}, holder={}",
                    dbId, tableId, bookmarkId, holderId);
        }
        if (refSet.isEmpty()) {
            activeBookmarks.remove(bookmarkId);
            referencesByBookmark.remove(bookmarkId);
            LOG.log(level, "bookmark removed: db={}, table={}, bookmarkId={}", dbId, tableId, bookmarkId);
        }
    }

    @VisibleForTesting
    public Bookmark peekCreating() {
        return creating;
    }
}
