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

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.RuntimeTypeAdapterFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Journal record for the bookmark module; carries the target {@code (dbId, tableId)}. */
public abstract class BookmarkLogEntry implements Writable {
    @SerializedName("db")
    protected long dbId;
    @SerializedName("t")
    protected long tableId;

    protected BookmarkLogEntry() {
    }

    protected BookmarkLogEntry(long dbId, long tableId) {
        this.dbId = dbId;
        this.tableId = tableId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    /** Polymorphic dispatch for Gson; the {@code "clazz"} discriminator picks the subclass. */
    public static RuntimeTypeAdapterFactory<BookmarkLogEntry> typeAdapterFactory() {
        return RuntimeTypeAdapterFactory.of(BookmarkLogEntry.class, "clazz")
                .registerSubtype(AddBookmark.class, "ab")
                .registerSubtype(AcquireReference.class, "ar")
                .registerSubtype(ReleaseReference.class, "rr");
    }

    /* ---------- subclasses ---------- */

    /** Adds a brand-new bookmark together with its initial reference set. */
    public static final class AddBookmark extends BookmarkLogEntry {
        @SerializedName("m")
        private Bookmark bookmark;
        @SerializedName("rs")
        private Map<HolderId, Reference> initialReferences;

        public AddBookmark() {
        }

        public AddBookmark(Bookmark bookmark, Map<HolderId, Reference> initialReferences) {
            super(bookmark.getDbId(), bookmark.getTableId());
            this.bookmark = bookmark;
            this.initialReferences = initialReferences;
        }

        public Bookmark getBookmark() {
            return bookmark;
        }

        public Map<HolderId, Reference> getInitialReferences() {
            return initialReferences == null ? Collections.emptyMap() : initialReferences;
        }

        public static AddBookmark of(Bookmark bookmark, BookmarkHolder holder, long acquiredAtMs) {
            Map<HolderId, Reference> map = new HashMap<>(1);
            map.put(holder.getHolderId(), new Reference(acquiredAtMs, holder.getHolderInfo()));
            return new AddBookmark(bookmark, map);
        }
    }

    /** Acquires references on one already-tracked bookmark. */
    public static final class AcquireReference extends BookmarkLogEntry {
        @SerializedName("b")
        private long bookmarkId;
        @SerializedName("rs")
        private Map<HolderId, Reference> references;

        public AcquireReference() {
        }

        public AcquireReference(long dbId, long tableId, long bookmarkId, Map<HolderId, Reference> references) {
            super(dbId, tableId);
            this.bookmarkId = bookmarkId;
            this.references = references;
        }

        public long getBookmarkId() {
            return bookmarkId;
        }

        public Map<HolderId, Reference> getReferences() {
            return references == null ? Collections.emptyMap() : references;
        }

        public static AcquireReference of(long dbId, long tableId, long bookmarkId,
                                          BookmarkHolder holder, long acquiredAtMs) {
            Map<HolderId, Reference> map = new HashMap<>(1);
            map.put(holder.getHolderId(), new Reference(acquiredAtMs, holder.getHolderInfo()));
            return new AcquireReference(dbId, tableId, bookmarkId, map);
        }
    }

    /** Releases references on one tracked bookmark. */
    public static final class ReleaseReference extends BookmarkLogEntry {
        @SerializedName("b")
        private long bookmarkId;
        @SerializedName("rs")
        private Map<HolderId, Reference> references;

        public ReleaseReference() {
        }

        public ReleaseReference(long dbId, long tableId, long bookmarkId, Map<HolderId, Reference> references) {
            super(dbId, tableId);
            this.bookmarkId = bookmarkId;
            this.references = references;
        }

        public long getBookmarkId() {
            return bookmarkId;
        }

        public Map<HolderId, Reference> getReferences() {
            return references == null ? Collections.emptyMap() : references;
        }

        public static ReleaseReference of(long dbId, long tableId, long bookmarkId,
                                          HolderId holderId, Reference reference) {
            Map<HolderId, Reference> map = new HashMap<>(1);
            map.put(holderId, reference);
            return new ReleaseReference(dbId, tableId, bookmarkId, map);
        }
    }
}
