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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.starrocks.lake.bookmark.BookmarkLogEntry.AcquireReference;
import com.starrocks.lake.bookmark.BookmarkLogEntry.AddBookmark;
import com.starrocks.lake.bookmark.BookmarkLogEntry.ReleaseReference;
import com.starrocks.lake.bookmark.BookmarkLogEntry.RenewReference;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BookmarkLogEntryTest {

    @Test
    public void testEntries() {
        Gson gson = new GsonBuilder()
                .enableComplexMapKeySerialization()
                .registerTypeAdapterFactory(BookmarkLogEntry.typeAdapterFactory())
                .registerTypeAdapterFactory(HolderInfo.typeAdapterFactory())
                .create();

        BookmarkHolder holder = BookmarkHolder.forEmptyInfo("h1");
        HolderId holderId = holder.getHolderId();
        Bookmark bookmark = new Bookmark(1L, 2L, 30L, 9999L, new HashMap<>());

        // An unregistered "rn" falls back to the reflective adapter, which writes no discriminator:
        // the entry serializes fine and only fails on replay, after it is durable.
        RenewReference rn = RenewReference.of(1L, 2L, 30L, holder,
                HolderInfo.EmptyInfo.INSTANCE, 1111L, 2222L, 7_000L, 4L);
        assertEquals(30L, rn.getBookmarkId());
        assertEquals(1111L, rn.getReferences().get(holderId).getAcquiredAtMs());
        assertEquals(2222L, rn.getReferences().get(holderId).getRenewedAtMs());
        assertEquals(4L, rn.getReferences().get(holderId).getRenewCount());
        BookmarkLogEntry rnBack = gson.fromJson(gson.toJson(rn, BookmarkLogEntry.class), BookmarkLogEntry.class);
        assertInstanceOf(RenewReference.class, rnBack);
        assertEquals(2222L, ((RenewReference) rnBack).getReferences().get(holderId).getRenewedAtMs());
        assertEquals(4L, ((RenewReference) rnBack).getReferences().get(holderId).getRenewCount());
        assertTrue(new RenewReference().getReferences().isEmpty());

        // AddBookmark.of()
        AddBookmark add = AddBookmark.of(bookmark, holder, 1234L, 7_000L);
        assertEquals(1L, add.getDbId());
        assertEquals(2L, add.getTableId());
        assertSame(bookmark, add.getBookmark());
        Map<HolderId, Reference> initial = add.getInitialReferences();
        assertEquals(1, initial.size());
        assertEquals(1234L, initial.get(holderId).getAcquiredAtMs());
        assertEquals(7_000L, initial.get(holderId).getTtlMs());

        // AddBookmark Gson round-trip
        String addJson = gson.toJson(add, BookmarkLogEntry.class);
        BookmarkLogEntry addBack = gson.fromJson(addJson, BookmarkLogEntry.class);
        AddBookmark addBack2 = assertInstanceOf(AddBookmark.class, addBack);
        assertEquals(1L, addBack2.getDbId());
        assertEquals(2L, addBack2.getTableId());
        assertEquals(30L, addBack2.getBookmark().getBookmarkId());
        assertEquals(1, addBack2.getInitialReferences().size());
        assertEquals(1234L, addBack2.getInitialReferences().get(holderId).getAcquiredAtMs());
        assertEquals(7_000L, addBack2.getInitialReferences().get(holderId).getTtlMs());

        // AcquireReference.of()
        AcquireReference acq = AcquireReference.of(1L, 2L, 30L, holder, 1234L, 8_000L);
        assertEquals(1L, acq.getDbId());
        assertEquals(2L, acq.getTableId());
        assertEquals(30L, acq.getBookmarkId());
        assertEquals(1, acq.getReferences().size());
        assertEquals(1234L, acq.getReferences().get(holderId).getAcquiredAtMs());
        assertEquals(8_000L, acq.getReferences().get(holderId).getTtlMs());

        String acqJson = gson.toJson(acq, BookmarkLogEntry.class);
        AcquireReference acqBack = assertInstanceOf(AcquireReference.class,
                gson.fromJson(acqJson, BookmarkLogEntry.class));
        assertEquals(30L, acqBack.getBookmarkId());
        assertEquals(1234L, acqBack.getReferences().get(holderId).getAcquiredAtMs());
        assertEquals(8_000L, acqBack.getReferences().get(holderId).getTtlMs());

        // ReleaseReference.of()
        Reference existing = new Reference(1234L, HolderInfo.EmptyInfo.INSTANCE, -1L);
        ReleaseReference rel = ReleaseReference.of(1L, 2L, 30L, holderId, existing);
        assertEquals(30L, rel.getBookmarkId());
        assertEquals(1, rel.getReferences().size());
        assertFalse(rel.isExpiredByTtl());

        ReleaseReference expired = new ReleaseReference(1L, 2L, 30L, rel.getReferences(), true);
        assertTrue(expired.isExpiredByTtl());
        ReleaseReference expiredBack = assertInstanceOf(ReleaseReference.class,
                gson.fromJson(gson.toJson(expired, BookmarkLogEntry.class), BookmarkLogEntry.class));
        assertTrue(expiredBack.isExpiredByTtl());

        String relJson = gson.toJson(rel, BookmarkLogEntry.class);
        assertInstanceOf(ReleaseReference.class, gson.fromJson(relJson, BookmarkLogEntry.class));

        // Null-safe getters: a default-constructed entry returns an empty map (never null).
        assertTrue(new AddBookmark().getInitialReferences().isEmpty());
        assertTrue(new AcquireReference().getReferences().isEmpty());
        assertTrue(new ReleaseReference().getReferences().isEmpty());
    }
}
