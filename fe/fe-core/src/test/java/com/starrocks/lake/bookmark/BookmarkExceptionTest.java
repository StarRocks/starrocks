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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BookmarkExceptionTest {

    @Test
    public void testMessages() {
        HolderId holderId = new HolderId("h1");
        long dbId = 11L;
        long tableId = 22L;
        long bookmarkId = 33L;

        AlreadyAtLatestException atLatest = new AlreadyAtLatestException(dbId, tableId, bookmarkId, holderId);
        assertContainsAll(atLatest.getMessage(), holderId.toString(), "11", "22", "33");
        assertEquals(bookmarkId, atLatest.getBookmarkId());

        AlreadyReferencedException refAlready = new AlreadyReferencedException(dbId, tableId, bookmarkId, holderId);
        assertContainsAll(refAlready.getMessage(), holderId.toString(), "11", "22", "33");

        BookmarkNotFoundException notFound = new BookmarkNotFoundException(dbId, tableId, bookmarkId, holderId);
        assertContainsAll(notFound.getMessage(), holderId.toString(), "11", "22", "33");

        ReferenceNotFoundException refNotFound = new ReferenceNotFoundException(dbId, tableId, bookmarkId, holderId);
        assertContainsAll(refNotFound.getMessage(), holderId.toString(), "11", "22", "33");
    }

    private static void assertContainsAll(String message, String... fragments) {
        for (String fragment : fragments) {
            assertTrue(message.contains(fragment),
                    "expected message to contain '" + fragment + "', but was: " + message);
        }
    }
}
