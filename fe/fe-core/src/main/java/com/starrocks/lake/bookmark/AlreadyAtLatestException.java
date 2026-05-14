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

/**
 * Raised by {@code create} when the holder already references a bookmark whose
 * partition meta matches the current table state — the create would produce
 * no new bookmark and the holder's reference is unchanged.
 */
public class AlreadyAtLatestException extends BookmarkException {

    private final long bookmarkId;

    public AlreadyAtLatestException(long dbId, long tableId, long bookmarkId, HolderId holderId) {
        super("holder " + holderId + " is already at the latest bookmark "
                + bookmarkId + " (db=" + dbId + ", table=" + tableId + ")");
        this.bookmarkId = bookmarkId;
    }

    public long getBookmarkId() {
        return bookmarkId;
    }
}
