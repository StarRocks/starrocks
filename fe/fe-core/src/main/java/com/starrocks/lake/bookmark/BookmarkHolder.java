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

import com.starrocks.catalog.MvId;

/**
 * Caller-facing bundle of a holder's identity and its type-specific sidecar
 * info. Used only at API boundaries; internally the manager and tracker key by
 * {@link HolderId} and persist {@link HolderInfo} on {@link Reference}. The
 * factories pair the id and the info from one source value, so caller cannot
 * produce a mismatched pair.
 */
public final class BookmarkHolder {
    private final HolderId holderId;
    private final HolderInfo holderInfo;

    private BookmarkHolder(HolderId holderId, HolderInfo holderInfo) {
        this.holderId = holderId;
        this.holderInfo = holderInfo;
    }

    public static BookmarkHolder forMv(MvId mvId) {
        return new BookmarkHolder(HolderId.forMv(mvId), new HolderInfo.MvInfo(mvId));
    }

    public static BookmarkHolder forEmptyInfo(String holderId) {
        return new BookmarkHolder(new HolderId(holderId), HolderInfo.EmptyInfo.INSTANCE);
    }

    public HolderId getHolderId() {
        return holderId;
    }

    public HolderInfo getHolderInfo() {
        return holderInfo;
    }
}
