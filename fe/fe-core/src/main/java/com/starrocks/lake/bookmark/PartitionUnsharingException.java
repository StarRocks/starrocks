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
 * A physical partition is mid-UNSHARE, so no bookmark can be taken on it.
 *
 * <p>While an ORDER BY != PK split is unsharing, writes use the newly installed child layout but
 * queries stay pinned to the superseded parent. A bookmark has to name one index and one version,
 * and during that window neither choice is sound: the writable child paired with the partition's
 * current version sends a read down the normal scan path, whose range pruning is exactly what the
 * pin exists to avoid; the queryable parent paired with that same version names a combination that
 * never existed on the parent, since its last version precedes the split commit. Rather than pick a
 * semantics with a known hole, refuse for the duration -- it lasts one compaction.
 */
public class PartitionUnsharingException extends BookmarkException {
    public PartitionUnsharingException(String message) {
        super(message);
    }
}
