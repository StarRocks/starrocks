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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Holds every reference on one bookmark, keyed by {@link HolderId}. Not
 * thread-safe; callers must hold the owning tracker's lock.
 *
 * <p>{@code referencedSinceMs} is fixed at the moment the set is created — the
 * earliest acquisition time of its initial holders — and is not bumped when
 * later holders join. Once the set empties it is dropped together with the
 * bookmark; if the same bookmark is later re-referenced, a fresh set is
 * created with its own {@code referencedSinceMs}.
 */
public final class ReferenceSet {
    @SerializedName("st")
    private final long referencedSinceMs;
    @SerializedName("rs")
    private final Map<HolderId, Reference> references = new HashMap<>();

    public ReferenceSet(long referencedSinceMs) {
        this.referencedSinceMs = referencedSinceMs;
    }

    public long getReferencedSinceMs() {
        return referencedSinceMs;
    }

    /** Idempotent: if the holder already has a reference, this call is a no-op. */
    public void put(HolderId holderId, Reference ref) {
        references.putIfAbsent(holderId, ref);
    }

    public Reference get(HolderId holderId) {
        return references.get(holderId);
    }

    public void remove(HolderId holderId) {
        references.remove(holderId);
    }

    public int size() {
        return references.size();
    }

    public boolean isEmpty() {
        return references.isEmpty();
    }

    public Map<HolderId, Reference> entries() {
        return references;
    }

    public Collection<Reference> values() {
        return references.values();
    }
}
