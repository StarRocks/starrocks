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

import java.util.Objects;

/**
 * One holder's reference to a bookmark. Immutable. The holder identity is the
 * surrounding map key (a {@link HolderId}); this object carries the acquisition
 * time, the holder's type-specific sidecar {@link HolderInfo}, and a per-reference
 * time-to-live in ms ({@code <= 0} disables expiry).
 */
public final class Reference {
    @SerializedName("at")
    private final long acquiredAtMs;
    @SerializedName("i")
    private final HolderInfo holderInfo;
    @SerializedName("ttl")
    private final long ttlMs;

    public Reference(long acquiredAtMs, HolderInfo holderInfo, long ttlMs) {
        this.acquiredAtMs = acquiredAtMs;
        this.holderInfo = Objects.requireNonNull(holderInfo, "holderInfo");
        this.ttlMs = ttlMs;
    }

    public long getAcquiredAtMs() {
        return acquiredAtMs;
    }

    public HolderInfo getHolderInfo() {
        return holderInfo;
    }

    public long getTtlMs() {
        return ttlMs;
    }

    /**
     * Effective TTL in ms: the cluster ceiling caps the per-reference TTL, so the
     * smaller of the two limits applies. Either side {@code <= 0} means "no limit";
     * if neither sets one the result is {@code -1} (no expiry).
     */
    public long effectiveTtlMs(long maxTtlMs) {
        if (ttlMs <= 0 && maxTtlMs <= 0) {
            return -1;
        }
        if (ttlMs <= 0) {
            return maxTtlMs;
        }
        if (maxTtlMs <= 0) {
            return ttlMs;
        }
        return Math.min(ttlMs, maxTtlMs);
    }

    /** True when this reference's effective lifetime has elapsed by {@code nowMs}. */
    public boolean isExpired(long nowMs, long maxTtlMs) {
        long eff = effectiveTtlMs(maxTtlMs);
        return eff > 0 && acquiredAtMs + eff <= nowMs;
    }

    /**
     * Read-only externalized form of one reference: holder identity as a string
     * (the holder-type sidecar is dropped), the acquisition timestamp, and the
     * per-reference TTL.
     */
    public static final class View {
        private final String holderId;
        private final long acquiredAtMs;
        private final long ttlMs;

        public View(String holderId, long acquiredAtMs, long ttlMs) {
            this.holderId = holderId;
            this.acquiredAtMs = acquiredAtMs;
            this.ttlMs = ttlMs;
        }

        public String getHolderId() {
            return holderId;
        }

        public long getAcquiredAtMs() {
            return acquiredAtMs;
        }

        public long getTtlMs() {
            return ttlMs;
        }
    }
}
