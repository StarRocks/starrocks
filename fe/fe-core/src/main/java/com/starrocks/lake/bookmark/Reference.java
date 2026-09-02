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
import com.starrocks.persist.gson.GsonPostProcessable;

import java.io.IOException;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * One holder's reference to a bookmark. Immutable. The holder identity is the
 * surrounding map key (a {@link HolderId}); this object carries the acquisition
 * time, the holder's type-specific sidecar {@link HolderInfo}, and a per-reference
 * time-to-live in ms ({@code <= 0} drops only this limit, leaving the cluster ceiling).
 *
 * <p>Renewal moves {@code renewedAtMs} only; {@code acquiredAtMs} feeds CREATE_TIME and the
 * oldest/newest system-table columns, and restamping it would hide long-held pins from them.
 */
public final class Reference implements GsonPostProcessable {
    @SerializedName("at")
    private final long acquiredAtMs;
    @SerializedName("i")
    private final HolderInfo holderInfo;
    @SerializedName("ttl")
    private final long ttlMs;
    /** 0 = never renewed; pre-renewal journals and images deserialize to this too. */
    @SerializedName("rn")
    private final long renewedAtMs;
    /** Successful renewals of this reference; 0 if never renewed. */
    @SerializedName("rc")
    private long renewCount;

    public Reference(long acquiredAtMs, HolderInfo holderInfo, long ttlMs) {
        this(acquiredAtMs, holderInfo, ttlMs, 0L, 0L);
    }

    public Reference(long acquiredAtMs, HolderInfo holderInfo, long ttlMs, long renewedAtMs) {
        this(acquiredAtMs, holderInfo, ttlMs, renewedAtMs, 0L);
    }

    public Reference(long acquiredAtMs, HolderInfo holderInfo, long ttlMs, long renewedAtMs, long renewCount) {
        this.acquiredAtMs = acquiredAtMs;
        this.holderInfo = Objects.requireNonNull(holderInfo, "holderInfo");
        this.ttlMs = ttlMs;
        this.renewedAtMs = renewedAtMs;
        this.renewCount = atLeastOneIfRenewed(renewedAtMs, renewCount);
    }

    /**
     * Pre-{@code rc} journals store {@code rn} with no count. Gson leaves the primitive at 0, which
     * the information schema documents as "never renewed" even when LAST_RENEW_TIME is set. The
     * historical N is gone; 1 is the lower bound that keeps 0 meaning never.
     */
    @Override
    public void gsonPostProcess() throws IOException {
        renewCount = atLeastOneIfRenewed(renewedAtMs, renewCount);
    }

    private static long atLeastOneIfRenewed(long renewedAtMs, long renewCount) {
        return renewedAtMs > 0 && renewCount == 0 ? 1L : renewCount;
    }

    public long getAcquiredAtMs() {
        return acquiredAtMs;
    }

    public long getRenewedAtMs() {
        return renewedAtMs;
    }

    public long getRenewCount() {
        return renewCount;
    }

    /** Where the current lease starts: the last renewal, or the acquisition when never renewed. */
    public long leaseStartMs() {
        return renewedAtMs > 0 ? renewedAtMs : acquiredAtMs;
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
        return effectiveTtlMs(ttlMs, maxTtlMs);
    }

    /** Same rule without a reference to hand; bookmark_renew reports this back to its caller. */
    public static long effectiveTtlMs(long ttlMs, long maxTtlMs) {
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
        return eff > 0 && leaseStartMs() + eff <= nowMs;
    }

    /** Epoch millis when the effective lease ends; empty when there is no expiry. */
    public OptionalLong expireAtMs(long maxTtlMs) {
        long eff = effectiveTtlMs(maxTtlMs);
        if (eff <= 0) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(leaseStartMs() + eff);
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
        private final long renewedAtMs;
        private final long renewCount;

        public View(String holderId, long acquiredAtMs, long ttlMs) {
            this(holderId, acquiredAtMs, ttlMs, 0L, 0L);
        }

        public View(String holderId, long acquiredAtMs, long ttlMs, long renewedAtMs) {
            this(holderId, acquiredAtMs, ttlMs, renewedAtMs, 0L);
        }

        public View(String holderId, long acquiredAtMs, long ttlMs, long renewedAtMs, long renewCount) {
            this.holderId = holderId;
            this.acquiredAtMs = acquiredAtMs;
            this.ttlMs = ttlMs;
            this.renewedAtMs = renewedAtMs;
            this.renewCount = renewCount;
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

        /** 0 when never renewed. */
        public long getRenewedAtMs() {
            return renewedAtMs;
        }

        public long getRenewCount() {
            return renewCount;
        }

        public long leaseStartMs() {
            return renewedAtMs > 0 ? renewedAtMs : acquiredAtMs;
        }

        public OptionalLong expireAtMs(long maxTtlMs) {
            long eff = Reference.effectiveTtlMs(ttlMs, maxTtlMs);
            if (eff <= 0) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(leaseStartMs() + eff);
        }
    }
}
