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
 * time and the holder's type-specific sidecar {@link HolderInfo}.
 */
public final class Reference {
    @SerializedName("at")
    private final long acquiredAtMs;
    @SerializedName("i")
    private final HolderInfo holderInfo;

    public Reference(long acquiredAtMs, HolderInfo holderInfo) {
        this.acquiredAtMs = acquiredAtMs;
        this.holderInfo = Objects.requireNonNull(holderInfo, "holderInfo");
    }

    public long getAcquiredAtMs() {
        return acquiredAtMs;
    }

    public HolderInfo getHolderInfo() {
        return holderInfo;
    }

    /**
     * Read-only externalized form of one reference: holder identity as a
     * string (the holder-type sidecar is dropped) and the acquisition
     * timestamp.
     */
    public static final class View {
        private final String holderId;
        private final long acquiredAtMs;

        public View(String holderId, long acquiredAtMs) {
            this.holderId = holderId;
            this.acquiredAtMs = acquiredAtMs;
        }

        public String getHolderId() {
            return holderId;
        }

        public long getAcquiredAtMs() {
            return acquiredAtMs;
        }
    }
}
