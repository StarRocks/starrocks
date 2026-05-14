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
import com.starrocks.catalog.MvId;
import com.starrocks.persist.gson.RuntimeTypeAdapterFactory;

import java.util.Objects;

/**
 * Type-specific sidecar metadata attached to a bookmark holder, stored on
 * {@link Reference}. Polymorphic via {@link #typeAdapterFactory()}.
 */
public interface HolderInfo {

    static RuntimeTypeAdapterFactory<HolderInfo> typeAdapterFactory() {
        return RuntimeTypeAdapterFactory.of(HolderInfo.class, "clazz")
                .registerSubtype(MvInfo.class, "mv")
                .registerSubtype(EmptyInfo.class, "e");
    }

    /* ---------- subclasses ---------- */

    /**
     * Sidecar info for a materialized-view holder. Carries the {@link MvId} so
     * the cleanup path can probe the metastore to verify the MV still exists.
     */
    final class MvInfo implements HolderInfo {
        @SerializedName("mv")
        private final MvId mvId;

        public MvInfo(MvId mvId) {
            this.mvId = Objects.requireNonNull(mvId, "mvId");
        }

        public MvId getMvId() {
            return mvId;
        }
    }

    /** Sidecar info marker meaning "no extra metadata". Use {@link #INSTANCE}. */
    final class EmptyInfo implements HolderInfo {
        public static final EmptyInfo INSTANCE = new EmptyInfo();

        private EmptyInfo() {
        }
    }
}
