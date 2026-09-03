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

package com.starrocks.lake.changes;

import com.starrocks.thrift.TChangesScanCacheMode;

/**
 * Whether a CHANGES scan writes the data it reads back into the compute node's caches. The table's
 * schema entry is outside this control: it is cached unconditionally and shared across every reader
 * of the table, so no mode suppresses it.
 *
 * <p>The right answer differs by workload, which is why this is a choice and not a fixed rule.
 * A materialized view refreshing every few minutes reads a narrow window that ordinary queries
 * are reading too, so caching it serves both. A one-off backfill walks a long stretch of history
 * instead, most of which no later query will ask for again, and filling the cache with it evicts
 * what the live workload depends on. A scan cannot tell from the inside which of the two it is.
 *
 * <p>{@code AUTO} means the system decides. It currently resolves to {@code ALWAYS}; a rule that
 * tells the two workloads apart — for instance from how many materialized views read the same base
 * table — belongs in {@link #resolve()}, so that whoever wrote {@code NEVER} or {@code ALWAYS} keeps
 * exactly what they asked for.
 */
public enum ChangesScanCacheMode {
    AUTO("auto"),
    NEVER("never"),
    ALWAYS("always");

    private final String modeName;

    ChangesScanCacheMode(String modeName) {
        this.modeName = modeName;
    }

    public String modeName() {
        return modeName;
    }

    public static ChangesScanCacheMode fromName(String modeName) {
        if (modeName != null) {
            for (ChangesScanCacheMode mode : values()) {
                if (mode.modeName.equalsIgnoreCase(modeName)) {
                    return mode;
                }
            }
        }
        throw new IllegalArgumentException(
                "Unknown changes scan cache mode: " + modeName + ", only support auto, never and always");
    }

    /** Decides what AUTO means, so the backend never has to. */
    public TChangesScanCacheMode resolve() {
        return this == NEVER ? TChangesScanCacheMode.NEVER : TChangesScanCacheMode.ALWAYS;
    }
}
