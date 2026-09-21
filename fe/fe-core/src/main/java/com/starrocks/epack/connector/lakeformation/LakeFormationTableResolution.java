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

package com.starrocks.epack.connector.lakeformation;

import com.starrocks.catalog.Table;

/**
 * One table's outcome inside a single query's Lake Formation memo.
 *
 * A failure is sticky on purpose: within one planning attempt a table that failed authorization must keep
 * failing. Retrying it would let a transient Lake Formation error turn into a silently different
 * authorization, and a second call succeeding after a first that failed is exactly the "fallback after
 * refusal" the design forbids.
 */
public final class LakeFormationTableResolution {

    private final Table authorizedTable;
    private final boolean unregistered;
    private final LakeFormationTableAccessException failure;

    private LakeFormationTableResolution(Table authorizedTable, boolean unregistered,
                                         LakeFormationTableAccessException failure) {
        this.authorizedTable = authorizedTable;
        this.unregistered = unregistered;
        this.failure = failure;
    }

    public static LakeFormationTableResolution authorized(Table table) {
        return new LakeFormationTableResolution(table, false, null);
    }

    /** The table exists but Lake Formation explicitly answered IsRegisteredWithLakeFormation = false. */
    public static LakeFormationTableResolution unregistered() {
        return new LakeFormationTableResolution(null, true, null);
    }

    public static LakeFormationTableResolution failed(LakeFormationTableAccessException failure) {
        return new LakeFormationTableResolution(null, false, failure);
    }

    public boolean isUnregistered() {
        return unregistered;
    }

    /**
     * Rethrows the memoized failure wrapped in a new exception, so that each caller gets its own stack
     * trace. Rethrowing the original would leave the trace pointing at whichever path failed first.
     */
    public void rethrowIfFailed() {
        if (failure != null) {
            throw new LakeFormationTableAccessException(failure.getMessage(), failure);
        }
    }

    public Table authorizedTable() {
        return authorizedTable;
    }
}
