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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.starrocks.catalog.Table;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Tables a statement authorized for data access while it was being planned, for the one caller that resolves
 * the same table again once planning is over: ANALYZE, which hands its table to the thread that collects.
 *
 * Deliberately not a field on the connector metadata, which is where it belongs by shape. The planner drops
 * the whole per-query metadata cache the moment planning ends (MetadataMgr#removeQueryMetadata, called from
 * StatementPlanner's finally block), so the next resolution is served by a brand new instance - a memo living
 * on the instance is empty exactly when the second resolution needs it, and ANALYZE fails with the refusal
 * meant for callers that have no statement at all.
 *
 * Keyed by query id, which is what keeps it honest: one statement cannot read what another authorized, and a
 * thread with no query of its own - a cache loader, a follower replaying a journal - finds nothing here and
 * stays refused. Entries hold an authorized table, never a credential.
 */
final class LakeFormationPlannedTables {

    // The second resolution happens within milliseconds of the first, on the same thread. The bound and the
    // expiry are here for the statements that never come back - one that failed between planning and
    // execution leaves its entry behind, and nothing else would ever remove it.
    //
    // One entry per statement, not per table, and every statement that resolves a governed table for data
    // access writes one. So eviction needs this many other statements to start inside the gap between one
    // statement's two resolutions - a gap of milliseconds - which is why the bound is well above what a
    // busy cluster has in flight rather than sized for the handover itself.
    //
    // If it did evict, the second resolution finds nothing and is refused: the statement fails, saying it
    // has no planning attempt. Confusing, but fail-closed - it can never hand back the wrong table.
    private static final Cache<String, Map<LakeFormationTableIdentity, Table>> BY_QUERY =
            CacheBuilder.newBuilder()
                    .maximumSize(8192)
                    .expireAfterWrite(5, TimeUnit.MINUTES)
                    .build();

    private LakeFormationPlannedTables() {
    }

    static void remember(String queryId, LakeFormationTableIdentity identity, Table table) {
        try {
            BY_QUERY.get(queryId, ConcurrentHashMap::new).put(identity, table);
        } catch (ExecutionException e) {
            // ConcurrentHashMap::new does not throw, so this cannot happen; losing the memo would only cost
            // the statement its handover, which is not worth failing the resolution that just succeeded.
            throw new IllegalStateException(e);
        }
    }

    static Table find(String queryId, LakeFormationTableIdentity identity) {
        Map<LakeFormationTableIdentity, Table> planned = BY_QUERY.getIfPresent(queryId);
        return planned == null ? null : planned.get(identity);
    }

    /** Test seam: the cache is static, so one test's statement must not be visible to the next one's. */
    static void forgetAll() {
        BY_QUERY.invalidateAll();
    }
}
