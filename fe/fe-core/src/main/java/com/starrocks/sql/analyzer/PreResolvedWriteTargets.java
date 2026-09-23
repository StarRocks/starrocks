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

package com.starrocks.sql.analyzer;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;

import java.util.Map;

/**
 * A DML's write target when it lives in an external catalog, resolved before the meta lock was taken.
 *
 * <p><b>Why the target needs this at all.</b> The unlocked pre-pass
 * ({@code QueryAnalyzer#analyzeExternalTablesOnly}) resolves the tables a statement <em>reads</em>, and
 * deliberately leaves the write target to the locked analyzer -- for an internal target that is exactly
 * right, since the target is the object the lock protects. An external target is the opposite case: the FE
 * meta lock protects nothing about it, yet every DML analyzer resolves it with that lock held, so
 * {@code MERGE INTO iceberg_cat.db.t USING internal_tbl} contacts the iceberg catalog inside the internal
 * source table's READ lock -- the T3 shape of the original report, on the write side.
 *
 * <p><b>Why no "still current" check.</b> The rule for moving a resolve out of a critical section is to ask
 * whether what was captured can go stale while the lock is held, the way a view definition can. Here it
 * cannot, for two independent reasons. The lock never covered this object: an external table is not in
 * {@code PlannerMetaLocker}'s lock set, so holding the lock never made its metadata stable and resolving it
 * again would not either. And the second resolve would read the same answer anyway -- connector metadata is
 * snapshotted per query id ({@code MetadataMgr#getOptionalMetadata}), so both resolves see one snapshot of
 * the external catalog. Handing the table over is therefore equivalent to resolving it twice, minus the
 * round trip that the first resolve may pay.
 *
 * <p>Entries are handed out at most once and dropped with the statement, like {@link PreResolvedViewBodies}:
 * a statement has one write target, and anything the analyzer did not come to collect must not be offered to
 * the next statement on this connection.
 */
public class PreResolvedWriteTargets {
    private final Map<String, Table> byQualifiedName = Maps.newHashMap();

    /**
     * @param tableName fully qualified, i.e. already through {@code TableName#normalization}, because that
     *                  is the form the analyzer will look it up by
     */
    public void put(TableName tableName, Table table) {
        byQualifiedName.put(key(tableName), table);
    }

    /**
     * @return the table pre-resolved for this name, or null when the pre-pass did not resolve it -- which is
     *         the ordinary case for an internal target, for a statement planned with no pre-pass, and for
     *         one whose pre-resolve failed. Every caller falls back to resolving it itself, so a miss costs
     *         what the code did before this class existed.
     */
    public Table take(TableName tableName) {
        return byQualifiedName.remove(key(tableName));
    }

    public boolean isEmpty() {
        return byQualifiedName.isEmpty();
    }

    public void clear() {
        byQualifiedName.clear();
    }

    private static String key(TableName tableName) {
        return tableName.getCatalog() + "." + tableName.getDb() + "." + tableName.getTbl();
    }
}
