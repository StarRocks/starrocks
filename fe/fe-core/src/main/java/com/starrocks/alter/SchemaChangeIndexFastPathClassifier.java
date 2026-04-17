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

package com.starrocks.alter;

import com.starrocks.catalog.Index;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.CreateIndexClause;
import com.starrocks.sql.ast.DropIndexClause;
import com.starrocks.sql.ast.IndexDef;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Coarse classifier that decides whether an ADD INDEX or DROP INDEX alter
 * statement is eligible for the lake-only fast path producing standalone
 * {@code .idx} files (Index Delta Group) instead of rewriting segment data.
 *
 * <p>This helper covers the safe, cheap-to-check preconditions. Finer-grained
 * column-level rules (short-key / sort-key / PK-column exclusion, column-type
 * vs index-type compatibility via {@code IndexAnalyzer.checkColumn}) are
 * enforced at job-construction time inside the forthcoming
 * {@code LakeTableAddIndexJob} / {@code LakeTableDropIndexJob} to keep the
 * responsibility split clean and avoid duplicating table-internals lookups here.
 *
 * <p>Supported index types for the fast path: BITMAP, NGRAMBF, GIN. VECTOR is
 * intentionally excluded (separate lifecycle / parameter surface). Bloom filter
 * is a table-level property rather than a {@link CreateIndexClause}, so it is
 * not routed through this path.
 */
public final class SchemaChangeIndexFastPathClassifier {

    private static final Logger LOG = LogManager.getLogger(SchemaChangeIndexFastPathClassifier.class);

    private SchemaChangeIndexFastPathClassifier() {
    }

    /**
     * @return true if every clause in {@code alterClauses} is a
     *         {@link CreateIndexClause} whose index type is supported, AND the
     *         table is a cloud-native (lake) table. Column-level checks run
     *         later in the Job class.
     */
    public static boolean shouldUseAddIndexFastPath(OlapTable table, List<AlterClause> alterClauses) {
        if (table == null || !table.isCloudNativeTableOrMaterializedView()) {
            return false;
        }
        if (alterClauses == null || alterClauses.isEmpty()) {
            return false;
        }
        for (AlterClause clause : alterClauses) {
            if (!(clause instanceof CreateIndexClause)) {
                // Mixed alter: anything other than CreateIndexClause forces the
                // regular path. Keeps the fast-path contract narrow and safe.
                return false;
            }
            IndexDef def = ((CreateIndexClause) clause).getIndexDef();
            if (def == null || !isSupportedIndexType(def.getIndexType())) {
                LOG.debug("ADD INDEX fast-path rejected: index type {} not supported",
                        def == null ? null : def.getIndexType());
                return false;
            }
        }
        return true;
    }

    /**
     * @return true if every clause in {@code alterClauses} is a
     *         {@link DropIndexClause} naming an existing index of a supported
     *         type, AND the table is a cloud-native (lake) table.
     */
    public static boolean shouldUseDropIndexFastPath(OlapTable table, List<AlterClause> alterClauses) {
        if (table == null || !table.isCloudNativeTableOrMaterializedView()) {
            return false;
        }
        if (alterClauses == null || alterClauses.isEmpty()) {
            return false;
        }
        for (AlterClause clause : alterClauses) {
            if (!(clause instanceof DropIndexClause)) {
                return false;
            }
            DropIndexClause drop = (DropIndexClause) clause;
            Index existing = findIndexByName(table, drop.getIndexName());
            if (existing == null) {
                // Analyzer should have failed earlier; be defensive here too.
                return false;
            }
            if (!isSupportedIndexType(existing.getIndexType())) {
                return false;
            }
        }
        return true;
    }

    public static boolean isSupportedIndexType(IndexDef.IndexType type) {
        if (type == null) {
            return false;
        }
        switch (type) {
            case BITMAP:
            case NGRAMBF:
            case GIN:
                return true;
            case VECTOR:
            default:
                return false;
        }
    }

    private static Index findIndexByName(OlapTable table, String name) {
        List<Index> all = table.getIndexes();
        if (all == null) {
            return null;
        }
        for (Index ix : all) {
            if (ix.getIndexName().equalsIgnoreCase(name)) {
                return ix;
            }
        }
        return null;
    }
}
