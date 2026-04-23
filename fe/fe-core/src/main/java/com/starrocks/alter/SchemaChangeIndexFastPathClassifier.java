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
import com.starrocks.common.Config;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.CreateIndexClause;
import com.starrocks.sql.ast.DropIndexClause;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        if (!Config.enable_lake_add_index_fast_path) {
            return false;
        }
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
        if (!Config.enable_lake_add_index_fast_path) {
            return false;
        }
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

    /**
     * Result of classifying a {@code bloom_filter_columns} property alter.
     * Carries the delta (columns added / dropped relative to the current
     * table state) so the caller can route the job without re-parsing the
     * property map.
     */
    public static final class BloomFilterDelta {
        public final Set<String> added;
        public final Set<String> dropped;
        private BloomFilterDelta(Set<String> added, Set<String> dropped) {
            this.added = added;
            this.dropped = dropped;
        }
        public boolean isPureAdd() {
            return !added.isEmpty() && dropped.isEmpty();
        }
        public boolean isPureDrop() {
            return added.isEmpty() && !dropped.isEmpty();
        }
    }

    /**
     * Classify a {@link ModifyTablePropertiesClause} that touches
     * {@code bloom_filter_columns}. Returns a {@link BloomFilterDelta} if
     * the alter is a pure add or pure drop on a lake table — and therefore
     * eligible for the IDG fast path — or null otherwise.
     *
     * <p>Rules:
     * <ul>
     *   <li>Must be a single clause (no mixed alter).</li>
     *   <li>Lake table, classifier config enabled.</li>
     *   <li>Only {@code bloom_filter_columns} (and optionally
     *       {@code bloom_filter_fpp} left unchanged) in the property map.
     *       An fpp change forces the legacy path because existing segments
     *       were built with the old fpp and must be rewritten to stay
     *       consistent — the fast path only appends new payloads.</li>
     *   <li>The new set of bf columns differs from the old set in exactly
     *       one direction — either strictly adds or strictly drops. Mixed
     *       changes fall back.</li>
     * </ul>
     */
    public static BloomFilterDelta classifyBloomFilterChange(OlapTable table, List<AlterClause> alterClauses) {
        if (!Config.enable_lake_add_index_fast_path) {
            return null;
        }
        if (table == null || !table.isCloudNativeTableOrMaterializedView()) {
            return null;
        }
        if (alterClauses == null || alterClauses.size() != 1) {
            return null;
        }
        AlterClause clause = alterClauses.get(0);
        if (!(clause instanceof ModifyTablePropertiesClause)) {
            return null;
        }
        Map<String, String> props = ((ModifyTablePropertiesClause) clause).getProperties();
        if (props == null || props.isEmpty()) {
            return null;
        }
        // Reject the clause when it carries properties other than the two
        // bloom-filter ones. Keeps the fast-path contract narrow and safe.
        for (String key : props.keySet()) {
            if (!PropertyAnalyzer.PROPERTIES_BF_COLUMNS.equalsIgnoreCase(key)
                    && !PropertyAnalyzer.PROPERTIES_BF_FPP.equalsIgnoreCase(key)) {
                return null;
            }
        }
        // If fpp is present, require it to match the current fpp — an fpp
        // change demands rebuilding all existing BFs, which the fast path
        // is not designed for.
        String fppStr = null;
        for (Map.Entry<String, String> e : props.entrySet()) {
            if (PropertyAnalyzer.PROPERTIES_BF_FPP.equalsIgnoreCase(e.getKey())) {
                fppStr = e.getValue();
                break;
            }
        }
        if (fppStr != null && !fppStr.isEmpty()) {
            try {
                double newFpp = Double.parseDouble(fppStr);
                if (Math.abs(newFpp - table.getBfFpp()) > 1e-9) {
                    return null;
                }
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        String bfColsStr = null;
        for (Map.Entry<String, String> e : props.entrySet()) {
            if (PropertyAnalyzer.PROPERTIES_BF_COLUMNS.equalsIgnoreCase(e.getKey())) {
                bfColsStr = e.getValue();
                break;
            }
        }
        // The bf-columns property is required for this classifier (pure fpp
        // changes are not eligible — see above).
        if (bfColsStr == null) {
            return null;
        }
        Set<String> newSet = new HashSet<>();
        if (!bfColsStr.isEmpty()) {
            for (String col : bfColsStr.split(",")) {
                String trimmed = col.trim();
                if (!trimmed.isEmpty()) {
                    newSet.add(trimmed.toLowerCase());
                }
            }
        }
        Set<String> oldSet = new HashSet<>();
        if (table.getBfColumnNames() != null) {
            for (String c : table.getBfColumnNames()) {
                oldSet.add(c.toLowerCase());
            }
        }
        Set<String> added = new HashSet<>(newSet);
        added.removeAll(oldSet);
        Set<String> dropped = new HashSet<>(oldSet);
        dropped.removeAll(newSet);
        if (added.isEmpty() && dropped.isEmpty()) {
            // Redundant no-op alter — let the legacy path raise the usual
            // "no change" error message rather than silently accept here.
            return null;
        }
        if (!added.isEmpty() && !dropped.isEmpty()) {
            // Mixed alter: fall back to legacy so the rewrite sees the whole
            // new set atomically.
            LOG.debug("BF fast-path rejected: mixed add+drop");
            return null;
        }
        return new BloomFilterDelta(
                Collections.unmodifiableSet(added), Collections.unmodifiableSet(dropped));
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
