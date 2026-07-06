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

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddRollupClause;
import com.starrocks.type.JsonType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AddRollupOrderByAnalyzerTest {

    private static AddRollupClause makeClause(java.util.List<String> cols, java.util.List<String> sortKeys) {
        return new AddRollupClause("r2", cols, null, sortKeys, null, null,
                com.starrocks.sql.parser.NodePosition.ZERO);
    }

    /**
     * An OlapTable that reports itself as a shared-data range-distribution table, so the ORDER BY-on-
     * ADD-ROLLUP support guard passes through to the subset/dup validation under test.
     */
    private static OlapTable rangeCloudNativeTable() {
        new MockUp<OlapTable>() {
            @Mock
            public boolean isRangeDistribution() {
                return true;
            }

            @Mock
            public boolean isCloudNativeTable() {
                return true;
            }
        };
        return new OlapTable();
    }

    @Test
    public void testRollupOrderByColumnNotInRollup() {
        SemanticException ex = assertThrows(SemanticException.class, () -> {
            AddRollupClause clause = makeClause(
                    Lists.newArrayList("k1", "k2"),
                    Lists.newArrayList("k3"));
            new AlterTableClauseAnalyzer(rangeCloudNativeTable()).analyze(null, clause);
        });
        assertTrue(ex.getMessage().contains("ORDER BY column 'k3' is not in the rollup column list"),
                "expected column-not-in-list message, got: " + ex.getMessage());
    }

    @Test
    public void testRollupOrderByDuplicateColumn() {
        SemanticException ex = assertThrows(SemanticException.class, () -> {
            AddRollupClause clause = makeClause(
                    Lists.newArrayList("k1", "k2"),
                    Lists.newArrayList("k1", "k1"));
            new AlterTableClauseAnalyzer(rangeCloudNativeTable()).analyze(null, clause);
        });
        assertTrue(ex.getMessage().contains("Duplicate ORDER BY column"),
                "expected duplicate-column message, got: " + ex.getMessage());
    }

    @Test
    public void testRollupOrderByValid() {
        // valid: sort keys are a subset (reordered) of rollup columns
        AddRollupClause clause = makeClause(
                Lists.newArrayList("k1", "k2"),
                Lists.newArrayList("k2", "k1"));
        new AlterTableClauseAnalyzer(rangeCloudNativeTable()).analyze(null, clause);
        // no exception expected
    }

    @Test
    public void testRollupOrderByCaseInsensitive() {
        // sort key matches rollup column case-insensitively → should pass
        AddRollupClause clause = makeClause(
                Lists.newArrayList("K1", "k2"),
                Lists.newArrayList("k1"));
        new AlterTableClauseAnalyzer(rangeCloudNativeTable()).analyze(null, clause);
    }

    @Test
    public void testRollupOrderByEmpty() {
        // empty sort keys → no validation, no exception (and the support guard never fires)
        AddRollupClause clause = makeClause(
                Lists.newArrayList("k1", "k2"),
                Lists.newArrayList());
        new AlterTableClauseAnalyzer(null).analyze(null, clause);
    }

    /**
     * ORDER BY on ADD ROLLUP is only supported for shared-data range-distribution tables. A plain
     * HASH-distributed (non-range, non-cloud-native) table must be rejected before the subset/dup
     * validation runs. A default {@code new OlapTable()} is neither range-distributed nor cloud-native,
     * so it stands in for any unsupported table here.
     */
    @Test
    public void testRollupOrderByOnNonRangeTableRejected() {
        SemanticException ex = assertThrows(SemanticException.class, () -> {
            AddRollupClause clause = makeClause(
                    Lists.newArrayList("k1", "k2"),
                    Lists.newArrayList("k2", "k1"));
            new AlterTableClauseAnalyzer(new OlapTable()).analyze(null, clause);
        });
        assertTrue(ex.getMessage().contains("only supported for shared-data range-distribution"),
                "expected range-distribution-only message, got: " + ex.getMessage());
    }

    /**
     * A range rollup ORDER BY column of type JSON must be rejected because JSON is non-sortable
     * (canDistributedBy() == false). This mirrors the check in #74758 for base-table rollup keys,
     * applied here because createRangeRollupJob re-derives key flags from the ORDER BY columns.
     */
    @Test
    public void testRollupOrderByJsonColumnRejected() {
        new MockUp<OlapTable>() {
            @Mock
            public boolean isRangeDistribution() {
                return true;
            }

            @Mock
            public boolean isCloudNativeTable() {
                return true;
            }

            @Mock
            public Column getColumn(String name) {
                if ("jcol".equalsIgnoreCase(name)) {
                    return new Column("jcol", JsonType.JSON);
                }
                return null;
            }
        };
        OlapTable table = new OlapTable();

        SemanticException ex = assertThrows(SemanticException.class, () -> {
            // jcol is in the rollup column list; ORDER BY jcol should fail on non-sortable type
            AddRollupClause clause = makeClause(
                    Lists.newArrayList("jcol", "k1"),
                    Lists.newArrayList("jcol"));
            new AlterTableClauseAnalyzer(table).analyze(null, clause);
        });
        assertTrue(ex.getMessage().contains("non-sortable"),
                "expected non-sortable message, got: " + ex.getMessage());
    }
}
