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
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddRollupClause;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AddRollupOrderByAnalyzerTest {

    private static AddRollupClause makeClause(java.util.List<String> cols, java.util.List<String> sortKeys) {
        return new AddRollupClause("r2", cols, null, sortKeys, null, null,
                com.starrocks.sql.parser.NodePosition.ZERO);
    }

    @Test
    public void testRollupOrderByColumnNotInRollup() {
        SemanticException ex = assertThrows(SemanticException.class, () -> {
            AddRollupClause clause = makeClause(
                    Lists.newArrayList("k1", "k2"),
                    Lists.newArrayList("k3"));
            new AlterTableClauseAnalyzer(null).analyze(null, clause);
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
            new AlterTableClauseAnalyzer(null).analyze(null, clause);
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
        new AlterTableClauseAnalyzer(null).analyze(null, clause);
        // no exception expected
    }

    @Test
    public void testRollupOrderByCaseInsensitive() {
        // sort key matches rollup column case-insensitively → should pass
        AddRollupClause clause = makeClause(
                Lists.newArrayList("K1", "k2"),
                Lists.newArrayList("k1"));
        new AlterTableClauseAnalyzer(null).analyze(null, clause);
    }

    @Test
    public void testRollupOrderByEmpty() {
        // empty sort keys → no validation, no exception
        AddRollupClause clause = makeClause(
                Lists.newArrayList("k1", "k2"),
                Lists.newArrayList());
        new AlterTableClauseAnalyzer(null).analyze(null, clause);
    }
}
