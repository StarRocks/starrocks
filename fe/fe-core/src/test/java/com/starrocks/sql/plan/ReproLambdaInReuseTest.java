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

package com.starrocks.sql.plan;

import com.starrocks.utframe.StarRocksAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

public class ReproLambdaInReuseTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("CREATE TABLE test_table_repro (\n" +
                "  id INT,\n" +
                "  arr_datetime ARRAY<DATETIME>\n" +
                ") DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 " +
                "PROPERTIES ( \"replication_num\"=\"1\" );");
    }

    // PullUpScanPredicateRule (gated by enable_predicate_expr_reuse) rewrites sub-expressions of the
    // pulled-up predicate to reuse scan projection output columns. It must NOT descend into a lambda
    // body: rewriting the constant `CAST(1 AS BIGINT)` inside the array_map lambda's IN-list into a
    // ColumnRef of the outer `SELECT CAST(1 AS BIGINT)` column produced a column ref that BE cannot
    // resolve in the lambda's local chunk, and turned a const IN-predicate into one with a non-constant
    // element, crashing VectorizedInConstPredicate::open() (SIGSEGV).
    @Test
    public void testLambdaInPredicateNotRewrittenToOuterColumnRef() throws Exception {
        String sql = "WITH input AS (\n" +
                "    SELECT\n" +
                "        ARRAY_MAP(arg->(CAST(MONTH(arg) AS BIGINT) IN (1, 2)), arr_datetime) AS arr\n" +
                "    FROM test_table_repro\n" +
                ")\n" +
                "SELECT\n" +
                "    CAST(1 AS BIGINT)\n" +
                "FROM input\n" +
                "WHERE ARRAY_SUM(arr)";

        boolean origin = connectContext.getSessionVariable().isEnablePredicateExprReuse();
        try {
            connectContext.getSessionVariable().setEnablePredicateExprReuse(true);
            String plan = getFragmentPlan(sql);
            // The IN-list must stay a pure constant list inside the lambda, not reference an outer column.
            assertContains(plan, "IN (1, 2)");
            // Reject a column reference (any slot id) appearing as an IN-list element, instead of
            // hard-coding one slot id which can shift across planner changes. A constant list like
            // "IN (1, 2)" has no "<id>:" token; a rewritten element renders as e.g. "IN (5: cast, 2)".
            Assertions.assertFalse(Pattern.compile("IN \\([^)]*\\d+\\s*:").matcher(plan).find(),
                    "IN-list must not contain a column reference:\n" + plan);
        } finally {
            connectContext.getSessionVariable().setEnablePredicateExprReuse(origin);
        }
    }
}