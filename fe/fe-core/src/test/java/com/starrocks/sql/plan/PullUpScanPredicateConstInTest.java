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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

// PullUpScanPredicateRule rewrites sub-expressions of the pulled-up predicate into scan projection columns.
// The scan projection of a count(*) carries the placeholder `auto_fill_col := TINYINT 1`, so a TINYINT 1
// in a const IN list used to become `IN (0, auto_fill_col)`, which crashes VectorizedInConstPredicate::open()
// on the BE.
public class PullUpScanPredicateConstInTest extends PlanTestBase {

    private static final Pattern COLUMN_IN_IN_LIST = Pattern.compile("IN \\([^)]*\\d+\\s*:");

    private boolean originPredicateReuse;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("CREATE TABLE const_in_t1 (\n" +
                "  k_id INT NOT NULL,\n" +
                "  c_int INT NULL,\n" +
                "  c_bigint BIGINT NULL,\n" +
                "  c_tinyint TINYINT NULL\n" +
                ") DUPLICATE KEY(k_id) DISTRIBUTED BY HASH(k_id) BUCKETS 2 " +
                "PROPERTIES ( \"replication_num\"=\"1\" );");
    }

    @BeforeEach
    public void enableReuse() {
        originPredicateReuse = connectContext.getSessionVariable().isEnablePredicateExprReuse();
        connectContext.getSessionVariable().setEnablePredicateExprReuse(true);
    }

    @AfterEach
    public void restoreReuse() {
        connectContext.getSessionVariable().setEnablePredicateExprReuse(originPredicateReuse);
    }

    private void assertNoColumnInInList(String plan) {
        Assertions.assertFalse(COLUMN_IN_IN_LIST.matcher(plan).find(),
                "IN-list must not contain a column reference:\n" + plan);
    }

    @Test
    public void testOuterInOverDerivedColumn() throws Exception {
        String sql = "SELECT count(*) FROM ( SELECT MOD ( CAST ( c_int AS BOOLEAN ) = c_bigint , 3 ) " +
                "FROM const_in_t1 ) t(c1) WHERE c1 IN (FALSE, TRUE)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "IN (0, 1)");
        assertNoColumnInInList(plan);
    }

    @Test
    public void testNestedInUnderOuterFilter() throws Exception {
        String sql = "SELECT count(*) FROM ( SELECT ( c_bigint > c_int ) >= ( c_tinyint IN ( 1 , 2 , 3 ) ) " +
                "FROM const_in_t1 ) t(c1) WHERE c1 IS NULL";
        assertNoColumnInInList(getFragmentPlan(sql));
    }

    @Test
    public void testInUnderJoinOnWithConstantSide() throws Exception {
        String sql = "SELECT count(*) FROM (SELECT 1 AS j0) tl JOIN const_in_t1 " +
                "ON CAST ( CAST ( k_id AS BOOLEAN ) AS DATETIME ) = ( c_tinyint IN ( '1' , '2' ) )";
        assertNoColumnInInList(getFragmentPlan(sql));
    }
}
