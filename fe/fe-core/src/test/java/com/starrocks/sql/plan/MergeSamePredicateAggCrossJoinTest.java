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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MergeSamePredicateAggCrossJoinTest extends TPCDSPlanTestBase {

    @BeforeAll
    public static void createRuleTestTables() throws Exception {
        starRocksAssert.withTable("CREATE TABLE mspa_pk (pk bigint NOT NULL, v1 int, v2 bigint)"
                + " PRIMARY KEY(pk) DISTRIBUTED BY HASH(pk) BUCKETS 3"
                + " PROPERTIES('replication_num' = '1')");
        starRocksAssert.withTable("CREATE TABLE mspa_part (dt date NOT NULL, k int, v bigint)"
                + " DUPLICATE KEY(dt) PARTITION BY RANGE(dt) ("
                + " PARTITION p1 VALUES [('2024-01-01'), ('2024-02-01')),"
                + " PARTITION p2 VALUES [('2024-02-01'), ('2024-03-01')))"
                + " DISTRIBUTED BY HASH(k) BUCKETS 3 PROPERTIES('replication_num' = '1')");
    }

    @BeforeEach
    public void setUp() {
        connectContext.getSessionVariable().setEnableMergeSamePredicateAggCrossJoin(true);
    }

    @AfterEach
    public void tearDown() {
        connectContext.getSessionVariable().setEnableMergeSamePredicateAggCrossJoin(true);
    }

    private static int countOccurrences(String text, String pattern) {
        int count = 0;
        int index = text.indexOf(pattern);
        while (index >= 0) {
            count++;
            index = text.indexOf(pattern, index + pattern.length());
        }
        return count;
    }

    private int scanCount(String sql, String table) throws Exception {
        String plan = getFragmentPlan(sql);
        return countOccurrences(plan, "TABLE: " + table);
    }

    // ------------------------------------------------------------------------------------------------
    // TPC-DS q09: 15 uncorrelated scalar subqueries carrying only 5 distinct predicates
    // ------------------------------------------------------------------------------------------------

    @Test
    public void testTPCDSQ09MergesTo5Scans() throws Exception {
        Assertions.assertEquals(5, scanCount(Q09, "store_sales"));
    }

    @Test
    public void testTPCDSQ09NotMergedWhenDisabled() throws Exception {
        connectContext.getSessionVariable().setEnableMergeSamePredicateAggCrossJoin(false);
        Assertions.assertEquals(15, scanCount(Q09, "store_sales"));
    }

    @Test
    public void testTPCDSQ09ResultShapeUnchanged() throws Exception {
        // the five CASE WHEN outputs must survive the rewrite
        String plan = getFragmentPlan(Q09);
        Assertions.assertEquals(5, countOccurrences(plan, "if("));
    }

    // ------------------------------------------------------------------------------------------------
    // positive cases
    // ------------------------------------------------------------------------------------------------

    @Test
    public void testTwoScalarSubqueriesSamePredicate() throws Exception {
        String sql = "select (select count(*) from t0 where v1 = 1), (select sum(v2) from t0 where v1 = 1)";
        Assertions.assertEquals(1, scanCount(sql, "t0"));
    }

    @Test
    public void testThreeScalarSubqueriesSamePredicate() throws Exception {
        String sql = "select (select count(*) from t0 where v1 = 1),"
                + " (select sum(v2) from t0 where v1 = 1),"
                + " (select max(v3) from t0 where v1 = 1)";
        Assertions.assertEquals(1, scanCount(sql, "t0"));
    }

    @Test
    public void testTwoGroupsMergeIndependently() throws Exception {
        String sql = "select (select count(*) from t0 where v1 = 1), (select sum(v2) from t0 where v1 = 1),"
                + " (select count(*) from t0 where v1 = 2), (select sum(v2) from t0 where v1 = 2)";
        Assertions.assertEquals(2, scanCount(sql, "t0"));
    }

    @Test
    public void testDerivedTablesSamePredicate() throws Exception {
        // same physical shape as the scalar-subquery form, reached through a different syntax
        String sql = "select * from (select count(*) c from t0 where v1 = 1) a,"
                + " (select sum(v2) s from t0 where v1 = 1) b";
        Assertions.assertEquals(1, scanCount(sql, "t0"));
    }

    @Test
    public void testMergeWithOuterDrivingTable() throws Exception {
        String sql = "select t1.v4, (select count(*) from t0 where v1 = 1), (select sum(v2) from t0 where v1 = 1)"
                + " from t1";
        Assertions.assertEquals(1, scanCount(sql, "t0"));
        Assertions.assertEquals(1, scanCount(sql, "t1"));
    }

    @Test
    public void testNoPredicateBranchesStillMerge() throws Exception {
        String sql = "select (select count(*) from t0), (select sum(v2) from t0)";
        Assertions.assertEquals(1, scanCount(sql, "t0"));
    }

    // ------------------------------------------------------------------------------------------------
    // negative cases: the rule must leave these alone
    // ------------------------------------------------------------------------------------------------

    @Test
    public void testDifferentPredicatesNotMerged() throws Exception {
        String sql = "select (select count(*) from t0 where v1 = 1), (select sum(v2) from t0 where v1 = 2)";
        Assertions.assertEquals(2, scanCount(sql, "t0"));
    }

    @Test
    public void testDifferentTablesNotMerged() throws Exception {
        String sql = "select (select count(*) from t0 where v1 = 1), (select sum(v5) from t1 where v4 = 1)";
        Assertions.assertEquals(1, scanCount(sql, "t0"));
        Assertions.assertEquals(1, scanCount(sql, "t1"));
    }

    @Test
    public void testGroupByBranchNotMerged() throws Exception {
        // a grouping aggregation is not a scalar aggregation; it also gets an ASSERT LE 1 guard
        String sql = "select (select count(*) from t0 where v1 = 1),"
                + " (select sum(v2) from t0 where v1 = 1 group by v3)";
        Assertions.assertEquals(2, scanCount(sql, "t0"));
    }

    @Test
    public void testBranchWithJoinNotMerged() throws Exception {
        String sql = "select (select count(*) from t0 where v1 = 1),"
                + " (select sum(v2) from t0 join t1 on v1 = v4 where v1 = 1)";
        Assertions.assertEquals(2, scanCount(sql, "t0"));
    }

    @Test
    public void testBranchWithLimitNotMerged() throws Exception {
        String sql = "select (select count(*) from t0 where v1 = 1),"
                + " (select sum(v2) from (select v2 from t0 where v1 = 1 limit 10) x)";
        Assertions.assertEquals(2, scanCount(sql, "t0"));
    }

    @Test
    public void testTwoDistinctColumnsNotMerged() throws Exception {
        // merging would hand a multi-distinct aggregation to RewriteMultiDistinctRule; stay out of it
        String sql = "select (select count(distinct v2) from t0 where v1 = 1),"
                + " (select count(distinct v3) from t0 where v1 = 1)";
        Assertions.assertEquals(2, scanCount(sql, "t0"));
    }

    @Test
    public void testSingleDistinctColumnMerged() throws Exception {
        String sql = "select (select count(distinct v2) from t0 where v1 = 1),"
                + " (select sum(v3) from t0 where v1 = 1)";
        Assertions.assertEquals(1, scanCount(sql, "t0"));
    }

    @Test
    public void testNonDeterministicPredicateNotMerged() throws Exception {
        String sql = "select (select count(*) from t0 where v1 = rand()),"
                + " (select sum(v2) from t0 where v1 = rand())";
        Assertions.assertEquals(2, scanCount(sql, "t0"));
    }

    // ------------------------------------------------------------------------------------------------
    // per-relation table hints and access-path options
    // ------------------------------------------------------------------------------------------------

    @Test
    public void testDisagreeingPkIndexHintNotMerged() throws Exception {
        // [_USE_PK_INDEX_] is a user-written hint that TableRelation.hasTableHints() does not cover; merging
        // would silently drop it from one branch or impose it on the other
        String sql = "select (select count(*) from mspa_pk [_USE_PK_INDEX_] where pk = 1),"
                + " (select sum(v2) from mspa_pk where pk = 1)";
        Assertions.assertEquals(2, scanCount(sql, "mspa_pk"));
    }

    @Test
    public void testAgreeingPkIndexHintMerged() throws Exception {
        String sql = "select (select count(*) from mspa_pk [_USE_PK_INDEX_] where pk = 1),"
                + " (select sum(v2) from mspa_pk [_USE_PK_INDEX_] where pk = 1)";
        Assertions.assertEquals(1, scanCount(sql, "mspa_pk"));
    }

    @Test
    public void testSampledScanNotMerged() throws Exception {
        // every TableSampleClause carries its own ThreadLocalRandom seed, so two SAMPLE relations draw
        // independent subsets and must not be folded into one
        String sql = "select (select count(*) from mspa_pk sample('percent'='10') where pk = 1),"
                + " (select sum(v2) from mspa_pk where pk = 1)";
        Assertions.assertEquals(2, scanCount(sql, "mspa_pk"));
    }

    @Test
    public void testExplicitPartitionNotMerged() throws Exception {
        String sql = "select (select count(*) from mspa_part partition(p1) where k = 1),"
                + " (select sum(v) from mspa_part where k = 1)";
        Assertions.assertEquals(2, scanCount(sql, "mspa_part"));
    }
}
