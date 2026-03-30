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

import com.starrocks.common.FeConstants;
import com.starrocks.qe.SessionVariable;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GlobalLateMaterializeNativeTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();

        FeConstants.runningUnitTest = true;
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setEnableGlobalLateMaterialization(true);
        connectContext.getSessionVariable().setEnableGlobalLateMaterializationCostBased(false);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        connectContext.getSessionVariable().setCboCTERuseRatio(0);

        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("""
                CREATE TABLE supplier_nullable ( S_SUPPKEY     INTEGER NOT NULL,
                                             S_NAME        CHAR(25) NOT NULL,
                                             S_ADDRESS     VARCHAR(40),\s
                                             S_NATIONKEY   INTEGER NOT NULL,
                                             S_PHONE       CHAR(15) NOT NULL,
                                             S_ACCTBAL     double NOT NULL,
                                             S_COMMENT     VARCHAR(101) NOT NULL,
                                             PAD char(1) NOT NULL)
                ENGINE=OLAP
                DUPLICATE KEY(`s_suppkey`)
                COMMENT "OLAP"
                DISTRIBUTED BY RANDOM BUCKETS 4
                PROPERTIES (
                "replication_num" = "1",
                "in_memory" = "false"
                );""");

        starRocksAssert.withTable("""
                CREATE TABLE test_array (
                                             test_key    INTEGER NOT NULL,
                                             test_a1     array<int>,\s
                                             PAD char(1) NOT NULL)
                ENGINE=OLAP
                DUPLICATE KEY(`test_key`)
                COMMENT "OLAP"
                DISTRIBUTED BY RANDOM BUCKETS 4
                PROPERTIES (
                "replication_num" = "1",
                "in_memory" = "false"
                );""");
        starRocksAssert.withTable("""
                CREATE TABLE test_struct (
                                             test_key    INTEGER NOT NULL,
                                             test_struct struct<name int, value string>,\s
                                             PAD char(1) NOT NULL)
                ENGINE=OLAP
                DUPLICATE KEY(`test_key`)
                COMMENT "OLAP"
                DISTRIBUTED BY RANDOM BUCKETS 4
                PROPERTIES (
                "replication_num" = "1",
                "in_memory" = "false"
                );""");

        starRocksAssert.withTable("""
                CREATE TABLE test_agg2 (
                                             agg_key    INTEGER NOT NULL,
                                             agg_val    BIGINT SUM DEFAULT "0",
                                             agg_str    VARCHAR(40) REPLACE DEFAULT "")
                ENGINE=OLAP
                AGGREGATE KEY(`agg_key`)
                COMMENT "OLAP"
                DISTRIBUTED BY HASH(`agg_key`) BUCKETS 4
                PROPERTIES (
                "replication_num" = "1",
                "in_memory" = "false"
                );""");

        // Table for ScalarOperatorsReuse + GLM interaction tests.
        // The 3-way self-join with cross-table expressions causes ScalarOperatorsReuseRule to extract
        // common sub-expressions into PhysicalProjectOperator.commonSubOperatorMap, which GLM's
        // tryPushDownFetch must then traverse correctly without NPE.
        starRocksAssert.withTable("""
                CREATE TABLE A (
                    a_pk  INTEGER NOT NULL,
                    a_c0  BIGINT NOT NULL,
                    a_c1  BIGINT NOT NULL,
                    a_c2  BIGINT NOT NULL,
                    a_c3  BIGINT NOT NULL,
                    PAD   VARCHAR(40) NOT NULL)
                ENGINE=OLAP
                DUPLICATE KEY(`a_pk`)
                DISTRIBUTED BY HASH(`a_pk`) BUCKETS 4
                PROPERTIES (
                "replication_num" = "1"
                );""");

        // Range-partitioned table where the partition column (dt) is NOT the distribution column (k1).
        // This causes partition predicates on dt to be moved into prunedPartitionPredicates
        // and removed from the main predicate during partition pruning.
        starRocksAssert.withTable("""
                CREATE TABLE test_range_partitioned (
                    k1      INTEGER NOT NULL,
                    dt      DATE    NOT NULL,
                    name    VARCHAR(40),
                    comment VARCHAR(101),
                    value   BIGINT NOT NULL,
                    PAD     CHAR(1) NOT NULL)
                ENGINE=OLAP
                DUPLICATE KEY(`k1`)
                PARTITION BY RANGE(`dt`)
                (PARTITION p2020 VALUES [('2020-01-01'), ('2021-01-01')),
                 PARTITION p2021 VALUES [('2021-01-01'), ('2022-01-01')))
                DISTRIBUTED BY HASH(`k1`) BUCKETS 4
                PROPERTIES (
                "replication_num" = "1"
                );""");

        starRocksAssert.withTable("""
                CREATE TABLE glm_join_expr_t0 (
                    k0 int(11) NULL,
                    v1 date NULL,
                    v2 datetime NULL,
                    v3 char(20) NULL,
                    v4 varchar(20) NULL,
                    v5 boolean NULL
                ) ENGINE=OLAP
                DUPLICATE KEY(`k0`)
                DISTRIBUTED BY HASH(`k0`) BUCKETS 3
                PROPERTIES ("replication_num" = "1");""");
        starRocksAssert.withTable("""
                CREATE TABLE glm_join_expr_t1 (
                    k0 int(11) NULL,
                    v1 date NULL,
                    v2 datetime NULL,
                    v3 char(20) NULL,
                    v4 varchar(20) NULL,
                    v5 boolean NULL
                ) ENGINE=OLAP
                DUPLICATE KEY(`k0`)
                DISTRIBUTED BY HASH(`k0`) BUCKETS 3
                PROPERTIES ("replication_num" = "1");""");
        starRocksAssert.withTable("""
                CREATE TABLE glm_join_expr_t2 (
                    k0 int(11) NULL,
                    v1 date NULL,
                    v2 datetime NULL,
                    v3 char(20) NULL,
                    v4 varchar(20) NULL,
                    v5 boolean NULL
                ) ENGINE=OLAP
                DUPLICATE KEY(`k0`)
                DISTRIBUTED BY HASH(`k0`) BUCKETS 3
                PROPERTIES ("replication_num" = "1");""");
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableGlobalLateMaterialization(false);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);
    }

    @Test
    public void testLazyMaterialize() throws Exception {
        String sql;
        String plan;
        sql = "select *,upper(S_ADDRESS) from supplier_nullable limit 10";
        plan = getFragmentPlan(sql);
        assertCContains(plan, """
                  6:Decode
                  |  <dict id 10> : <string id 3>
                  |  <dict id 11> : <string id 7>
                  |  <dict id 12> : <string id 9>\
                """);

        sql = "select distinct S_SUPPKEY, S_ADDRESS from ( select S_ADDRESS, S_SUPPKEY " +
                "from supplier_nullable limit 10) t";
        plan = getFragmentPlan(sql);
        assertCContains(plan, "  6:Decode\n" +
                "  |  <dict id 9> : <string id 3>");

        sql = "select * from supplier where S_SUPPKEY < 10 order by 1,2 limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");
    }

    @Test
    public void testProjection() throws Exception {
        String sql;
        String plan;

        sql = "select *,upper(S_ADDRESS) from supplier_nullable limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");
        assertContains(plan, "Decode");
        assertContains(plan, "Project");
    }

    @Test
    public void testArraySubFieldPrune() throws Exception {
        String sql;
        String plan;
        sql = "select array_length(test_a1) from test_array limit 1";
        plan = getCostExplain(sql);
        assertNotContains(plan, "FETCH");

        sql = "select array_length(test_a1),* from test_array limit 1";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  4:FETCH
                  |  lookup node: 03
                  |  table: test_array
                  |    <slot 1> => test_key
                  |    <slot 3> => PAD
                  |  limit: 1\
                """);
    }

    @Test
    public void testStructSubFieldPrune() throws Exception {
        String sql;
        String plan;
        sql = "select test_struct.name from test_struct limit 1";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "FETCH");
    }

    @Test
    public void testLeftJoin() throws Exception {
        String sql;
        String plan;

        sql = "select * from test_struct l join test_array r on l.test_key=r.test_key limit 1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");
    }

    @Test
    public void testWithOffset() throws Exception {
        String sql;
        String plan;

        sql = "select * from test_struct order by 1 limit 100000000,10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");
    }

    @Test
    public void testJson() throws Exception {
        String sql;
        String plan;
        connectContext.getSessionVariable().setEnableDeferProjectAfterTopN(true);
        connectContext.getSessionVariable().setCboPruneJsonSubfield(false);

        sql = "select v_int, get_json_string(v_json, '$.a') from tjson order by v_int limit 1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");
    }

    @Test
    public void testAggregateTable() throws Exception {
        // aggregate key table is not supported by lazy materialize
        String sql = "select * from test_agg2 limit 10";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "FETCH");
    }

    @Test
    public void testWindowFunction() throws Exception {
        String sql;
        String plan;

        // window function without limit - no FETCH
        sql = "select *, row_number() over (partition by S_SUPPKEY) from supplier_nullable";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "FETCH");

        // window function with limit - FETCH
        sql = "select *, row_number() over (partition by S_SUPPKEY) from supplier_nullable limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");
    }

    @Test
    public void testGroupBy() throws Exception {
        String sql;
        String plan;

        // group by without limit - no FETCH (aggregation blocks lazy materialize)
        sql = "select count(*) from supplier_nullable group by S_SUPPKEY";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "FETCH");

        // select distinct with limit from subquery - FETCH
        sql = "select distinct S_SUPPKEY, S_NAME from (select S_SUPPKEY, S_NAME from supplier_nullable limit 10) t";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");
    }

    @Test
    public void testCTE() throws Exception {
        String sql;
        String plan;

        // CTE with limit - FETCH after limit
        sql = "with cte as (select * from supplier_nullable limit 10) " +
                "select * from cte where S_ACCTBAL > 0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");

        // CTE without limit - no FETCH
        sql = "with cte as (select * from supplier_nullable) " +
                "select * from cte where S_ACCTBAL > 0";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "FETCH");

        // CTE with join - FETCH at top
        sql = "with cte as (select * from supplier_nullable limit 10) " +
                "select * from cte l join cte r on l.S_SUPPKEY = r.S_SUPPKEY limit 5";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");
    }

    // Tests that GLM correctly handles the combination of CTE (common expression reuse),
    // OlapScan, HashJoin, and Project (select) operators together.
    //
    // This covers two bug fixes in GlobalLateMaterializationRewriter:
    //   1. tryPushDownFetch's PhysicalProjectOperator loop: a column in `columns` may not
    //      exist in the projection map (e.g. pruned by column-prune after CTE inlining),
    //      so a null check is required before calling isColumnRef().
    //   2. FetchMerger.visit() dependency check: for a multi-input operator (join / CTE anchor),
    //      the dependency must be looked up using the CHILD operator's IdentifyOperator (cIdx),
    //      not the parent's, to correctly determine which join side holds the scan.
    @Test
    public void testCTEScanJoinSelect() throws Exception {
        String sql;
        String plan;

        // Case 1: CTE → scan → join → select-projection
        // The CTE result is joined to another table and specific columns are projected.
        // GLM must push FETCH through the join to the correct (CTE) side and handle the
        // select-projection's column map without NPE when a column is missing from the map.
        sql = "with top_s as (select S_SUPPKEY, S_NAME, S_ADDRESS, PAD " +
                "from supplier_nullable order by S_SUPPKEY limit 10) " +
                "select top_s.S_NAME, top_s.PAD, a.test_key " +
                "from top_s join test_array a on top_s.S_SUPPKEY = a.test_key";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");

        // Case 2: scan → join → select with common sub-expression in projection
        // Both sides of the join contribute to the projected output; GLM must correctly
        // identify which scan's columns are deferred and push FETCH to that side only.
        sql = "select sn.S_NAME, sn.PAD, ta.test_key " +
                "from supplier_nullable sn join test_array ta on sn.S_SUPPKEY = ta.test_key " +
                "order by sn.S_SUPPKEY limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");

        // Case 3: CTE used twice in a self-join with explicit select-projection
        // The same CTE scan is referenced by both sides of the join; GLM must handle
        // the duplicated common expression without pushing FETCH to the wrong side.
        sql = "with cte as (select S_SUPPKEY, S_NAME, S_ADDRESS, S_ACCTBAL, PAD " +
                "from supplier_nullable order by S_SUPPKEY limit 10) " +
                "select l.S_NAME, r.S_ADDRESS " +
                "from cte l join cte r on l.S_SUPPKEY = r.S_SUPPKEY limit 5";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");
    }

    // Tests that GLM correctly interacts with ScalarOperatorsReuseRule (the "reuse rule").
    // ScalarOperatorsReuseRule extracts common sub-expressions from projections into
    // PhysicalProjectOperator.commonSubOperatorMap.  In a 3-way self-join with cross-table
    // arithmetic expressions, the same column ref (e.g. A0.a_c1) appears in multiple
    // projection outputs, triggering CSE extraction.
    //
    // GLM's tryPushDownFetch must then:
    //   (a) follow the commonSubOperatorMap indirection to find the real source column, and
    //   (b) handle the case where a column in `columns` has no entry in the projection map
    //       (null scalarOperator — the pruned-column fix).
    @Test
    public void testScalarOperatorsReuseWithGLM2() throws Exception {
        String sql;
        String plan;

        sql = "select * from (" +
                "select A0.a_pk as a0_pk, A1.a_pk as a1_pk, A2.a_pk as a2_pk, " +
                "murmur_hash3_32(A0.a_c0+A0.a_c1) as a0_c0, " +
                "murmur_hash3_32(A1.a_c1+A1.a_c2) as a1_c1, " +
                "murmur_hash3_32(A2.a_c2+A2.a_c3) as a2_c2, " +
                "murmur_hash3_32(A0.a_c1+A1.a_c2+A2.a_c2) as a0_c1 " +
                "from A A0, A A1, A A2 " +
                "where A0.a_pk = A1.a_pk and A1.a_pk = A2.a_pk" +
                ") t order by a0_pk limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");

        sql = "select * from (" +
                "select A0.a_pk as a0_pk, A1.a_pk as a1_pk, A2.a_pk as a2_pk, " +
                "a0_c0, a1_c1, a2_c2, murmur_hash3_32(A0.a_c0+A1.a_c1+A2.a_c2) as a0_c1 " +
                "from " +
                "(select a_pk, a_c0, murmur_hash3_32(a_c0+a_c0) as a0_c0 from A) A0, " +
                "(select a_pk, a_c1, murmur_hash3_32(a_c1+a_c2) as a1_c1 from A) A1, " +
                "(select a_pk, a_c2, murmur_hash3_32(a_c2+a_c3) as a2_c2 from A) A2 " +
                "where A0.a_pk = A1.a_pk and A1.a_pk = A2.a_pk" +
                ") t order by a0_pk limit 10";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "FETCH");
    }

    // -------------------------------------------------------------------------
    // Bug ① – ClassCastException in tryPushDownFetch when a projection's
    //          commonSubOperatorMap value is a compound ScalarOperator.
    //
    // Root cause (GlobalLateMaterializationRewriter.java ~line 662):
    //   if (common.containsKey(origin)) {
    //       origin = (ColumnRefOperator) common.get(origin);  // unsafe cast
    //   }
    // After ScalarOperatorsReuseRule, the VALUE stored in commonSubOperatorMap
    // for a CSE key is the *original compound expression* (e.g. "a_c0 + a_c1"),
    // which is a BinaryPredicateOperator — NOT a ColumnRefOperator.  The direct
    // cast throws ClassCastException.
    //
    // Trigger pattern: the SAME compound expression appears in two or more
    // SELECT outputs → ScalarOperatorsReuseRule extracts it as a CSE with a
    // compound value.  GLM then tries to push FETCH through the projection and
    // hits the cast.
    //
    // Fix: replace the blind cast with an instanceof check + getUsedColumns()
    // for non-ColumnRef CSE values.
    // -------------------------------------------------------------------------
    @Test
    public void testBug1CseCompoundValueClassCast() throws Exception {
        // The expression `a_c0 + a_c1` appears TWICE in the SELECT list.
        // ScalarOperatorsReuseRule extracts it into commonSubOperatorMap as
        //   { cseRef  →  BinaryPredicateOperator(a_c0 + a_c1) }
        // and rewrites the columnRefMap to
        //   { sum1_out → cseRef, sum2_out → cseRef, a_pk_out → a_pk, PAD_out → PAD }
        //
        // GLM: a_pk is the sort key (early-materialized); PAD is deferred to FETCH.
        // When FetchMerger pushes the FETCH requirement through the top-level
        // projection, tryPushDownFetch encounters the above projection:
        //   project.get(PAD_out) = PAD  → simple pass-through (no CSE involvement)
        //
        // Note: in the current plan shape the FETCH push-down path happens to
        // avoid the CSE entry because PAD is the only deferred column and it
        // maps through a direct column-ref value.  The test therefore validates
        // the *neighbouring* code path and ensures no exception is thrown even
        // when a compound CSE is present in the projection.
        String plan = getFragmentPlan(
                "select a_pk, a_c0 + a_c1 as sum1, a_c0 + a_c1 as sum2, PAD " +
                "from A " +
                "order by a_pk limit 10");
        assertContains(plan, "FETCH");

        // Nested subquery: the inner projection has the duplicate compound
        // expression; the outer SELECT * wraps it.  GLM must push FETCH
        // through two stacked projections without tripping on the compound CSE.
        plan = getFragmentPlan(
                "select * from (" +
                "  select a_pk, a_c0+a_c1 as s1, a_c0+a_c1 as s2, PAD " +
                "  from A" +
                ") t " +
                "order by a_pk limit 10");
        assertContains(plan, "FETCH");
    }

    // -------------------------------------------------------------------------
    // Bug ② – `addProjection(null, col)` called before the null guard in
    //          Rewriter.visitPhysicalProject (line ~1133).
    //
    // Root cause:
    //   ColumnRefOperator after = getColumnRefAfterProjection(...);
    //   context.resolver.addProjection(after, col);   // ← BUG: after can be null
    //   if (after == null) { continue; }
    //
    // `getColumnRefAfterProjection` returns null ("column pruned") when the
    // column `col` tracked as un-materialized (to be FETCH-ed) does not appear
    // as a VALUE in the current projection's columnRefMap — i.e., the column is
    // projected *out* by a narrower outer SELECT.
    //
    // Concrete trigger:
    //   CTE body defers S_NAME, S_ADDRESS, PAD (only S_SUPPKEY needed for sort).
    //   The outer SELECT only requests S_NAME and PAD — **pruning S_ADDRESS**.
    //   In visitPhysicalProject for the outer projection, the Rewriter iterates
    //   over `value.columns` = {S_NAME_c, S_ADDRESS_c, PAD_c} and calls
    //   getColumnRefAfterProjection(S_ADDRESS_c, ...).  Because S_ADDRESS_c is
    //   not a value in the outer projection, it returns null.
    //   The bug then calls addProjection(null, S_ADDRESS_c), inserting a null
    //   key into the AliasResolver's `resolved` HashMap.
    //
    // Fix: swap the order — null-check first, addProjection only when non-null.
    // -------------------------------------------------------------------------
    @Test
    public void testBug2NullAddProjectionBeforeNullCheck() throws Exception {
        // S_ADDRESS is deferred to FETCH (not needed for ORDER BY S_SUPPKEY),
        // but the outer SELECT prunes it.  Without the fix, addProjection(null, ...)
        // is called, corrupting the AliasResolver's resolved map.
        // With the fix, the null return is detected first and the column is skipped.
        String sql =
                "with top_s as (" +
                "  select S_SUPPKEY, S_NAME, S_ADDRESS, PAD " +
                "  from supplier_nullable " +
                "  order by S_SUPPKEY limit 10" +
                ") " +
                "select top_s.S_NAME, top_s.PAD, a.test_key " +      // S_ADDRESS pruned!
                "from top_s join test_array a on top_s.S_SUPPKEY = a.test_key";
        String plan = getFragmentPlan(sql);
        // FETCH must still be inserted for S_NAME and PAD even though S_ADDRESS
        // was pruned by the outer projection.
        assertContains(plan, "FETCH");
        // S_ADDRESS must NOT appear in the FETCH column list: it was pruned and
        // the resolver should have skipped it cleanly rather than crashing.
        assertNotContains(plan, "=> S_ADDRESS");

        // Same pattern, slightly wider outer select — also prunes S_ACCTBAL.
        sql = "with top_s as (" +
                "  select S_SUPPKEY, S_NAME, S_ADDRESS, S_ACCTBAL, PAD " +
                "  from supplier_nullable " +
                "  order by S_SUPPKEY limit 5" +
                ") " +
                "select top_s.S_NAME " +       // prunes S_ADDRESS, S_ACCTBAL, PAD
                "from top_s";
        plan = getFragmentPlan(sql);
        // S_NAME must be fetched; the other three columns are pruned and must
        // not cause a null-key corruption in the resolver.
        assertContains(plan, "FETCH");
        assertNotContains(plan, "=> S_ADDRESS");
        assertNotContains(plan, "=> S_ACCTBAL");
    }

    // -------------------------------------------------------------------------
    // Bug ③ – `dependency.get(cIdx)` NPE (ALREADY FIXED in the current code).
    //
    // Root cause (original code, line ~725):
    //   if (!context.collectorContext.dependency.get(cIdx).contains(scanId))
    //
    // If `cIdx` (the child operator) is not present in the dependency map
    // (possible if a new operator type forgets to register itself via
    // visitChildren), the `.get()` returns null and `.contains()` NPEs.
    //
    // Fix already applied:
    //   final Set<IdentifyOperator> dep = dependency.get(cIdx);
    //   if (dep == null || !dep.contains(scanId)) { continue; }
    //
    // This test exercises the join code path that performs the dependency
    // lookup, verifying that:
    //   (a) the plan is generated without NPE, and
    //   (b) FETCH is correctly pushed to the right join child.
    // -------------------------------------------------------------------------
    @Test
    public void testBug3DependencyGetNullGuard() throws Exception {
        // Two-way join: FetchMerger iterates both children and calls
        // dependency.get(cIdx) for each.  Verifies no NPE even for operators
        // that register themselves with an empty dependency set (e.g. leaf scans).
        String plan = getFragmentPlan(
                "select sn.S_NAME, sn.PAD, ta.test_key " +
                "from supplier_nullable sn " +
                "join test_array ta on sn.S_SUPPKEY = ta.test_key " +
                "order by sn.S_SUPPKEY limit 10");
        assertContains(plan, "FETCH");

        // Three-way self-join: FetchMerger iterates three children; each child's
        // dependency lookup must handle both populated and empty dependency sets.
        plan = getFragmentPlan(
                "select a0.* " +
                "from A a0 " +
                "join A a1 on a0.a_pk = a1.a_pk " +
                "join A a2 on a1.a_pk = a2.a_pk " +
                "order by a0.a_pk limit 5");
        assertContains(plan, "FETCH");
    }

    // Copied from TablePruningTest.testExplainLogicalCloneOperator to verify that
    // table-pruning's CLONE operator is handled correctly when GLM is also enabled.
    @Test
    public void testExplainLogicalCloneOperator() throws Exception {
        String tabAA = """
                CREATE TABLE `AA` (
                    `id` int(11) NOT NULL,
                    `b_id` int(11) NOT NULL,
                    `name` varchar(25) NOT NULL
                    ) ENGINE=OLAP
                DUPLICATE KEY(`id`)
                DISTRIBUTED BY HASH(`id`) BUCKETS 10 PROPERTIES ("replication_num" = "1");
                """;
        String tabBB = """
                CREATE TABLE `BB` (
                      `id` int(11) NOT NULL,
                      `name` varchar(25) NOT NULL,
                      `age` varchar(25)
                      ) ENGINE=OLAP
                UNIQUE KEY(`id`)
                DISTRIBUTED BY HASH(`id`) BUCKETS 10  PROPERTIES ("replication_num" = "1");""";
        starRocksAssert.withTable(tabAA);
        starRocksAssert.withTable(tabBB);
        try {
            starRocksAssert.alterTableProperties(
                    "alter table AA set(\"foreign_key_constraints\" = \"(b_id) REFERENCES BB(id)\");");
            String sql = "select AA.b_id, BB.id from AA inner join BB on AA.b_id = BB.id";
            connectContext.getSessionVariable().setEnableCboTablePrune(true);
            connectContext.getSessionVariable().setEnableGlobalLateMaterialization(false);
            String plan = UtFrameUtils.explainLogicalPlan(connectContext, sql);
            assertContains(plan, "CLONE");
        } finally {
            connectContext.getSessionVariable().setEnableCboTablePrune(false);
            connectContext.getSessionVariable().setEnableGlobalLateMaterialization(true);
            starRocksAssert.dropTable("AA");
            starRocksAssert.dropTable("BB");
        }
    }

    // -------------------------------------------------------------------------
    // Table function (UNNEST / lateral join) GLM interaction.
    //
    // PhysicalTableFunctionOperator passes through the outer column refs
    // (e.g. PAD) without consuming them — they remain un-materialised through
    // the table function and can be deferred to a FETCH node inserted above the
    // TopN, just like in the plain scan+sort case.
    //
    // The array column fed to the function (test_a1) MUST be early-materialised
    // because it is required by the function itself.
    //
    // The Rewriter must strip deferred columns from outerColRefs and thread the
    // row-locator columns through instead, so the FETCH node above can use them.
    // -------------------------------------------------------------------------
    @Test
    public void testTableFunction() throws Exception {
        String sql;
        String plan;

        // UNNEST with ORDER BY + LIMIT:
        //   - test_key must be early-materialised (ORDER BY key)
        //   - test_a1 must be early-materialised (UNNEST function input)
        //   - PAD is a pass-through outer column → deferred to FETCH
        sql = "select test_key, f.val, PAD " +
                "from test_array, unnest(test_a1) AS f(val) " +
                "order by test_key limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");
        // PAD must appear in the FETCH column list
        assertContains(plan, "=> PAD");
        // test_a1 is consumed by UNNEST and must NOT be deferred to FETCH
        assertNotContains(plan, "=> test_a1");

        // UNNEST without a LIMIT: GLM should not apply (no TopN to trigger it)
        sql = "select test_key, f.val, PAD " +
                "from test_array, unnest(test_a1) AS f(val)";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "FETCH");
    }

    @Test
    public void testPrunedPartitionPredicatesHandledCorrectly() throws Exception {
        String sql = "select * from test_range_partitioned " +
                "where dt >= '2020-01-01' and dt < '2021-01-01' " +
                "order by k1 limit 10";
        String plan = getFragmentPlan(sql);

        // GLM should still be applied: non-predicate, non-sort-key columns are deferred
        assertContains(plan, "FETCH");

        // dt is referenced in prunedPartitionPredicates, so it must be materialized before FETCH.
        // It must NOT appear in the FETCH lookup column list.
        assertNotContains(plan, "=> dt");
    }

    public void testWithVirtualColumn() throws Exception {
        String sql;
        String plan;
        sql = "select _row_id_,* from test_struct order by 1 limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");
    }

    @Test
    public void testSessionVariableControl() throws Exception {
        final SessionVariable sv = connectContext.getSessionVariable();
        final int prevMaxLimit = sv.getGlobalLateMaterializeMaxLimit();
        final int prevMaxFetchOps = sv.getGlobalLateMaterializeMaxFetchOps();

        try {
            // set a low limit threshold
            sv.setGlobalLateMaterializeMaxLimit(5);

            String sql;
            String plan;

            // limit exceeds max - no FETCH
            sql = "select * from supplier_nullable order by S_SUPPKEY limit 10";
            plan = getFragmentPlan(sql);
            assertNotContains(plan, "FETCH");

            // limit within max - FETCH
            sql = "select * from supplier_nullable order by S_SUPPKEY limit 4";
            plan = getFragmentPlan(sql);
            assertContains(plan, "FETCH");
        } finally {
            sv.setGlobalLateMaterializeMaxLimit(prevMaxLimit);
            sv.setGlobalLateMaterializeMaxFetchOps(prevMaxFetchOps);
        }
    }

    @Test
    public void testLambdaConjuncts() throws Exception {
        // connectContext.getSessionVariable().setEnableGlobalLateMaterialization(false);
        String table = """
                create table tlambda (a int,b map<int,varchar(20)>) ENGINE=OLAP\s
                DUPLICATE KEY(a)  DISTRIBUTED BY HASH(a)\s
                BUCKETS 3\s
                PROPERTIES("replication_num" = "1");
                """;
        starRocksAssert.withTable(table);
        String sql = "select a from tlambda where map_filter((k,v)->v is not null and k is not null, " +
                "map_apply((k,v)->(k+1,concat(v,'a')), b))=map{2:\"aba\",4:\"cdda\"} order by 1 limit 1;\n";
        final String plan = getFragmentPlan(sql);
        System.out.println("plan = " + plan);
    }

    @Test
    public void testCostBasedGlmKeepsDerivedJoinKeyProjection() throws Exception {
        final SessionVariable sv = connectContext.getSessionVariable();
        try {
            sv.setEnableGlobalLateMaterializationCostBased(true);
            String plan = getFragmentPlan(
                    "select * from glm_join_expr_t0 t0 " +
                            "join glm_join_expr_t1 t1 " +
                            "on t0.v1 = t1.v1 " +
                            "and t0.v2 = (exists (select max(v2) from glm_join_expr_t2)) " +
                            "order by 1");
            assertContains(plan, "1:Project\n" +
                    "  |  <slot 1> : 1: k0\n" +
                    "  |  <slot 2> : 2: v1\n" +
                    "  |  <slot 3> : 3: v2\n" +
                    "  |  <slot 4> : 4: v3\n" +
                    "  |  <slot 6> : 6: v5\n" +
                    "  |  <slot 23> : CAST(3: v2 AS DOUBLE)\n" +
                    "  |  <slot 27> : 27: v4");
        } finally {
            sv.setEnableGlobalLateMaterializationCostBased(false);
        }
    }

    @Test
    public void testCostBasedGlm() throws Exception {
        final SessionVariable sv = connectContext.getSessionVariable();
        try {
            sv.setEnableGlobalLateMaterializationCostBased(true);

            // All deferred columns are numeric (S_NATIONKEY INTEGER, S_ACCTBAL DOUBLE):
            // their total type-size is well below the 24-byte row-id overhead, so
            // cost-based GLM should suppress the rewrite → no FETCH node.
            String plan = getFragmentPlan(
                    "select S_SUPPKEY, S_NATIONKEY, S_ACCTBAL " +
                    "from supplier_nullable order by S_SUPPKEY limit 10");
            assertNotContains(plan, "FETCH");

            // At least one deferred column is non-numeric (S_COMMENT VARCHAR):
            // cost-based GLM should allow the rewrite → FETCH node present.
            plan = getFragmentPlan(
                    "select S_SUPPKEY, S_COMMENT " +
                    "from supplier_nullable order by S_SUPPKEY limit 10");
            assertContains(plan, "FETCH");

            // DATE and BIGINT are both treated as numeric, so deferring only such
            // columns should also suppress GLM.
            plan = getFragmentPlan(
                    "select k1, value, dt " +
                    "from test_range_partitioned order by k1 limit 10");
            assertNotContains(plan, "FETCH");

            // VARCHAR columns present → GLM applies even on test_range_partitioned.
            plan = getFragmentPlan(
                    "select k1, name " +
                    "from test_range_partitioned order by k1 limit 10");
            assertContains(plan, "FETCH");

            // Exactly 2 deferred BIGINT columns: numericDeferredCount == 2 (≤ 2) → GLM skipped.
            plan = getFragmentPlan(
                    "select a_pk, a_c0, a_c1 from A order by a_pk limit 10");
            assertNotContains(plan, "FETCH");

            // 3 deferred BIGINT columns: numericDeferredCount == 3 (> 2) → GLM applied.
            plan = getFragmentPlan(
                    "select a_pk, a_c0, a_c1, a_c2 from A order by a_pk limit 10");
            assertContains(plan, "FETCH");

        } finally {
            sv.setEnableGlobalLateMaterializationCostBased(false);
        }
    }
}
