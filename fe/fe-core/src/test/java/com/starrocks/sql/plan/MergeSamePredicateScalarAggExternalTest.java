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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * External-table coverage for {@link com.starrocks.sql.optimizer.rule.transformation
 * .MergeSamePredicateScalarAggRule}. Iceberg is the shape the rule was written for (the TPC-DS benchmark runs on
 * Iceberg external tables), and external catalogs hand out different Table/Column instances per scan, so the
 * branch matching cannot rely on object identity.
 */
public class MergeSamePredicateScalarAggExternalTest extends ConnectorPlanTestBase {
    private static final String TBL = "iceberg0.unpartitioned_db.t_numeric";

    @BeforeEach
    public void setUp() {
        connectContext.getSessionVariable().setEnableMergeSamePredicateScalarAgg(true);
    }

    @AfterEach
    public void tearDown() {
        connectContext.getSessionVariable().setEnableMergeSamePredicateScalarAgg(true);
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

    private int scanCount(String sql) throws Exception {
        return countOccurrences(getFragmentPlan(sql), "IcebergScanNode");
    }

    @Test
    public void testIcebergSamePredicateMerged() throws Exception {
        String sql = "select (select count(*) from " + TBL + " where c1 = 1),"
                + " (select sum(c2) from " + TBL + " where c1 = 1)";
        Assertions.assertEquals(1, scanCount(sql));
    }

    @Test
    public void testIcebergSamePredicateNotMergedWhenDisabled() throws Exception {
        connectContext.getSessionVariable().setEnableMergeSamePredicateScalarAgg(false);
        String sql = "select (select count(*) from " + TBL + " where c1 = 1),"
                + " (select sum(c2) from " + TBL + " where c1 = 1)";
        Assertions.assertEquals(2, scanCount(sql));
    }

    @Test
    public void testIcebergThreeBranchesMerged() throws Exception {
        String sql = "select (select count(*) from " + TBL + " where c1 = 1),"
                + " (select sum(c2) from " + TBL + " where c1 = 1),"
                + " (select max(id) from " + TBL + " where c1 = 1)";
        Assertions.assertEquals(1, scanCount(sql));
    }

    @Test
    public void testIcebergDifferentPredicatesNotMerged() throws Exception {
        String sql = "select (select count(*) from " + TBL + " where c1 = 1),"
                + " (select sum(c2) from " + TBL + " where c1 = 2)";
        Assertions.assertEquals(2, scanCount(sql));
    }

    @Test
    public void testIcebergTwoGroupsMergeIndependently() throws Exception {
        String sql = "select (select count(*) from " + TBL + " where c1 = 1),"
                + " (select sum(c2) from " + TBL + " where c1 = 1),"
                + " (select count(*) from " + TBL + " where c1 = 2),"
                + " (select sum(c2) from " + TBL + " where c1 = 2)";
        Assertions.assertEquals(2, scanCount(sql));
    }

    @Test
    public void testIcebergDifferentTablesNotMerged() throws Exception {
        String sql = "select (select count(*) from " + TBL + " where c1 = 1),"
                + " (select count(*) from iceberg0.unpartitioned_db.t0 where id = 1)";
        Assertions.assertEquals(2, scanCount(sql));
    }

    // ------------------------------------------------------------------------------------------------
    // Hive: the rule is gated on a generic "unspecialized scan" invariant rather than a per-connector allow
    // list, so every lake format that carries no state beyond ScanOperatorPredicates is covered.
    // ------------------------------------------------------------------------------------------------

    private static final String HIVE_TBL = "hive0.tpch.lineitem";

    private int hiveScanCount(String sql) throws Exception {
        return countOccurrences(getFragmentPlan(sql), "HdfsScanNode");
    }

    @Test
    public void testHiveSamePredicateMerged() throws Exception {
        String sql = "select (select count(*) from " + HIVE_TBL + " where l_partkey = 1),"
                + " (select sum(l_quantity) from " + HIVE_TBL + " where l_partkey = 1)";
        Assertions.assertEquals(1, hiveScanCount(sql));
    }

    @Test
    public void testHiveSamePredicateNotMergedWhenDisabled() throws Exception {
        connectContext.getSessionVariable().setEnableMergeSamePredicateScalarAgg(false);
        String sql = "select (select count(*) from " + HIVE_TBL + " where l_partkey = 1),"
                + " (select sum(l_quantity) from " + HIVE_TBL + " where l_partkey = 1)";
        Assertions.assertEquals(2, hiveScanCount(sql));
    }

    @Test
    public void testHiveDifferentPredicatesNotMerged() throws Exception {
        String sql = "select (select count(*) from " + HIVE_TBL + " where l_partkey = 1),"
                + " (select sum(l_quantity) from " + HIVE_TBL + " where l_partkey = 2)";
        Assertions.assertEquals(2, hiveScanCount(sql));
    }

    @Test
    public void testHiveAndIcebergNotMergedAcrossConnectors() throws Exception {
        String sql = "select (select count(*) from " + HIVE_TBL + " where l_partkey = 1),"
                + " (select count(*) from " + TBL + " where c1 = 1)";
        Assertions.assertEquals(1, hiveScanCount(sql));
        Assertions.assertEquals(1, scanCount(sql));
    }

    @Test
    public void testHiveTwoCountBranchesNotMerged() throws Exception {
        // Each count(*) can be answered from manifest metadata on its own, and RewriteSimpleAggToHDFSScanRule
        // only rewrites a single count. Merging them would give up two zero-data plans for one real scan, so the
        // pair must stay apart - and must not blow up in that rule's Preconditions on the way.
        String sql = "select (select count(*) from " + HIVE_TBL + ") c1, (select count(1) from " + HIVE_TBL + ") c2";
        Assertions.assertEquals(2, hiveScanCount(sql));
    }

    @Test
    public void testHiveCountRidesAlongOnAScanThatHappensAnyway() throws Exception {
        // count(*) reads no column, so folding it into the sum branch costs nothing and saves a scan
        String sql = "select (select count(*) from " + HIVE_TBL + ") c,"
                + " (select sum(l_quantity) from " + HIVE_TBL + ") s";
        Assertions.assertEquals(1, hiveScanCount(sql));
    }

    @Test
    public void testIcebergTwoCountBranchesNotMerged() throws Exception {
        String sql = "select (select count(*) from " + TBL + ") c1, (select count(1) from " + TBL + ") c2";
        Assertions.assertEquals(2, scanCount(sql));
    }
}
