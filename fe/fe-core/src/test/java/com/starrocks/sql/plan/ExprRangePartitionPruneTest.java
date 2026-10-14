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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ExprRangePartitionPruneTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;

        starRocksAssert.withTable("CREATE TABLE `test32` (\n" +
                "  `busness_date` varchar(8) NULL COMMENT \"\",\n" +
                "  `id` int(11) NULL COMMENT \"\",\n" +
                "  `name` varchar(65533) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`busness_date`)\n" +
                "PARTITION BY RANGE(str2date(busness_date, '%Y%m%d'))\n" +
                "(PARTITION p2025 VALUES [(\"0000-01-01\"), (\"2026-01-01\")),\n" +
                "PARTITION p2026 VALUES [(\"2026-01-01\"), (\"2027-01-01\")))\n" +
                "DISTRIBUTED BY RANDOM\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")");

        // The partition column of an expression-partitioned table is a COPY of the source column:
        // PartitionInfoBuilder keeps the name and replaces only the type. A predicate on `code`
        // therefore reaches RangePartitionPruner looking like a predicate on the partition key,
        // carrying VARCHAR values for a BIGINT column. substr() is not order preserving, which is
        // what separates what may be mapped from what may not.
        starRocksAssert.withTable("CREATE TABLE `test_bill_code` (\n" +
                "  `code` varchar(32) NOT NULL COMMENT \"\",\n" +
                "  `v` int NULL\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`code`)\n" +
                "PARTITION BY RANGE(cast(substr(code, 3, 4) as bigint))\n" +
                "(PARTITION p1 VALUES [(\"0\"), (\"2000\")),\n" +
                " PARTITION p2 VALUES [(\"2000\"), (\"5000\")),\n" +
                " PARTITION p3 VALUES [(\"5000\"), (\"9999\")))\n" +
                "DISTRIBUTED BY HASH(code) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")");
    }

    @Test
    public void testExprRangePartitionUpperBoundReserved1() throws Exception {
        String sql = "select * from test32 "
                + "where str2date(busness_date, '%Y%m%d')>= '2026-01-01' "
                + "and str2date(busness_date, '%Y%m%d')<= '2026-03-01'";
        String plan = getFragmentPlan(sql);

        assertCContains(plan, "PREDICATES: str2date(1: busness_date, '%Y%m%d') >= '2026-01-01', " +
                "str2date(1: busness_date, '%Y%m%d') <= '2026-03-01'");

        assertCContains(plan, "partitions=2/2");
    }

    @Test
    public void testExprRangePartitionUpperBoundReserved2() throws Exception {
        String sql = "select * from test32 "
                + "where busness_date >= '20260101' "
                + "and busness_date <= '20260301'";
        String plan = getFragmentPlan(sql);

        assertCContains(plan, "1: busness_date <= '20260301");
        assertCContains(plan, "partitions=1/2");
    }

    /**
     * 'AB1000x' lands on partition value 1000 (p1) and 'AB3000x' on 3000 (p2), so the IN list
     * selects two partitions of three. It used to select all three: the literals went to the pruner
     * as they were written, of the source column's VARCHAR type, and the BIGINT partition column
     * made Preconditions in RangePartitionPruner.prune fail. OptOlapPartitionPruner caught that and
     * gave back every partition, so the query was correct and read the whole table. `=` on the same
     * value pruned, because only a binary predicate had its constant mapped through the partition
     * expression.
     */
    @Test
    public void testInListOnSourceColumnPrunes() throws Exception {
        assertCContains(getFragmentPlan(
                "select * from test_bill_code where code in ('AB1000x', 'AB3000x')"), "partitions=2/3");
        assertCContains(getFragmentPlan(
                "select * from test_bill_code where code in ('AB1000x')"), "partitions=1/3");
        assertCContains(getFragmentPlan(
                "select * from test_bill_code where code in ('AB1000x', 'AB1001x')"), "partitions=1/3");
        // the equality that always worked must keep working
        assertCContains(getFragmentPlan(
                "select * from test_bill_code where code = 'AB1000x'"), "partitions=1/3");
    }

    /**
     * A literal the partition expression cannot take gives up the whole list rather than part of it:
     * dropping one literal from an IN list drops the partitions the rows carrying it live in.
     */
    @Test
    public void testInListKeepsEveryPartitionWhenALiteralCannotBeMapped() throws Exception {
        assertCContains(getFragmentPlan(
                "select * from test_bill_code where code in ('AB1000x', 'Z')"), "partitions=3/3");
    }

    /**
     * substr() does not preserve order, so a range on the source column cannot be mapped onto the
     * partition value and every partition has to be read. That was already the answer, but it was
     * reached by throwing a ClassCastException out of PartitionKey.compareLiteralExpr and catching
     * it in OptOlapPartitionPruner -- which also threw away the pruning of every other column.
     */
    @Test
    public void testRangeOnSourceColumnOfNonMonotonicExpressionKeepsAllPartitions() throws Exception {
        assertCContains(getFragmentPlan(
                "select * from test_bill_code where code >= 'AB1000x'"), "partitions=3/3");
        assertCContains(getFragmentPlan(
                "select * from test_bill_code where code < 'AB5000x'"), "partitions=3/3");
    }

    /**
     * str2date() reaches the pruner's own date conversion, which takes the literal as written. That
     * path is separate from the mapping above and must keep pruning.
     */
    @Test
    public void testInListOnDateExpressionPartitionStillPrunes() throws Exception {
        assertCContains(getFragmentPlan(
                "select * from test32 where busness_date in ('20260101')"), "partitions=1/2");
        assertCContains(getFragmentPlan(
                "select * from test32 where busness_date in ('20260101', '20260301')"), "partitions=1/2");
    }
}
