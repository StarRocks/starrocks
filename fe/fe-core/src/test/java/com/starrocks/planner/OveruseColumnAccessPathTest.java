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

package com.starrocks.planner;

import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.statistics.CachedStatisticStorageTest.DEFAULT_CREATE_TABLE_TEMPLATE;

public class OveruseColumnAccessPathTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        String createTbl0StmtStr = "" +
                "create table table_struct_smallint (\n" +
                "    col_int int,\n" +
                "    col_struct struct < c_smallint smallint >,\n" +
                "    col_struct2 struct < c_double double >,\n" +
                "    col_struct3 struct < c_int int >\n" +
                ")\n" +
                "duplicate key(col_int)\n" +
                "properties (\"replication_num\" = \"1\")";

        String createTbl1StmtStr = "" +
                "create table duplicate_table_struct (\n" +
                "    col_int int,\n" +
                "    col_string string,\n" +
                "    col_struct struct < c_int int,\n" +
                "    c_float float,\n" +
                "    c_double double,\n" +
                "    c_char char(30),\n" +
                "    c_varchar varchar(200),\n" +
                "    c_date date,\n" +
                "    c_timestamp datetime,\n" +
                "    c_boolean boolean >\n" +
                ")\n" +
                "duplicate key(col_int)\n" +
                "properties (\"replication_num\" = \"1\")";

        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.getSessionVariable().setOptimizerExecuteTimeout(30000);
        FeConstants.runningUnitTest = true;
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase(StatsConstants.STATISTICS_DB_NAME)
                .useDatabase(StatsConstants.STATISTICS_DB_NAME)
                .withTable(DEFAULT_CREATE_TABLE_TEMPLATE);
        starRocksAssert.withDatabase("test").useDatabase("test");
        starRocksAssert.withTable(createTbl0StmtStr);
        starRocksAssert.withTable(createTbl1StmtStr);
    }

    @Test
    public void test() throws Exception {
        String q = "" +
                "select distinct t1.col_struct.c_char \n" +
                "from duplicate_table_struct t1 inner join[shuffle] \n" +
                "  (select col_struct3.c_int as a, max(col_struct2.c_double) as b \n" +
                "   from table_struct_smallint \n" +
                "   group by col_struct3.c_int) t2 on t1.col_struct.c_double = t2.b and t1.col_struct.c_int = t2.a";
        starRocksAssert.getCtx().getSessionVariable().setCboPushDownAggregateMode(1);
        Pair<String, ExecPlan> planAndFragments = UtFrameUtils.getPlanAndFragment(starRocksAssert.getCtx(), q);
        List<ScanNode> scanNodeList = planAndFragments.second.getFragments().stream()
                .flatMap(fragment -> fragment.collectScanNodes().values().stream())
                .filter(scanNode -> scanNode.getTableName().equals("duplicate_table_struct"))
                .collect(Collectors.toList());
        Assert.assertEquals(1, scanNodeList.size());
        ScanNode scanNode = scanNodeList.get(0);
        long aggregationNodeNum = scanNode.getFragment().collectNodes().stream()
                .filter(planNode -> planNode instanceof AggregationNode)
                .count();
        Assert.assertEquals(1, aggregationNodeNum);
        long subfieldPruningProjectingNum = scanNode.getColumnAccessPaths()
                .stream()
                .filter(accessPath -> !accessPath.isFromPredicate() && !accessPath.onlyRoot())
                .count();
        Assert.assertEquals(0, subfieldPruningProjectingNum);
    }
}
