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

package com.starrocks.sql.optimizer.rule.mv;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class MaterializedViewRuleTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW lo_count_mv as " +
                "select LO_ORDERDATE,count(LO_LINENUMBER) from lineorder_flat_for_mv group by LO_ORDERDATE;");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW lo_count_key_mv as " +
                "select LO_ORDERDATE, LO_ORDERKEY, count(LO_LINENUMBER) from lineorder_flat_for_mv" +
                " group by LO_ORDERDATE, LO_ORDERKEY;");
    }

    @Test
    public void testMaterializedViewWithCountSelection() throws Exception {
        String sql = "select LO_ORDERDATE,count(LO_LINENUMBER) from lineorder_flat_for_mv group by LO_ORDERDATE;";
        ExecPlan plan = getExecPlan(sql);
        Assert.assertTrue(plan != null);
        Assert.assertEquals(1, plan.getScanNodes().size());
        Assert.assertTrue(plan.getScanNodes().get(0) instanceof OlapScanNode);
        OlapScanNode olapScanNode = (OlapScanNode) plan.getScanNodes().get(0);
        Long selectedIndexid = olapScanNode.getSelectedIndexId();
        GlobalStateMgr globalStateMgr = starRocksAssert.getCtx().getGlobalStateMgr();
        Database database = globalStateMgr.getDb("test");
        Table table = database.getTable("lineorder_flat_for_mv");
        Assert.assertTrue(table instanceof OlapTable);
        OlapTable baseTable = (OlapTable) table;
        Assert.assertEquals(baseTable.getIndexIdByName("lo_count_mv"), selectedIndexid);
    }

    @Test
    public void testKeyColumnsMatch() throws Exception {
        GlobalStateMgr globalStateMgr = starRocksAssert.getCtx().getGlobalStateMgr();
        Database database = globalStateMgr.getDb("test");
        Table table = database.getTable("lineorder_flat_for_mv");
        OlapTable baseTable = (OlapTable) table;

        String sql = "select LO_ORDERDATE, sum(case when LO_ORDERKEY=0 then 0 else 1 end) as test, " +
                "sum(case when LO_ORDERKEY=1 then 1 else 0 end) as nontest " +
                " from lineorder_flat_for_mv group by LO_ORDERDATE;";
        ExecPlan plan = getExecPlan(sql);
        OlapScanNode olapScanNode = (OlapScanNode) plan.getScanNodes().get(0);
        Long selectedIndexid = olapScanNode.getSelectedIndexId();
        Assert.assertNotEquals(baseTable.getIndexIdByName("lo_count_key_mv"), selectedIndexid);
    }

    @Test
    public void testCount1Rewrite() {
        ColumnRefFactory tmpRefFactory = new ColumnRefFactory();
        ColumnRefOperator queryColumnRef = tmpRefFactory.create("count", Type.INT, false);
        ColumnRefOperator mvColumnRef = tmpRefFactory.create("count", Type.INT, false);
        Column mvColumn = new Column();
        List<ScalarOperator> arguments = Lists.newArrayList();
        arguments.add(queryColumnRef);
        CallOperator aggCall = new CallOperator(FunctionSet.COUNT, Type.BIGINT, arguments);
        MaterializedViewRule.RewriteContext rewriteContext =
                new MaterializedViewRule.RewriteContext(aggCall, queryColumnRef, mvColumnRef, mvColumn);
        ColumnRefOperator dsColumnRef = tmpRefFactory.create("ds", Type.INT, false);
        List<ColumnRefOperator> groupKeys = Lists.newArrayList();
        groupKeys.add(dsColumnRef);

        Map<ColumnRefOperator, CallOperator> aggregates = Maps.newHashMap();
        ConstantOperator one = ConstantOperator.createInt(1);
        List<ScalarOperator> countAgruments = Lists.newArrayList();
        countAgruments.add(one);
        CallOperator countOne = new CallOperator(FunctionSet.COUNT, Type.BIGINT, countAgruments);
        ColumnRefOperator countOneKey = tmpRefFactory.create("countKey", Type.BIGINT, false);
        aggregates.put(countOneKey, countOne);
        LogicalAggregationOperator aggregationOperator = new LogicalAggregationOperator(AggType.GLOBAL, groupKeys, aggregates);
        OptExpression aggExpr = OptExpression.create(aggregationOperator);
        MaterializedViewRewriter rewriter = new MaterializedViewRewriter();
        try {
            rewriter.rewrite(aggExpr, rewriteContext);
        } catch (NoSuchElementException e) {
            Assert.assertTrue(false);
        }

        CallOperator countStar = new CallOperator(FunctionSet.COUNT, Type.BIGINT, Lists.newArrayList());
        aggregates.clear();
        ColumnRefOperator countStarkey = tmpRefFactory.create("countStar", Type.BIGINT, false);
        aggregates.put(countStarkey, countStar);
        LogicalAggregationOperator aggregationOperator2 = new LogicalAggregationOperator(AggType.GLOBAL, groupKeys, aggregates);
        OptExpression aggExpr2 = OptExpression.create(aggregationOperator2);
        MaterializedViewRewriter rewriter2 = new MaterializedViewRewriter();
        try {
            rewriter2.rewrite(aggExpr2, rewriteContext);
        } catch (NoSuchElementException e) {
            Assert.assertTrue(false);
        }

        CallOperator sumDs = new CallOperator(FunctionSet.SUM, Type.BIGINT, arguments);
        aggregates.clear();
        ColumnRefOperator sumKey = tmpRefFactory.create("sumKey", Type.BIGINT, false);
        aggregates.put(sumKey, sumDs);
        LogicalAggregationOperator aggregationOperator3 = new LogicalAggregationOperator(AggType.GLOBAL, groupKeys, aggregates);
        OptExpression aggExpr3 = OptExpression.create(aggregationOperator3);
        MaterializedViewRewriter rewriter3 = new MaterializedViewRewriter();
        try {
            rewriter3.rewrite(aggExpr3, rewriteContext);
        } catch (NoSuchElementException e) {
            Assert.assertTrue(false);
        }
    }
}
