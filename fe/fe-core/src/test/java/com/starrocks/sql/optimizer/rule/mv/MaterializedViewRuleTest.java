// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.mv;

import com.clearspring.analytics.util.Lists;
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
    }
}
