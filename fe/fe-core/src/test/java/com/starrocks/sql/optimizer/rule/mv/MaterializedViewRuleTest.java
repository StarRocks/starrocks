package com.starrocks.sql.optimizer.rule.mv;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Assert;
import org.junit.Test;

public class MaterializedViewRuleTest extends PlanTestBase {
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

        String sql = "select count(LO_ORDERDATE) from lineorder_flat_for_mv group by LO_ORDERDATE;";
        ExecPlan plan = getExecPlan(sql);
        OlapScanNode olapScanNode = (OlapScanNode) plan.getScanNodes().get(0);
        Long selectedIndexid = olapScanNode.getSelectedIndexId();
        Assert.assertNotEquals(baseTable.getIndexIdByName("lo_count_mv"), selectedIndexid);

        sql = "select max(LO_ORDERDATE) from lineorder_flat_for_mv group by LO_ORDERDATE;";
        plan = getExecPlan(sql);
        olapScanNode = (OlapScanNode) plan.getScanNodes().get(0);
        selectedIndexid = olapScanNode.getSelectedIndexId();
        Assert.assertEquals(baseTable.getIndexIdByName("lo_count_mv"), selectedIndexid);
    }
}
