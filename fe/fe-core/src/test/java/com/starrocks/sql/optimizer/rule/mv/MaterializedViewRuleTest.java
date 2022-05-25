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
        String sql = "select L_SHIPDATE, count(L_LINENUMBER) from lineitem group by L_SHIPDATE;";
        ExecPlan plan = getExecPlan(sql);
        Assert.assertTrue(plan != null);
        Assert.assertEquals(1, plan.getScanNodes().size());
        Assert.assertTrue(plan.getScanNodes().get(0) instanceof OlapScanNode);
        OlapScanNode olapScanNode = (OlapScanNode) plan.getScanNodes().get(0);
        Long selectedIndexid = olapScanNode.getSelectedIndexId();
        GlobalStateMgr globalStateMgr = starRocksAssert.getCtx().getGlobalStateMgr();
        Database database = globalStateMgr.getDb("default_cluster:test");
        Table table = database.getTable("lineitem");
        Assert.assertTrue(table instanceof OlapTable);
        OlapTable baseTable = (OlapTable) table;
        Assert.assertEquals(baseTable.getIndexIdByName("lineitem_count_mv"), selectedIndexid);
    }
}
