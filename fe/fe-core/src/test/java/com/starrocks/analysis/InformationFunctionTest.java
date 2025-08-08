package com.starrocks.analysis;

import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Test;

import java.util.UUID;

public class InformationFunctionTest extends PlanTestBase {
    @Test
    public void testInformationFunction() throws Exception {
        starRocksAssert.getCtx().setConnectionId(1);
        starRocksAssert.getCtx().setSessionId(UUID.fromString("436b9df9-5406-4eb7-984e-9d96acedde16"));
        String sql = "select connection_id(), catalog(), database(), schema(), user(), session_id(), user()";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  <slot 2> : 1\n" +
                "  |  <slot 3> : 'default_catalog'\n" +
                "  |  <slot 4> : 'test'\n" +
                "  |  <slot 5> : 'test'\n" +
                "  |  <slot 7> : '436b9df9-5406-4eb7-984e-9d96acedde16'\n" +
                "  |  <slot 8> : '\\'root\\'@%'");
    }
}
