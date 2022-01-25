// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCheckGeoFunctionGeneratedInvalidUtfString extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @AfterClass
    public static void afterClass() {
        PlanTestBase.tearDown();
    }

    @Test
    public void testFailCases() {
        String[] cases = new String[]{
                "select reverse(st_circle(0.2,0.3,0.4))",
                "select substr(st_circle(0.2,0.3,0.4), 1, 2)",
                "select concat('abc', 'bcd', 'efg', cast(1 as varchar), st_circle(0.2,0.3,0.4))",
                "select reverse(cast(st_circle(0.2,0.3,0.4) as varchar))",
                "select substr(cast(cast(st_circle(0.2,0.3,0.4) as varchar) as varchar), 1, 2)",
                "select concat('abc', 'bcd', 'efg', cast(1 as varchar), " +
                        "cast(cast(st_circle(0.2,0.3,0.4) as varchar) as varchar))",
        };
        for (String sql : cases) {
            try {
                String explain = getFragmentPlan(sql);
                Assert.fail("should throw SemanticException");
            } catch (Exception ex) {
                Assert.assertTrue(ex.getMessage().matches("Function.*cannot invoke.*"));
            }
        }
    }

    @Test
    public void testSuccessCases() throws Exception {
        String[] cases = new String[]{
                "select reverse(st_astext(st_circle(0.2,0.3,0.4)))",
                "select substr(st_astext(st_circle(0.2,0.3,0.4)), 1, 2)",
                "select concat('abc', 'bcd', 'efg', cast(1 as varchar), st_astext(st_circle(0.2,0.3,0.4)))",
                "select reverse(cast(st_astext(st_circle(0.2,0.3,0.4)) as varchar))",
                "select substr(cast(cast(st_astext(st_circle(0.2,0.3,0.4)) as varchar) as varchar), 1, 2)",
                "select concat('abc', 'bcd', 'efg', cast(1 as varchar), " +
                        "cast(cast(st_astext(st_circle(0.2,0.3,0.4)) as varchar) as varchar))",
        };
        for (String sql : cases) {
            String explain = getFragmentPlan(sql);
            Assert.assertTrue(explain.contains("st_astext(st_circle"));
        }
    }
}
