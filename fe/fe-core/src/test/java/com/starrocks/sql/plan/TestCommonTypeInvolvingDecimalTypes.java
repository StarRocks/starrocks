// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.plan;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCommonTypeInvolvingDecimalTypes extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE tab0 (\n" +
                "  c_0_0 TINYINT NULL, \n" +
                "  c_0_1 BOOLEAN NULL, \n" +
                "  c_0_2 LARGEINT NOT NULL, \n" +
                "  c_0_3 INT NOT NULL, \n" +
                "  c_0_4 BOOLEAN NULL, \n" +
                "  c_0_5 DOUBLE NOT NULL, \n" +
                "  c_0_6 VARCHAR(51) NULL, \n" +
                "  c_0_7 BOOLEAN NULL, \n" +
                "  c_0_8 DECIMAL(28, 20) NULL, \n" +
                "  c_0_9 DATE NOT NULL, \n" +
                "  c_0_10 DATETIME NOT NULL\n" +
                ")\n" +
                "UNIQUE KEY (c_0_0, c_0_1, c_0_2, c_0_3)\n" +
                "DISTRIBUTED BY HASH (c_0_3, c_0_2, c_0_0, c_0_1)\n" +
                "properties(\"replication_num\" = \"1\");");
    }

    @AfterClass
    public static void afterClass() {
        PlanTestBase.tearDown();
    }

    @Test
    public void testCastWhenInvolvingDecimalTypes() throws Exception {
        String sql = "SELECT \n" +
                "  (\n" +
                "    CAST(\n" +
                "      (5) < (\n" +
                "        CASE WHEN TRUE THEN CAST(\n" +
                "          tab0.c_0_9 AS DECIMAL(7, 4)\n" +
                "        ) WHEN (\n" +
                "          NOT (\n" +
                "            (\n" +
                "              NOT (\n" +
                "                (tab0.c_0_2) BETWEEN (tab0.c_0_2) \n" +
                "                AND (tab0.c_0_2)\n" +
                "              )\n" +
                "            )\n" +
                "          )\n" +
                "        ) THEN 7 WHEN TRUE THEN 4 END\n" +
                "      ) AS BOOLEAN\n" +
                "    ) = TRUE\n" +
                "  ) AS COUNT \n" +
                "FROM \n" +
                "  tab0;";
        String explain = getFragmentPlan(sql);
        System.out.println(explain);
        Assert.assertTrue(!explain.contains("DECIMAL32"));
    }

    @Test
    public void testBinaryPredicateInvolvingDecimalTypes() throws Exception {
        String sql = "SELECT * from tab0 where cast(tab0.c_0_9 as DECIMAL64(7,4)) > tab0.c_0_0";
        String explain = getFragmentPlan(sql);
        System.out.println(explain);
        Assert.assertTrue(!explain.contains("DECIMAL32"));
    }
}
