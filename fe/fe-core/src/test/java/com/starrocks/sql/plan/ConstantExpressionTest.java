// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.plan;

import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

public class ConstantExpressionTest {
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDir = "fe/mocked/ConstantExpressTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.startFEServer(runningDir);
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
    }

    private static void testFragmentPlanContainsConstExpr(String sql, String result) throws Exception {
        String explainString = UtFrameUtils.getNewFragmentPlan(connectContext, sql);
        System.out.println("explainString=" + explainString);
        Assert.assertTrue(explainString.contains("constant exprs: \n         " + result));
    }

    private static void testFragmentPlanContains(String sql, String result) throws Exception {
        String explainString = UtFrameUtils.getNewFragmentPlan(connectContext, sql);
        System.out.println("explainString=" + explainString);
        Assert.assertTrue(explainString.contains(result));
    }

    @Test
    public void testDate() throws Exception {
        testFragmentPlanContainsConstExpr(
                "select date_format('2020-02-19 16:01:12','%H%i');",
                "'1601'");

        testFragmentPlanContainsConstExpr(
                "select date_format('2020-02-19 16:01:12','%Y%m%d');",
                "'20200219'");

        testFragmentPlanContainsConstExpr(
                "select date_format(date_sub('2018-07-24 07:16:19',1),'yyyyMMdd');",
                "'20180723'");

        testFragmentPlanContainsConstExpr(
                "select year('2018-07-24')*12 + month('2018-07-24');",
                "24223");

        testFragmentPlanContainsConstExpr(
                "select date_format('2018-08-08 07:16:19', 'yyyyMMdd');",
                "'20180808'");

        testFragmentPlanContainsConstExpr(
                "select date_format('2018-08-08 07:16:19', 'yyyy-MM-dd HH:mm:ss');",
                "'2018-08-08 07:16:19'");

        testFragmentPlanContainsConstExpr(
                "select datediff('2018-08-08','1970-01-01');",
                "17751");

        testFragmentPlanContainsConstExpr(
                "select date_add('2018-08-08', 1);",
                "'2018-08-09 00:00:00'");

        testFragmentPlanContainsConstExpr(
                "select date_add('2018-08-08', -1);",
                "'2018-08-07 00:00:00'");

        testFragmentPlanContainsConstExpr(
                "select date_sub('2018-08-08 07:16:19',1);",
                "'2018-08-07 07:16:19'");

        testFragmentPlanContainsConstExpr(
                "select year('2018-07-24');",
                "2018");

        testFragmentPlanContainsConstExpr(
                "select month('2018-07-24');",
                "7");

        testFragmentPlanContainsConstExpr(
                "select day('2018-07-24');",
                "24");

        testFragmentPlanContainsConstExpr(
                "select UNIX_TIMESTAMP(\"1970-01-01 08:00:01\");",
                "1");

        testFragmentPlanContainsConstExpr(
                "select now();",
                "");

        testFragmentPlanContainsConstExpr(
                "select curdate();",
                "");
    }

    @Test
    public void testCast() throws Exception {
        testFragmentPlanContainsConstExpr(
                "select cast ('1' as int) ;",
                "1");

        testFragmentPlanContainsConstExpr(
                "select cast ('2020-01-20' as date);",
                "'2020-01-20'");
    }

    @Test
    public void testArithmetic() throws Exception {
        testFragmentPlanContainsConstExpr(
                "select 1 + 10;",
                "11");

        testFragmentPlanContainsConstExpr(
                "select 1 - 10;",
                "-9");

        testFragmentPlanContainsConstExpr(
                "select 1 * 10.0;",
                "10");

        testFragmentPlanContainsConstExpr(
                "select 1 / 10.0;",
                "0.1");
    }

    @Test
    public void testDecimalArithmetic() throws Exception {
        testFragmentPlanContainsConstExpr(
                "select 1 * 10.0;",
                "10");

        testFragmentPlanContainsConstExpr(
                "select 1 + 10.0;",
                "11");

        testFragmentPlanContainsConstExpr(
                "select 1 - 10.0;",
                "-9");

        testFragmentPlanContainsConstExpr(
                "select cast('10.11' as DECIMAL(9,2)) + cast('120.34' as DECIMAL(9,2));",
                "130.45");

        testFragmentPlanContainsConstExpr(
                "select cast('10.11' as DECIMAL(9,2)) * cast('120.34' as DECIMAL(9,2));",
                "1216.6374");
    }

    @Test
    public void testDecimalArithmeticDivide() throws Exception {
        testFragmentPlanContainsConstExpr(
                "select 1 / 10.0;",
                "0.1");
    }

    @Test
    public void testMath() throws Exception {
        testFragmentPlanContainsConstExpr(
                "select floor(2.3);",
                "2");
    }

    @Test
    public void testPredicate() throws Exception {
        testFragmentPlanContainsConstExpr(
                "select 1 > 2",
                "FALSE");

        testFragmentPlanContainsConstExpr(
                "select 1 = 1",
                "TRUE");
    }

    @Test
    public void testConstantInPredicate() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        // for constant NOT IN PREDICATE
        testFragmentPlanContains("select 1 not in (1, 2);", "FALSE");

        testFragmentPlanContains("select 1 not in (2, 3);", "TRUE");

        testFragmentPlanContains("select 1 not in (2, null);", "NULL");

        testFragmentPlanContains("select 1 not in (1, 2, null);", "FALSE");

        testFragmentPlanContains("select null not in (1, 2);", "NULL");

        testFragmentPlanContains("select null not in (null);", "NULL");

        // for constant IN PREDICATE
        testFragmentPlanContains("select 1 in (1, 2);", "TRUE");

        testFragmentPlanContains("select 1 in (2, 3);", "FALSE");

        testFragmentPlanContains("select 1 in (1, 2, NULL);", "TRUE");

        testFragmentPlanContains("select 1 in (2, NULL);", "NULL");

        testFragmentPlanContains("select null in (2);", "NULL");

        testFragmentPlanContains("select null in (null);", "NULL");
    }
}
