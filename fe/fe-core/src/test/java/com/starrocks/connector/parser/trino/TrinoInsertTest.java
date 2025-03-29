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

package com.starrocks.connector.parser.trino;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TrinoInsertTest extends TrinoTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        TrinoTestBase.beforeClass();
    }

    @Before
    public void setUp() {
        connectContext.getSessionVariable().setSqlDialect("trino");
    }

    @Test
    public void testInsertTrinoDialect() throws Exception {
        String insertSql = "insert into t3 select doy(date '2022-03-06')";
        assertPlanContains(insertSql, "dayofyear('2022-03-06 00:00:00')");

        connectContext.getSessionVariable().setSqlDialect("starrocks");
        analyzeFail(insertSql, "No matching function with signature: doy(date)");
    }

    @Test
    public void testInsertValues() throws Exception {
        String sql = "insert into t0(v1) values (1)";
        assertPlanContains(sql, "Project\n" +
                "  |  <slot 1> : 1: column_0\n" +
                "  |  <slot 2> : NULL\n" +
                "  |  <slot 3> : NULL");

        sql = "insert into t0(v1, v2) values (1, 2)";
        assertPlanContains(sql, "Project\n" +
                "  |  <slot 1> : 1: column_0\n" +
                "  |  <slot 2> : 2: column_1\n" +
                "  |  <slot 3> : NULL");

        sql = "insert into t0 values (1, 2, 3)";
        assertPlanContains(sql, "constant exprs: \n" +
                "         1 | 2 | 3");

        sql = "insert into t3(day) values (20220306)";
        assertPlanContains(sql, "0:UNION\n" +
                "     constant exprs: \n" +
                "         20220306");

        sql = "insert into t3 values (20220306)";
        assertPlanContains(sql, "0:UNION\n" +
                "     constant exprs: \n" +
                "         20220306");
    }
}