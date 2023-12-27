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

import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.parser.SqlParser;
import org.junit.BeforeClass;
import org.junit.Test;

public class TrinoCtasTest extends TrinoTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        TrinoTestBase.beforeClass();
    }

    @Test
    public void testCtasTrinoDialect() throws Exception {
        String ctasSql = "create table test.t_doy as select doy(date '2022-03-06')";
        try {
            connectContext.getSessionVariable().setSqlDialect("trino");
            CreateTableAsSelectStmt ctasStmt =
                    (CreateTableAsSelectStmt) SqlParser.parse(ctasSql, connectContext.getSessionVariable()).get(0);
            QueryStatement queryStmt = ctasStmt.getQueryStatement();
            assertPlanContains(queryStmt, "dayofyear('2022-03-06 00:00:00')");

            connectContext.getSessionVariable().setSqlDialect("starrocks");
            analyzeFail(ctasSql, "No matching function with signature: doy(date)");
        } finally {
            connectContext.getSessionVariable().setSqlDialect("trino");
        }
    }

    @Test
    public void testCtasSubQueryNoAlias() throws Exception {
        String ctasSql = "create table test.sub_query_alias as select * from ( select * from ( select 1, 2, 3 ) )";
        try {
            connectContext.getSessionVariable().setSqlDialect("trino");
            analyzeSuccess(ctasSql);

            connectContext.getSessionVariable().setSqlDialect("starrocks");
            analyzeFail(ctasSql, "Every derived table must have its own alias");
        } finally {
            connectContext.getSessionVariable().setSqlDialect("trino");
        }
    }

    @Test
    public void testCtasSubQueryHasAlias() throws Exception {
        String ctasSql =
                "create table test.sub_query_alias as select * from ( select * from ( select 1, 2, 3 ) t1 ) t2";
        try {
            connectContext.getSessionVariable().setSqlDialect("trino");
            analyzeSuccess(ctasSql);

            connectContext.getSessionVariable().setSqlDialect("starrocks");
            analyzeSuccess(ctasSql);
        } finally {
            connectContext.getSessionVariable().setSqlDialect("trino");
        }
    }
}
