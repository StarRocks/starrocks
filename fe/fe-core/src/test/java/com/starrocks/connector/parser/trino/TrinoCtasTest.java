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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TrinoCtasTest extends TrinoTestBase {
    @BeforeAll
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
            Assertions.assertEquals(ctasSql, ctasStmt.getOrigStmt().originStmt);

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

    @Test
    public void testCtasWithTrinoWithClause() throws Exception {
        // Native Trino CTAS uses WITH (key = value) instead of PROPERTIES.
        // Keys are identifiers (quoted when they contain dots/hyphens) and
        // values can be string/int/double/boolean literals or identifiers.
        String ctasSql = "create table test.t_with_clause "
                + "with (replication_num = '1', file_format = 'parquet', "
                + "\"write.parquet.compression-codec\" = 'zstd') "
                + "as select doy(date '2022-03-06')";
        try {
            connectContext.getSessionVariable().setSqlDialect("trino");
            CreateTableAsSelectStmt ctasStmt =
                    (CreateTableAsSelectStmt) SqlParser.parse(ctasSql, connectContext.getSessionVariable()).get(0);
            Assertions.assertEquals("1",
                    ctasStmt.getCreateTableStmt().getProperties().get("replication_num"));
            Assertions.assertEquals("parquet",
                    ctasStmt.getCreateTableStmt().getProperties().get("file_format"));
            Assertions.assertEquals("zstd",
                    ctasStmt.getCreateTableStmt().getProperties().get("write.parquet.compression-codec"));
            assertPlanContains(ctasStmt.getQueryStatement(), "dayofyear('2022-03-06 00:00:00')");
        } finally {
            connectContext.getSessionVariable().setSqlDialect("trino");
        }
    }

    @Test
    public void testCtasWithPropertySetToDefaultIsRejected() throws Exception {
        // Trino's "key = DEFAULT" asks the connector to use its built-in default.
        // StarRocks' property map has no unset-vs-empty distinction, so silently
        // mapping DEFAULT to "" would override connector defaults with the empty
        // string. Ensure the parser rejects this case explicitly.
        String ctasSql = "create table test.t_default_prop "
                + "with (replication_num = DEFAULT) "
                + "as select 1";
        try {
            connectContext.getSessionVariable().setSqlDialect("trino");
            Exception ex = Assertions.assertThrows(Exception.class,
                    () -> SqlParser.parse(ctasSql, connectContext.getSessionVariable()));
            Assertions.assertTrue(ex.getMessage().toLowerCase().contains("default"),
                    "expected error about SET DEFAULT, got: " + ex.getMessage());
        } finally {
            connectContext.getSessionVariable().setSqlDialect("trino");
        }
    }
}
