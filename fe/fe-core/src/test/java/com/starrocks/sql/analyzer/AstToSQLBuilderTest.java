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


package com.starrocks.sql.analyzer;

import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class AstToSQLBuilderTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    private static List<Boolean> nullsFirstOf(String sql, long sqlMode) {
        QueryStatement stmt = (QueryStatement) SqlParser.parseSingleStatement(sql, sqlMode);
        return stmt.getQueryRelation().getOrderBy().stream()
                .map(OrderByElement::getNullsFirstParam).collect(Collectors.toList());
    }

    @Test
    public void testOrderByNullOrderingSurvivesSqlMode() {
        // A view definition is generated from the AST and parsed again under the sql_mode it was created with. The
        // NULL ordering a bare ASC / DESC means depends on SORT_NULLS_LAST, so the generated SQL must spell it out
        // (RB-1092).
        String sql = "select v1 from t0 order by v1 nulls first, v2 desc nulls last, v3, v1 desc";
        for (long createMode : new long[] {SqlModeHelper.MODE_DEFAULT, SqlModeHelper.MODE_SORT_NULLS_LAST}) {
            List<Boolean> expected = nullsFirstOf(sql, createMode);
            String generated = AstToSQLBuilder.toSQL(SqlParser.parseSingleStatement(sql, createMode));
            for (long parseMode : new long[] {SqlModeHelper.MODE_DEFAULT, SqlModeHelper.MODE_SORT_NULLS_LAST}) {
                Assertions.assertEquals(expected, nullsFirstOf(generated, parseMode), generated);
            }
        }
        String generated = AstToSQLBuilder.toSQL(SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_SORT_NULLS_LAST));
        // under SORT_NULLS_LAST a bare ASC means NULLS LAST and a bare DESC means NULLS FIRST
        Assertions.assertTrue(generated.contains("`v1` ASC NULLS FIRST, `v2` DESC NULLS LAST, `v3` ASC NULLS LAST, " +
                "`v1` DESC NULLS FIRST"), generated);

        // the digest text keeps its historical form, which leaves the default ordering out
        String digest = AstToSQLBuilder.toDigest(SqlParser.parseSingleStatement("select v1 from t0 order by v1",
                SqlModeHelper.MODE_DEFAULT));
        Assertions.assertFalse(digest.contains("NULLS"), digest);
    }

    @Test
    public void testCreatePipe() {
        {
            String sql = "create pipe if not exists pipe1 properties('auto_ingest' = 'true') as insert into t0 (v1, v2)" +
                    "select * from files('path' = 's3://xxx/zzz', 'format' = 'parquet', 'aws.s3.access_key' = 'ghi', " +
                    "'aws.s3.secret_key' = 'jkl', 'aws.s3.region' = 'us-west-1')";
            StatementBase stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
            Assertions.assertEquals(
                    "CREATE PIPE IF NOT EXISTS pipe1 PROPERTIES(\"auto_ingest\" = \"true\") AS INSERT INTO `t0` (`v1`,`v2`) " +
                            "SELECT *\nFROM FILES(\"aws.s3.access_key\" = \"***\", \"aws.s3.region\" = \"us-west-1\", " +
                            "\"aws.s3.secret_key\" = \"***\", \"format\" = \"parquet\", \"path\" = \"s3://xxx/zzz\")",
                    AstToSQLBuilder.toSQL(stmt));
        }

        {
            String sql = "create or replace pipe pipe1 as insert into t0 (v1, v2)" +
                    "select * from files('path' = 's3://xxx/zzz', 'format' = 'parquet', 'aws.s3.access_key' = 'ghi', " +
                    "'aws.s3.secret_key' = 'jkl', 'aws.s3.region' = 'us-west-1')";
            StatementBase stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
            Assertions.assertEquals(
                    "CREATE OR REPLACE PIPE pipe1 AS INSERT INTO `t0` (`v1`,`v2`) " +
                            "SELECT *\nFROM FILES(\"aws.s3.access_key\" = \"***\", \"aws.s3.region\" = \"us-west-1\", " +
                            "\"aws.s3.secret_key\" = \"***\", \"format\" = \"parquet\", \"path\" = \"s3://xxx/zzz\")",
                    AstToSQLBuilder.toSQL(stmt));
        }
    }

    @Test
    public void testInsertFromFiles() {
        String sql = "insert into t0 (v1, v2)" +
                "select * from files('path' = 's3://xxx/zzz', 'format' = 'parquet', 'aws.s3.access_key' = 'ghi', " +
                "'aws.s3.secret_key' = 'jkl', 'aws.s3.region' = 'us-west-1')";
        StatementBase stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertEquals(
                "INSERT INTO `t0` (`v1`,`v2`) " +
                        "SELECT *\nFROM FILES(\"aws.s3.access_key\" = \"***\", \"aws.s3.region\" = \"us-west-1\", " +
                        "\"aws.s3.secret_key\" = \"***\", \"format\" = \"parquet\", \"path\" = \"s3://xxx/zzz\")",
                AstToSQLBuilder.toSQL(stmt));
    }
    @Test
    public void testSelectStarExcludeToSQL() throws Exception {
        String sql = "SELECT * EXCLUDE (name, email) FROM test_exclude;";
        StatementBase stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertEquals("SELECT * EXCLUDE ( \"name\",\"email\" ) \nFROM `test_exclude`",
                AstToSQLBuilder.toSQL(stmt));
        
        sql = "SELECT test_exclude.* EXCLUDE (name) FROM test_exclude";
        stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertEquals("SELECT test_exclude.* EXCLUDE ( \"name\" ) \nFROM `test_exclude`",
                AstToSQLBuilder.toSQL(stmt));
    }

    @Test
    public void testFunctionTable() {
        String sql = "SELECT * from tarray, unnest(v3) as t(x)";
        StatementBase stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertEquals("SELECT *\nFROM `tarray` , unnest(`v3`) t(`x`) ",
                AstToSQLBuilder.toSQL(stmt));

        sql = "SELECT * from t0, generate_series(v1, v2, 1) as t(x)";
        stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertEquals("SELECT *\nFROM `t0` , generate_series(`v1`,`v2`,1) t(`x`) ",
                AstToSQLBuilder.toSQL(stmt));
    }
}
