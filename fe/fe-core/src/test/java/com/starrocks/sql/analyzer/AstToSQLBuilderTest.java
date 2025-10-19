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
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AstToSQLBuilderTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
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
                            "SELECT \n  *\nFROM FILES(\"aws.s3.access_key\" = \"***\", \"aws.s3.region\" = \"us-west-1\", " +
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
                            "SELECT \n  *\nFROM FILES(\"aws.s3.access_key\" = \"***\", \"aws.s3.region\" = \"us-west-1\", " +
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
                        "SELECT \n  *\nFROM FILES(\"aws.s3.access_key\" = \"***\", \"aws.s3.region\" = \"us-west-1\", " +
                        "\"aws.s3.secret_key\" = \"***\", \"format\" = \"parquet\", \"path\" = \"s3://xxx/zzz\")",
                AstToSQLBuilder.toSQL(stmt));
    }
    @Test
    public void testSelectStarExcludeToSQL() throws Exception {
        String sql = "SELECT * EXCLUDE (name, email) FROM test_exclude;";
        StatementBase stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertEquals("SELECT \n  * EXCLUDE ( \"name\",\"email\" ) \nFROM `test_exclude`",
                AstToSQLBuilder.toSQL(stmt));
        
        sql = "SELECT test_exclude.* EXCLUDE (name) FROM test_exclude";
        stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertEquals("SELECT \n  test_exclude.* EXCLUDE ( \"name\" ) \nFROM `test_exclude`",
                AstToSQLBuilder.toSQL(stmt));
    }

    @Test
    public void testFunctionTable() {
        String sql = "SELECT * from tarray, unnest(v3) as t(x)";
        StatementBase stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertEquals("SELECT \n  *\nFROM `tarray` , unnest(`v3`) t(`x`) ",
                AstToSQLBuilder.toSQL(stmt));

        sql = "SELECT * from t0, generate_series(v1, v2, 1) as t(x)";
        stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertEquals("SELECT \n  *\nFROM `t0` , generate_series(`v1`,`v2`,1) t(`x`) ",
                AstToSQLBuilder.toSQL(stmt));
    }


    @Test
    public void testCaseWhenFormatting() {
        String sql = "SELECT CASE WHEN v1 < 10 THEN 'low' WHEN v1 >= 10 AND v1 < 20 THEN 'medium' ELSE 'high' END FROM t0";
        StatementBase stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        String expected = "SELECT \n" +
                "  CASE\n" +
                "    WHEN (`v1` < 10) THEN 'low'\n" +
                "    WHEN ((`v1` >= 10) AND (`v1` < 20)) THEN 'medium'\n" +
                "    ELSE 'high'\n" +
                "  END\n" +
                "FROM `t0`";
        Assertions.assertEquals(expected, AstToSQLBuilder.toSQL(stmt));
    }

    @Test
    public void testComplexCTEFormatting() {
        String sql = "WITH cte1 AS (SELECT v1, v2 FROM t0), " +
                "cte2 AS (SELECT v1, v3 FROM t1) " +
                "SELECT * FROM cte1 JOIN cte2 ON cte1.v1 = cte2.v1";
        StatementBase stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        String expected = "WITH `cte1` AS (\n" +
                "  SELECT \n" +
                "    `v1`,\n" +
                "    `v2`\n" +
                "FROM `t0`\n" +
                "),\n" +
                "`cte2` AS (\n" +
                "  SELECT \n" +
                "    `v1`,\n" +
                "    `v3`\n" +
                "FROM `t1`\n" +
                ")\n" +
                "SELECT \n" +
                "  *\n" +
                "FROM `cte1` INNER JOIN `cte2` ON `cte1`.`v1` = `cte2`.`v1`";
        Assertions.assertEquals(expected, AstToSQLBuilder.toSQL(stmt));
    }

    @Test
    public void testComplexNestedCTEWithCaseWhen() {
        // This is a complex query from issue #64056
        String sql = "WITH cte01 (id, region, len_bucket) AS " +
                "(SELECT cw.tbl01.id, cw.tbl01.region, " +
                "CASE WHEN (array_length(cw.tbl01.col_arr) < 2) THEN 'bucket1' " +
                "WHEN ((array_length(cw.tbl01.col_arr) >= 2) AND (array_length(cw.tbl01.col_arr) < 4)) THEN 'bucket2-3' " +
                "ELSE NULL END AS len_bucket " +
                "FROM cw.tbl01), " +
                "cte02 (id, region, priority) AS " +
                "(SELECT cte01.id, cte01.region, " +
                "CASE WHEN ((cte01.len_bucket = 'bucket1') AND (cte01.region = 'EMEA')) THEN 'priority1' " +
                "WHEN ((cte01.len_bucket = 'bucket1') AND (cte01.region = 'APAC')) THEN 'priority2' " +
                "WHEN ((cte01.len_bucket = 'bucket1') AND (cte01.region = 'NORAM')) THEN 'priority3' " +
                "WHEN ((cte01.len_bucket = 'bucket1') AND (cte01.region = 'LATAM')) THEN 'priority4' " +
                "ELSE NULL END AS priority " +
                "FROM cte01) " +
                "SELECT cte02.id, cte02.region, cte02.priority " +
                "FROM cte02 " +
                "WHERE cte02.priority IS NOT NULL";
        StatementBase stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        String expected = "WITH `cte01` (`id`, `region`, `len_bucket`) AS (\n" +
                "  SELECT \n" +
                "    `cw`.`tbl01`.`id`,\n" +
                "    `cw`.`tbl01`.`region`,\n" +
                "    CASE\n" +
                "      WHEN ((array_length(`cw`.`tbl01`.`col_arr`)) < 2) THEN 'bucket1'\n" +
                "      WHEN (((array_length(`cw`.`tbl01`.`col_arr`)) >= 2) AND " +
                "((array_length(`cw`.`tbl01`.`col_arr`)) < 4)) THEN 'bucket2-3'\n" +
                "      ELSE NULL\n" +
                "    END AS `len_bucket`\n" +
                "FROM `cw`.`tbl01`\n" +
                "),\n" +
                "`cte02` (`id`, `region`, `priority`) AS (\n" +
                "  SELECT \n" +
                "    `cte01`.`id`,\n" +
                "    `cte01`.`region`,\n" +
                "    CASE\n" +
                "      WHEN ((`cte01`.`len_bucket` = 'bucket1') AND (`cte01`.`region` = 'EMEA')) THEN 'priority1'\n" +
                "      WHEN ((`cte01`.`len_bucket` = 'bucket1') AND (`cte01`.`region` = 'APAC')) THEN 'priority2'\n" +
                "      WHEN ((`cte01`.`len_bucket` = 'bucket1') AND (`cte01`.`region` = 'NORAM')) THEN 'priority3'\n" +
                "      WHEN ((`cte01`.`len_bucket` = 'bucket1') AND (`cte01`.`region` = 'LATAM')) THEN 'priority4'\n" +
                "      ELSE NULL\n" +
                "    END AS `priority`\n" +
                "FROM `cte01`\n" +
                ")\n" +
                "SELECT \n" +
                "  `cte02`.`id`,\n" +
                "  `cte02`.`region`,\n" +
                "  `cte02`.`priority`\n" +
                "FROM `cte02`\n" +
                "WHERE `cte02`.`priority` IS NOT NULL";
        Assertions.assertEquals(expected, AstToSQLBuilder.toSQL(stmt));
    }
}
