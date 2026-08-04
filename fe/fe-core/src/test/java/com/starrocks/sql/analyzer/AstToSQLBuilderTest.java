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
import com.starrocks.sql.formatter.AST2SQLVisitor;
import com.starrocks.sql.formatter.FormatOptions;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AstToSQLBuilderTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }
    
    // Helper method to format SQL with pretty format enabled
    private static String toPrettySQL(StatementBase stmt) {
        return AST2SQLVisitor.withOptions(
                FormatOptions.allEnable()
                        .setColumnSimplifyTableName(false)
                        .setEnableDigest(false)
                        .setEnablePrettyFormat(true))
                .visit(stmt);
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
        Assertions.assertEquals(expected, toPrettySQL(stmt));
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
        Assertions.assertEquals(expected, toPrettySQL(stmt));
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
        Assertions.assertEquals(expected, toPrettySQL(stmt));
    }

    @Test
    public void testTimeTravelRoundTrip() {
        {
            String sql = "SELECT * FROM t0 FOR VERSION AS OF 1";
            StatementBase stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
            String serializedSql = AstToSQLBuilder.toSQL(stmt);
            Assertions.assertEquals("SELECT *\nFROM `t0` FOR VERSION AS OF 1", serializedSql);
            Assertions.assertEquals("SELECT *\nFROM `t0` FOR VERSION AS OF 1", AstToSQLBuilder.buildSimple(stmt));
            Assertions.assertDoesNotThrow(() -> SqlParser.parseSingleStatement(serializedSql, SqlModeHelper.MODE_DEFAULT));
        }

        {
            String sql = "SELECT * FROM t0 FOR SYSTEM_TIME AS OF '2016-10-09 08:07:06'";
            StatementBase stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
            String serializedSql = AstToSQLBuilder.toSQL(stmt);
            Assertions.assertEquals("SELECT *\nFROM `t0` FOR SYSTEM_TIME AS OF '2016-10-09 08:07:06'", serializedSql);
            Assertions.assertEquals("SELECT *\nFROM `t0` FOR SYSTEM_TIME AS OF '2016-10-09 08:07:06'",
                    AstToSQLBuilder.buildSimple(stmt));
            Assertions.assertDoesNotThrow(() -> SqlParser.parseSingleStatement(serializedSql, SqlModeHelper.MODE_DEFAULT));
        }
    }

    @Test
    public void testInsertValuesRoundTrip() {
        // The INSERT source takes a bare VALUES list. Emitting the parenthesized derived-table form
        // `INSERT INTO t (cols) (VALUES ...)` produces SQL the parser rejects.
        String[][] cases = {
                {"insert into t0 (v1, v2) values (1, 111)", "INSERT INTO `t0` (`v1`,`v2`) VALUES(1, 111)"},
                {"insert into t0 values (1, 2), (3, 4)", "INSERT INTO `t0` VALUES(1, 2), (3, 4)"},
                {"insert into t0 values (1, null)", "INSERT INTO `t0` VALUES(1, NULL)"},
                {"insert overwrite t0 (v1) values (1)", "INSERT OVERWRITE `t0` (`v1`) VALUES(1)"},
                {"insert into t0 with label lb (v1) values (1)", "INSERT INTO `t0` WITH LABEL `lb` (`v1`) VALUES(1)"},
        };
        for (String[] c : cases) {
            StatementBase stmt = SqlParser.parseSingleStatement(c[0], SqlModeHelper.MODE_DEFAULT);
            String serializedSql = AstToSQLBuilder.toSQL(stmt);
            Assertions.assertEquals(c[1], serializedSql, c[0]);
            Assertions.assertDoesNotThrow(() -> SqlParser.parseSingleStatement(serializedSql, SqlModeHelper.MODE_DEFAULT),
                    c[0]);
            // Deparsing is a fixpoint: re-serializing the output must not change it again.
            Assertions.assertEquals(serializedSql,
                    AstToSQLBuilder.toSQL(SqlParser.parseSingleStatement(serializedSql, SqlModeHelper.MODE_DEFAULT)),
                    c[0]);
        }
    }

    @Test
    public void testValuesInDerivedTablePositionKeepsParentheses() {
        // Counterpart to testInsertValuesRoundTrip: outside the INSERT source, a VALUES relation sits in
        // derived-table position and *must* stay parenthesized.
        String[][] cases = {
                // explicit column names
                {"select * from (values (1, 'a'), (2, 'b')) tt(x, y)",
                        "SELECT *\nFROM (VALUES(1, 'a'), (2, 'b')) tt(x,y)"},
                // alias without column names: the deparser supplies the generated column_0
                {"select cast(column_0 as datetime) from (values ('2020.02.29')) as tmp",
                        "SELECT CAST(`column_0` AS DATETIME)\nFROM (VALUES('2020.02.29')) tmp(column_0)"},
                // VALUES nested under an INSERT ... SELECT: the INSERT source is the SELECT, not the
                // VALUES, so the inner relation must still be parenthesized
                {"insert into t0 select cast(column_0 as int) from (values ('1')) as tmp",
                        "INSERT INTO `t0` SELECT CAST(`column_0` AS INT)\nFROM (VALUES('1')) tmp(column_0)"},
        };
        for (String[] c : cases) {
            StatementBase stmt = SqlParser.parseSingleStatement(c[0], SqlModeHelper.MODE_DEFAULT);
            String serializedSql = AstToSQLBuilder.toSQL(stmt);
            Assertions.assertEquals(c[1], serializedSql, c[0]);
            Assertions.assertDoesNotThrow(() -> SqlParser.parseSingleStatement(serializedSql, SqlModeHelper.MODE_DEFAULT),
                    c[0]);
            Assertions.assertEquals(serializedSql,
                    AstToSQLBuilder.toSQL(SqlParser.parseSingleStatement(serializedSql, SqlModeHelper.MODE_DEFAULT)),
                    c[0]);
        }
    }

    @Test
    public void testInsertValuesWithDefaultKeyword() {
        // DefaultValueExpr used to serialize to null, so joining the row's child strings threw NPE.
        String[][] cases = {
                {"insert into t0 (v1, v2) values (DEFAULT, 3)", "INSERT INTO `t0` (`v1`,`v2`) VALUES(DEFAULT, 3)"},
                {"insert into t0 values (DEFAULT)", "INSERT INTO `t0` VALUES(DEFAULT)"},
                {"insert into t0 (v1, v2) values (1, DEFAULT), (2, 3)",
                        "INSERT INTO `t0` (`v1`,`v2`) VALUES(1, DEFAULT), (2, 3)"},
                {"insert into t0 (v1, v2) values (DEFAULT, DEFAULT)",
                        "INSERT INTO `t0` (`v1`,`v2`) VALUES(DEFAULT, DEFAULT)"},
        };
        for (String[] c : cases) {
            StatementBase stmt = SqlParser.parseSingleStatement(c[0], SqlModeHelper.MODE_DEFAULT);
            String serializedSql = Assertions.assertDoesNotThrow(() -> AstToSQLBuilder.toSQL(stmt), c[0]);
            Assertions.assertEquals(c[1], serializedSql, c[0]);
            Assertions.assertEquals(serializedSql,
                    AstToSQLBuilder.toSQL(SqlParser.parseSingleStatement(serializedSql, SqlModeHelper.MODE_DEFAULT)),
                    c[0]);
        }
    }

    @Test
    public void testInsertValuesDigestUnaffected() {
        // The digest form stays unparenthesized and keeps only the first row.
        StatementBase stmt = SqlParser.parseSingleStatement(
                "insert into t0 (v1, v2) values (1, 111), (2, 222)", SqlModeHelper.MODE_DEFAULT);
        Assertions.assertEquals("INSERT INTO `t0` (`v1`,`v2`) VALUES(?, ?)", AstToSQLBuilder.toDigest(stmt));
    }

    /** Deparses in the production shape: the deparser is only used on an analyzed AST. */
    private static String deparseAnalyzed(String sql) {
        StatementBase stmt = SqlParser.parse(sql, AnalyzeTestUtil.getConnectContext().getSessionVariable()).get(0);
        Analyzer.analyze(stmt, AnalyzeTestUtil.getConnectContext());
        return AstToSQLBuilder.toSQL(stmt);
    }

    /** What a view persists is the deparsed text, so that text must analyze again. */
    private static void assertReanalyzable(String sql) {
        String out = deparseAnalyzed(sql);
        StatementBase again = SqlParser.parse(out, AnalyzeTestUtil.getConnectContext().getSessionVariable()).get(0);
        Assertions.assertDoesNotThrow(() -> Analyzer.analyze(again, AnalyzeTestUtil.getConnectContext()),
                () -> "deparsed form no longer analyzes: " + out);
    }

    @Test
    public void testUntypedArrayLiteralKeepsNoTypePrefix() {
        // ARRAY<NULL> has no printable form, so the prefix used to come out as the BOOLEAN stand-in. That
        // froze a type the literal never had, and the frozen type then failed function overload matching.
        Assertions.assertTrue(deparseAnalyzed("select [NULL] from t0").contains("[NULL]"));
        Assertions.assertFalse(deparseAnalyzed("select [NULL] from t0").contains("ARRAY<BOOLEAN>"));
        assertReanalyzable("select array_contains_all(v3, [NULL]) from tarray");

        // A literal that does carry an element type still prints it, or the type would be lost on the
        // way back in.
        Assertions.assertTrue(deparseAnalyzed("select [1, 2] from t0").contains("ARRAY<TINYINT>[1, 2]"));
        assertReanalyzable("select [1, 2] from t0");
        assertReanalyzable("select array_length([]) from t0");
    }
    @Test
    public void testTemporaryPartitionQualifierIsPreserved() throws Exception {
        // Temporary and formal partitions are separate namespaces. Dropping TEMPORARY on the way out
        // makes the text name a different partition, and a view stores exactly this text: creating a
        // view over a temporary partition used to succeed and then never resolve again.
        AnalyzeTestUtil.getStarRocksAssert().withTable("CREATE TABLE tp_tbl (k date, v int)\n"
                + "DUPLICATE KEY(k)\n"
                + "PARTITION BY RANGE(k) (PARTITION p1 VALUES LESS THAN ('2020-01-01'))\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
        AnalyzeTestUtil.getStarRocksAssert()
                .ddl("ALTER TABLE tp_tbl ADD TEMPORARY PARTITION tp1 VALUES LESS THAN ('2020-01-01')");

        String temp = AstToSQLBuilder.toSQL(
                SqlParser.parseSingleStatement("select * from tp_tbl temporary partition(tp1)",
                        SqlModeHelper.MODE_DEFAULT));
        String formal = AstToSQLBuilder.toSQL(
                SqlParser.parseSingleStatement("select * from tp_tbl partition(p1)",
                        SqlModeHelper.MODE_DEFAULT));
        Assertions.assertTrue(temp.contains("TEMPORARY PARTITION (`tp1`)"), temp);
        Assertions.assertFalse(formal.contains("TEMPORARY"), formal);

        // The INSERT target carries the same qualifier, and losing it would redirect the write.
        String insert = AstToSQLBuilder.toSQL(
                SqlParser.parseSingleStatement("insert into tp_tbl temporary partition(tp1) select * from tp_tbl",
                        SqlModeHelper.MODE_DEFAULT));
        Assertions.assertTrue(insert.contains("TEMPORARY PARTITION (tp1)"), insert);

        // Round trip: the serialized form must still resolve to the temporary partition.
        StatementBase again = SqlParser.parse(temp, AnalyzeTestUtil.getConnectContext().getSessionVariable()).get(0);
        Assertions.assertDoesNotThrow(() -> Analyzer.analyze(again, AnalyzeTestUtil.getConnectContext()),
                () -> "deparsed form no longer analyzes: " + temp);
    }
}
