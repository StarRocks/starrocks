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
    public void testCreateTableAsSelect() {
        // CTAS used to have no deparse visitor and fell through to visitNode() which returns an empty
        // string, so the profile and audit log of a CTAS in a multi-statement request showed no SQL.
        String[][] cases = {
                {"CREATE TABLE t1 AS SELECT v1, v2 FROM t0",
                        "CREATE TABLE `t1` AS SELECT `v1`, `v2`\nFROM `t0`"},
                {"CREATE TABLE IF NOT EXISTS db1.t1 (c1, c2) COMMENT \"test ctas\" " +
                        "DISTRIBUTED BY HASH(c1) BUCKETS 8 " +
                        "PROPERTIES('replication_num'='1') AS SELECT v1, v2 FROM t0 WHERE v1 > 1",
                        "CREATE TABLE IF NOT EXISTS `db1`.`t1` (`c1`,`c2`) COMMENT \"test ctas\" " +
                                "DISTRIBUTED BY HASH(c1) BUCKETS 8 " +
                                "PROPERTIES (\"replication_num\" = \"1\") AS SELECT `v1`, `v2`\nFROM `t0`\nWHERE `v1` > 1"},
                {"CREATE TEMPORARY TABLE t2 AS SELECT v1 FROM t0",
                        "CREATE TEMPORARY TABLE `t2` AS SELECT `v1`\nFROM `t0`"},
                {"CREATE TABLE t3 PRIMARY KEY (c1) DISTRIBUTED BY HASH(c1) AS SELECT v1 AS c1 FROM t0",
                        "CREATE TABLE `t3` PRIMARY KEY(`c1`) DISTRIBUTED BY HASH(c1) AS SELECT `v1` AS `c1`\nFROM `t0`"},
                // Automatic partitioning: the LIST keyword must not appear, or the output re-parses
                // as explicit list partitioning (a different table).
                {"CREATE TABLE t4 PARTITION BY (dt) AS SELECT dt, v1 FROM t0",
                        "CREATE TABLE `t4` PARTITION BY (`dt`) AS SELECT `dt`, `v1`\nFROM `t0`"},
                // An explicit LIST clause is folded into a RangePartitionDesc by AstBuilder#visitPartitionDesc
                // (the LIST/RANGE branch), dropping the list definitions, so the deparse reflects that AST.
                {"CREATE TABLE t4 PARTITION BY LIST(dt) (PARTITION p1 VALUES IN ('2021-01-01')) " +
                        "DISTRIBUTED BY HASH(dt) AS SELECT dt FROM t0",
                        "CREATE TABLE `t4` PARTITION BY RANGE(`dt`) () DISTRIBUTED BY HASH(dt) AS SELECT `dt`\nFROM `t0`"},
                {"CREATE TABLE t4 PARTITION BY date_trunc('day', dt) AS SELECT dt, v1 FROM t0",
                        "CREATE TABLE `t4` PARTITION BY date_trunc('day', `dt`) AS SELECT `dt`, `v1`\nFROM `t0`"},
                // The grammar requires parentheses after RANGE(cols), so an empty pair is kept.
                {"CREATE TABLE t4 PARTITION BY RANGE(dt) " +
                        "(START ('2021-01-01') END ('2021-01-10') EVERY (INTERVAL 1 DAY)) " +
                        "DISTRIBUTED BY HASH(dt) AS SELECT dt FROM t0",
                        "CREATE TABLE `t4` PARTITION BY RANGE(`dt`) () DISTRIBUTED BY HASH(dt) AS SELECT `dt`\nFROM `t0`"},
                {"CREATE TABLE t5 ORDER BY (v1) AS SELECT v1, v2 FROM t0",
                        "CREATE TABLE `t5` ORDER BY (`v1`) AS SELECT `v1`, `v2`\nFROM `t0`"},
                {"CREATE TABLE t7 (c1, c2, INDEX idx1 (c1) USING BITMAP) AS SELECT v1, v2 FROM t0",
                        "CREATE TABLE `t7` (`c1`,`c2`,INDEX idx1 (`c1`) USING BITMAP COMMENT '') " +
                                "AS SELECT `v1`, `v2`\nFROM `t0`"},
                // Index definitions alone must not drop the parenthesized clause.
                {"CREATE TABLE t8 (INDEX idx1 (c1) USING BITMAP) AS SELECT v1 AS c1 FROM t0",
                        "CREATE TABLE `t8` (INDEX idx1 (`c1`) USING BITMAP COMMENT '') AS SELECT `v1` AS `c1`\nFROM `t0`"},
                // A double quote inside the comment must be escaped to keep the output legal SQL.
                {"CREATE TABLE t9 COMMENT 'say \"hello\"' AS SELECT v1 FROM t0",
                        "CREATE TABLE `t9` COMMENT \"say \\\"hello\\\"\" AS SELECT `v1`\nFROM `t0`"},
        };
        for (String[] c : cases) {
            StatementBase stmt = SqlParser.parseSingleStatement(c[0], SqlModeHelper.MODE_DEFAULT);
            String serializedSql = AstToSQLBuilder.toSQL(stmt);
            Assertions.assertEquals(c[1], serializedSql, c[0]);
            Assertions.assertFalse(AstToStringBuilder.toString(stmt).isEmpty(), c[0]);
            // Regression: the fallback path used to hand out the visitor's empty string as-is.
            Assertions.assertEquals(c[1], AstToSQLBuilder.toSQLOrDefault(stmt, c[0]), c[0]);
            // The deparsed form must stay legal SQL even where partition definitions are omitted.
            Assertions.assertDoesNotThrow(() -> SqlParser.parseSingleStatement(serializedSql, SqlModeHelper.MODE_DEFAULT),
                    c[0]);
        }
    }

    @Test
    public void testCreateTableAsSelectHidesCredentials() {
        String sql = "CREATE TABLE t6 PROPERTIES ('aws.s3.access_key'='abc', 'aws.s3.secret_key'='def') " +
                "AS SELECT v1 FROM t0";
        StatementBase stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertEquals(
                "CREATE TABLE `t6` PROPERTIES (\"aws.s3.access_key\" = \"***\", \"aws.s3.secret_key\" = \"***\") " +
                        "AS SELECT `v1`\nFROM `t0`",
                AstToSQLBuilder.toSQL(stmt));
    }
}
