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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AstToSQLBuilderTest {
    @BeforeClass
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
            Assert.assertEquals(
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
            Assert.assertEquals(
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
        Assert.assertEquals(
                "INSERT INTO `t0` (`v1`,`v2`) " +
                        "SELECT *\nFROM FILES(\"aws.s3.access_key\" = \"***\", \"aws.s3.region\" = \"us-west-1\", " +
                        "\"aws.s3.secret_key\" = \"***\", \"format\" = \"parquet\", \"path\" = \"s3://xxx/zzz\")",
                AstToSQLBuilder.toSQL(stmt));
    }
}
