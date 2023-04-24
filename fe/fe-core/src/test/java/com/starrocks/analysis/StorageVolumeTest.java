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

package com.starrocks.analysis;

import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.AlterStorageVolumeStmt;
import com.starrocks.sql.ast.CreateStorageVolumeStmt;
import com.starrocks.sql.ast.DescStorageVolumeStmt;
import com.starrocks.sql.ast.DropStorageVolumeStmt;
import com.starrocks.sql.ast.ShowStorageVolumesStmt;
import com.starrocks.sql.ast.StatementBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class StorageVolumeTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testCreateStorageVolumeParserAndAnalyzer() {
        String sql = "CREATE STORAGE VOLUME storage_volume_1 type = s3 (\"aws.s3.region\"=\"us-west-2\") " +
                "LOCATIONS = ('s3://xxx', 's3://yyy')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof CreateStorageVolumeStmt);
        Assert.assertEquals("CREATE STORAGE VOLUME storage_volume_1 TYPE = s3 (\"aws.s3.region\"" +
                        " = \"us-west-2\") LOCATIONS = ('s3://xxx', 's3://yyy') ENABLED = true",
                stmt.toSql());

        sql = "CREATE STORAGE VOLUME IF NOT EXISTS storage_volume_1 type = s3 (\"aws.s3.region\"=\"us-west-2\", " +
                "\"aws.s3.endpoint\"=\"endpoint\") " +
                "LOCATIONS = ('s3://xxx') ENABLED=FALSE COMMENT 'comment'";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof CreateStorageVolumeStmt);
        Assert.assertEquals("CREATE STORAGE VOLUME IF NOT EXISTS storage_volume_1 " +
                "TYPE = s3 (\"aws.s3.endpoint\" = \"endpoint\", \"aws.s3.region\" = \"us-west-2\") " +
                "LOCATIONS = ('s3://xxx') " + "ENABLED = false COMMENT = comment", stmt.toSql());
    }

    @Test
    public void testAlterStorageVolumeParserAndAnalyzer() {
        String sql = "ALTER STORAGE VOLUME storage_volume_1 SET AS DEFAULT";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof AlterStorageVolumeStmt);
        Assert.assertEquals("ALTER STORAGE VOLUME storage_volume_1 SET AS DEFAULT", stmt.toSql());

        sql = "ALTER STORAGE VOLUME storage_volume_1 SET AS DEFAULT (\"aws.s3.region\"=\"us-west-2\") " +
                "ENABLED=FALSE COMMENT='comment'";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof AlterStorageVolumeStmt);
        Assert.assertEquals("ALTER STORAGE VOLUME storage_volume_1 SET AS DEFAULT " +
                "(\"aws.s3.region\" = \"us-west-2\") ENABLED = false COMMENT = 'comment'", stmt.toSql());

        sql = "ALTER STORAGE VOLUME storage_volume_1 SET ENABLED=FALSE COMMENT='comment'";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof AlterStorageVolumeStmt);
        Assert.assertEquals("ALTER STORAGE VOLUME storage_volume_1 SET ENABLED = false COMMENT = 'comment'",
                stmt.toSql());

        sql = "ALTER STORAGE VOLUME storage_volume_1 SET (\"aws.s3.region\"=\"us-west-2\", " +
                "\"aws.s3.endpoint\"=\"endpoint\")";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof AlterStorageVolumeStmt);
        Assert.assertEquals("ALTER STORAGE VOLUME storage_volume_1 SET (\"aws.s3.endpoint\" = \"endpoint\", " +
                        "\"aws.s3.region\" = \"us-west-2\")", stmt.toSql());
    }

    @Test
    public void testDropStorageVolumeParserAndAnalyzer() {
        String sql = "DROP STORAGE VOLUME storage_volume_1";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof DropStorageVolumeStmt);
        Assert.assertEquals("DROP STORAGE VOLUME storage_volume_1", stmt.toSql());

        sql = "DROP STORAGE VOLUME IF EXISTS storage_volume_1";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof DropStorageVolumeStmt);
        Assert.assertEquals("DROP STORAGE VOLUME IF EXISTS storage_volume_1", stmt.toSql());
    }

    @Test
    public void testShowStorageVolumesParserAndAnalyzer() {
        String sql = "SHOW STORAGE VOLUMES";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof ShowStorageVolumesStmt);
        Assert.assertEquals("SHOW STORAGE VOLUMES", stmt.toSql());

        sql = "SHOW STORAGE VOLUMES LIKE '%storage_volume%'";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof ShowStorageVolumesStmt);
        Assert.assertEquals("SHOW STORAGE VOLUMES LIKE '%storage_volume%'", stmt.toSql());
    }

    @Test
    public void testDescStorageVolumeParserAndAnalyzer() {
        String sql = "DESC STORAGE VOLUME storage_volume1";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof DescStorageVolumeStmt);
        Assert.assertEquals("DESC STORAGE VOLUME storage_volume1", stmt.toSql());

        sql = "DESCRIBE STORAGE VOLUME storage_volume1";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof DescStorageVolumeStmt);
        Assert.assertEquals("DESC STORAGE VOLUME storage_volume1", stmt.toSql());
    }
}
