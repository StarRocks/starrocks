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

import com.starrocks.server.SharedNothingStorageVolumeMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.AlterStorageVolumeStmt;
import com.starrocks.sql.ast.CreateStorageVolumeStmt;
import com.starrocks.sql.ast.DescStorageVolumeStmt;
import com.starrocks.sql.ast.DropStorageVolumeStmt;
import com.starrocks.sql.ast.SetDefaultStorageVolumeStmt;
import com.starrocks.sql.ast.ShowStorageVolumesStmt;
import com.starrocks.sql.ast.StatementBase;
import mockit.Mock;
import mockit.MockUp;
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
        String sql = "CREATE STORAGE VOLUME storage_volume_1 type = s3 " +
                "LOCATIONS = ('s3://xxx', 's3://yyy') PROPERTIES (\"aws.s3.region\"=\"us-west-2\")";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof CreateStorageVolumeStmt);
        Assert.assertEquals("CREATE STORAGE VOLUME storage_volume_1 TYPE = s3 " +
                        "LOCATIONS = ('s3://xxx', 's3://yyy') PROPERTIES (\"aws.s3.region\" = \"us-west-2\")",
                stmt.toSql());

        sql = "CREATE STORAGE VOLUME IF NOT EXISTS storage_volume_1 type = s3 "  +
                "LOCATIONS = ('s3://xxx') COMMENT 'comment' PROPERTIES (\"aws.s3.endpoint\"=\"endpoint\", " +
                "\"aws.s3.region\"=\"us-west-2\", \"enabled\"=\"false\")";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof CreateStorageVolumeStmt);
        Assert.assertEquals("CREATE STORAGE VOLUME IF NOT EXISTS storage_volume_1 " +
                "TYPE = s3 LOCATIONS = ('s3://xxx') COMMENT 'comment' PROPERTIES ("
                + "\"aws.s3.endpoint\" = \"endpoint\", \"aws.s3.region\" = \"us-west-2\", \"enabled\" = \"false\")",
                stmt.toSql());

        sql = "CREATE STORAGE VOLUME IF NOT EXISTS storage_volume_1 type = s3 "  +
                "LOCATIONS = ('') COMMENT 'comment' PROPERTIES (\"aws.s3.endpoint\"=\"endpoint\", " +
                "\"aws.s3.region\"=\"us-west-2\", \"enabled\"=\"false\")";
        AnalyzeTestUtil.analyzeFail(sql, "'location' field is required to create the storage volume");

        sql = "CREATE STORAGE VOLUME IF NOT EXISTS builtin_storage_volume type = s3 "  +
                "LOCATIONS = ('') COMMENT 'comment' PROPERTIES (\"aws.s3.endpoint\"=\"endpoint\", " +
                "\"aws.s3.region\"=\"us-west-2\", \"enabled\"=\"false\")";
        AnalyzeTestUtil.analyzeFail(sql,
                "builtin_storage_volume is a reserved storage volume name, please choose a different name for the storage volume");
    }

    @Test
    public void testAlterStorageVolumeParserAndAnalyzer() {
        String sql = "ALTER STORAGE VOLUME storage_volume_1";
        AnalyzeTestUtil.analyzeFail(sql, "Unexpected input '<EOF>', the most similar input is {'SET', 'COMMENT'}");

        sql = "ALTER STORAGE VOLUME storage_volume_1 COMMENT = 'comment', " +
                "SET (\"aws.s3.region\"=\"us-west-2\", \"enabled\"=\"false\")";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof AlterStorageVolumeStmt);
        Assert.assertEquals("ALTER STORAGE VOLUME storage_volume_1 COMMENT = 'comment' SET " +
                "(\"aws.s3.region\" = \"us-west-2\", \"enabled\" = \"false\")", stmt.toSql());

        sql = "ALTER STORAGE VOLUME storage_volume_1 COMMENT = 'comment'";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof AlterStorageVolumeStmt);
        Assert.assertEquals("ALTER STORAGE VOLUME storage_volume_1 COMMENT = 'comment'",
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
        new MockUp<SharedNothingStorageVolumeMgr>() {
            @Mock
            public boolean exists(String svKey) {
                if (svKey.equals("storage_volume1")) {
                    return true;
                }
                return false;
            }
        };
        String sql = "DESC STORAGE VOLUME storage_volume1";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof DescStorageVolumeStmt);
        Assert.assertEquals("DESC STORAGE VOLUME storage_volume1", stmt.toSql());

        sql = "DESCRIBE STORAGE VOLUME storage_volume1";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof DescStorageVolumeStmt);
        Assert.assertEquals("DESC STORAGE VOLUME storage_volume1", stmt.toSql());

        sql = "DESC STORAGE VOLUME storage_volume2";
        AnalyzeTestUtil.analyzeFail(sql, "Unknown storage volume: storage_volume2");
    }

    @Test
    public void testSetDefaultStorageVolumeParserAndAnalyzer() {
        String sql = "SET storage_volume1 AS DEFAULT STORAGE VOLUME";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof SetDefaultStorageVolumeStmt);
        Assert.assertEquals("SET storage_volume1 AS DEFAULT STORAGE VOLUME", stmt.toSql());
    }
}
