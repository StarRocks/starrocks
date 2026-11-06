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

package com.starrocks.connector.hive;

import com.starrocks.catalog.HiveTable;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.TypeFactory;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.starrocks.connector.hive.MockedRemoteFileSystem.HDFS_HIVE_TABLE;

public class HiveUtilsTest {
    @Test
    public void testDefaultStagingDirStaysUnderViewFsTable(@Mocked HiveTable table) {
        String location = "viewfs://cluster/warehouse/db/table";
        new Expectations() {
            {
                table.getTableLocation();
                result = location;
            }
        };

        String stagingDir = new SessionVariable().getHiveTempStagingDir();
        Path first = new Path(HiveUtils.getStagingDir(table, stagingDir));
        Path second = new Path(HiveUtils.getStagingDir(table, stagingDir));
        Assertions.assertEquals(new Path(location, ".sr_staging"), first.getParent());
        Assertions.assertEquals(first.getParent(), second.getParent());
        Assertions.assertNotEquals(first, second);
        Assertions.assertDoesNotThrow(() -> UUID.fromString(first.getName()));
    }

    @Test
    public void testExplicitAbsoluteStagingDir(@Mocked HiveTable table) {
        new Expectations() {
            {
                table.getTableLocation();
                result = "hdfs://namenode/warehouse/db/table";
            }
        };

        Path stagingDir = new Path(HiveUtils.getStagingDir(table, "/tmp/starrocks"));
        Assertions.assertEquals(new Path("hdfs://namenode/tmp/starrocks"), stagingDir.getParent());
    }

    @Test
    public void testS3DoesNotUseStagingDir(@Mocked HiveTable table) {
        new Expectations() {
            {
                table.getTableLocation();
                result = "s3://bucket/warehouse/db/table";
            }
        };

        Assertions.assertEquals("s3://bucket/warehouse/db/table/",
                HiveUtils.getStagingDir(table, new SessionVariable().getHiveTempStagingDir()));
    }

    @Test
    public void testIsS3Url() {
        Assertions.assertTrue(HiveUtils.isS3Url("obs://"));
    }

    @Test
    public void checkLocationProp() {
        Map<String, String> conf = new HashMap<>();
        conf.put("external_location", "xxx");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Can't create non-managed Hive table. Only supports creating hive table under Database location. " +
                        "You could execute command without external_location properties",
                () -> HiveUtils.checkLocationProperties(conf));
    }

    @Test
    public void testPathExists() {
        Path path = new Path("hdfs://127.0.0.1:9000/user/hive/warehouse/db");
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Failed to check path",
                () -> HiveUtils.pathExists(path, new Configuration()));

        new MockUp<FileSystem>() {
            @Mock
            public FileSystem get(URI uri, Configuration conf) {
                return new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
            }
        };
        Assertions.assertFalse(HiveUtils.pathExists(path, new Configuration()));
    }

    @Test
    public void testIsDirectory() {
        Path path = new Path("hdfs://127.0.0.1:9000/user/hive/warehouse/db");
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Failed checking path",
                () -> HiveUtils.isDirectory(path, new Configuration()));

        new MockUp<FileSystem>() {
            @Mock
            public FileSystem get(URI uri, Configuration conf) {
                return new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
            }
        };
        Assertions.assertFalse(HiveUtils.isDirectory(path, new Configuration()));
    }

    @Test
    public void testCreateDirectory() {
        Path path = new Path("hdfs://127.0.0.1:9000/user/hive/warehouse/db");
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Failed to create directory",
                () -> HiveUtils.createDirectory(path, new Configuration()));

        new MockUp<FileSystem>() {
            @Mock
            public FileSystem get(URI uri, Configuration conf) {
                return new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
            }
        };
        
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Failed to create directory",
                () -> HiveUtils.createDirectory(path, new Configuration()));
    }

    @Test
    public void testNormalizeKeyDecimal() throws Exception {
        ScalarType type = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 2);
        DecimalLiteral decimalLiteral = new DecimalLiteral("1.235", type);
        LiteralExpr normalized = HiveUtils.normalizeKey(decimalLiteral, type);
        Assertions.assertTrue(normalized instanceof DecimalLiteral);
        Assertions.assertEquals("1.24", ((DecimalLiteral) normalized).getValue().toPlainString());
    }

    @Test
    public void testNormalizeKeyNonDecimal() {
        LiteralExpr literal = new IntLiteral(5);
        Assertions.assertSame(literal, HiveUtils.normalizeKey(literal, literal.getType()));
    }
}
