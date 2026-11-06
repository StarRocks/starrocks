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


package com.starrocks.connector.iceberg;

import com.google.common.collect.ImmutableList;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.glue.IcebergGlueCatalog;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergGlueCatalogTest {
    public static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testListAllDatabases(@Mocked GlueCatalog glueCatalog) {
        new Expectations() {
            {
                glueCatalog.listNamespaces();
                result = ImmutableList.of(Namespace.of("db1"), Namespace.of("db2"));
                times = 1;
            }
        };

        Map<String, String> icebergProperties = new HashMap<>();
        IcebergGlueCatalog icebergGlueCatalog = new IcebergGlueCatalog(
                "glue_native_catalog", new Configuration(), icebergProperties);
        List<String> dbs = icebergGlueCatalog.listAllDatabases(connectContext);
        Assertions.assertEquals(Arrays.asList("db1", "db2"), dbs);
    }

    @Test
    public void testTableExists(@Mocked GlueCatalog glueCatalog) {
        new Expectations() {
            {
                glueCatalog.tableExists((TableIdentifier) any);
                result = true;
            }
        };
        Map<String, String> icebergProperties = new HashMap<>();
        IcebergGlueCatalog icebergGlueCatalog = new IcebergGlueCatalog(
                "glue_native_catalog", new Configuration(), icebergProperties);
        Assertions.assertTrue(icebergGlueCatalog.tableExists(connectContext, "db1", "tbl1"));
    }

    @Test
    public void testRenameTable(@Mocked GlueCatalog glueCatalog) {
        new Expectations() {
            {
                glueCatalog.tableExists((TableIdentifier) any);
                result = true;
            }
        };
        Map<String, String> icebergProperties = new HashMap<>();
        IcebergGlueCatalog icebergGlueCatalog = new IcebergGlueCatalog(
                "glue_native_catalog", new Configuration(), icebergProperties);
        icebergGlueCatalog.renameTable(connectContext, "db", "tb1", "tb2");
        boolean exists = icebergGlueCatalog.tableExists(connectContext, "db", "tbl2");
        Assertions.assertTrue(exists);
    }

    @Test
    public void testCreateDBWithInvalidLocationURI(@Mocked GlueCatalog glueCatalog) {
        Map<String, String> icebergProperties = new HashMap<>();
        IcebergGlueCatalog icebergGlueCatalog = new IcebergGlueCatalog(
                "glue_native_catalog", new Configuration(), icebergProperties);

        // An unsupported scheme causes FileSystem.get() to throw, which should surface via fromExternalException
        Map<String, String> dbProperties = new HashMap<>();
        dbProperties.put("location", "unsupportedscheme://bucket/path");
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Invalid location URI: unsupportedscheme://bucket/path",
                () -> icebergGlueCatalog.createDB(connectContext, "testdb", dbProperties));
    }

    @Test
    public void testDropDBWhenGetDBThrows(@Mocked GlueCatalog glueCatalog) {
        new Expectations() {
            {
                glueCatalog.loadNamespaceMetadata((Namespace) any);
                result = new RuntimeException("Glue access denied");
            }
        };
        Map<String, String> icebergProperties = new HashMap<>();
        IcebergGlueCatalog icebergGlueCatalog = new IcebergGlueCatalog(
                "glue_native_catalog", new Configuration(), icebergProperties);

        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Failed to access database testdb",
                () -> icebergGlueCatalog.dropDB(connectContext, "testdb"));
    }

    // Captures the properties map that IcebergGlueCatalog passes down to Iceberg's GlueCatalog.initialize().
    private Map<String, String> capturePropertiesPassedToGlue(GlueCatalog glueCatalog, Map<String, String> inputProperties) {
        new IcebergGlueCatalog("glue_native_catalog", new Configuration(), inputProperties);
        List<Map<String, String>> captured = new ArrayList<>();
        new Verifications() {
            {
                glueCatalog.initialize(anyString, withCapture(captured));
            }
        };
        Assertions.assertFalse(captured.isEmpty(), "GlueCatalog.initialize should have been invoked");
        return captured.get(captured.size() - 1);
    }

    @Test
    public void testGlueCatalogIdIsTranslatedToGlueId(@Mocked GlueCatalog glueCatalog) {
        // "aws.glue.catalog_id" (StarRocks property) must be forwarded as Iceberg's native "glue.id"
        // so that cross-account Glue Data Catalog access actually reaches the remote account.
        Map<String, String> icebergProperties = new HashMap<>();
        icebergProperties.put(CloudConfigurationConstants.AWS_GLUE_CATALOG_ID, "123456789012");

        Map<String, String> passed = capturePropertiesPassedToGlue(glueCatalog, icebergProperties);
        Assertions.assertEquals("123456789012", passed.get(AwsProperties.GLUE_CATALOG_ID));
    }

    @Test
    public void testExplicitGlueIdTakesPrecedence(@Mocked GlueCatalog glueCatalog) {
        // If the user sets the native "glue.id" explicitly, it must win over "aws.glue.catalog_id".
        Map<String, String> icebergProperties = new HashMap<>();
        icebergProperties.put(CloudConfigurationConstants.AWS_GLUE_CATALOG_ID, "111111111111");
        icebergProperties.put(AwsProperties.GLUE_CATALOG_ID, "222222222222");

        Map<String, String> passed = capturePropertiesPassedToGlue(glueCatalog, icebergProperties);
        Assertions.assertEquals("222222222222", passed.get(AwsProperties.GLUE_CATALOG_ID));
    }

    @Test
    public void testGlueIdAbsentWhenCatalogIdNotSet(@Mocked GlueCatalog glueCatalog) {
        // Without "aws.glue.catalog_id", StarRocks must not inject a "glue.id", so Iceberg keeps
        // defaulting to the caller's own account.
        Map<String, String> passed = capturePropertiesPassedToGlue(glueCatalog, new HashMap<>());
        Assertions.assertFalse(passed.containsKey(AwsProperties.GLUE_CATALOG_ID));
    }
}
