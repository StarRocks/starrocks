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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergView;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.ConnectorViewDefinition;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.rest.IcebergRESTCatalog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.ColWithComment;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static com.starrocks.catalog.Table.TableType.ICEBERG_VIEW;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CATALOG_TYPE;
import static com.starrocks.connector.iceberg.rest.IcebergRESTCatalog.ICEBERG_CATALOG_SECURITY;
import static com.starrocks.type.IntegerType.INT;
import static org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IcebergRESTCatalogTest {
    private static final String CATALOG_NAME = "iceberg_rest_catalog";
    public static final Map<String, String> DEFAULT_CONFIG = new HashMap<>();
    public static final IcebergCatalogProperties DEFAULT_CATALOG_PROPERTIES;

    public static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment();
    public static ConnectContext connectContext;

    static {
        DEFAULT_CONFIG.put(ICEBERG_CATALOG_TYPE, "rest");
        DEFAULT_CATALOG_PROPERTIES = new IcebergCatalogProperties(DEFAULT_CONFIG);
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        AnalyzeTestUtil.init();
    }

    public IcebergMetadata buildIcebergMetadata(RESTSessionCatalog restCatalog) {
        IcebergRESTCatalog icebergRESTCatalog = new IcebergRESTCatalog(restCatalog, new Configuration());
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(
                CATALOG_NAME, icebergRESTCatalog, DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());

        return new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, cachingIcebergCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(),
                new IcebergCatalogProperties(DEFAULT_CONFIG));
    }

    @Test
    public void testListAllDatabasesWithException(@Mocked RESTSessionCatalog restCatalog) {
        new Expectations() {
            {
                restCatalog.listNamespaces((SessionContext) any);
                result = new StarRocksConnectorException("Failed to list namespaces");
                times = 1;
            }
        };

        Map<String, String> icebergProperties = new HashMap<>();
        final IcebergRESTCatalog icebergRESTCatalog = new IcebergRESTCatalog(
                "rest_native_catalog", new Configuration(), icebergProperties);
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class, "Failed to list namespaces",
                () -> icebergRESTCatalog.listAllDatabases(connectContext));

        new Expectations() {
            {
                restCatalog.listNamespaces((SessionContext) any, Namespace.empty());
                result = new StarRocksConnectorException("Failed to list namespaces");
                times = 1;
            }
        };

        icebergProperties = ImmutableMap.of(
                "iceberg.catalog.rest.nested-namespace-enabled", "true");
        final IcebergRESTCatalog icebergRESTCatalog2 = new IcebergRESTCatalog(
                "rest_native_catalog", new Configuration(), icebergProperties);

        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class, "Failed to list namespaces",
                () -> icebergRESTCatalog2.listAllDatabases(connectContext));
    }

    @Test
    public void testListAllDatabases(@Mocked RESTSessionCatalog restCatalog) {
        new Expectations() {
            {
                restCatalog.listNamespaces((SessionContext) any, Namespace.empty());
                result = ImmutableList.of(Namespace.of("db1"));
                times = 1;

                restCatalog.listNamespaces((SessionContext) any, Namespace.of("db1"));
                result = ImmutableList.of(Namespace.of("db1", "ns1"));
                times = 1;

                restCatalog.listNamespaces((SessionContext) any, Namespace.of("db1", "ns1"));
                result = ImmutableList.of(Namespace.of("db1", "ns1", "ns2"));
                times = 1;
            }
        };

        Map<String, String> icebergProperties = ImmutableMap.of(
                "iceberg.catalog.rest.nested-namespace-enabled", "true");
        IcebergRESTCatalog icebergRESTCatalog = new IcebergRESTCatalog(
                "rest_native_catalog", new Configuration(), icebergProperties);
        List<String> dbs = icebergRESTCatalog.listAllDatabases(connectContext);
        Assertions.assertEquals(Arrays.asList("db1", "db1.ns1", "db1.ns1.ns2"), dbs);
    }

    @Test
    public void testTableExists(@Mocked RESTSessionCatalog restCatalog) {
        new Expectations() {
            {
                restCatalog.tableExists((SessionContext) any, (TableIdentifier) any);
                result = true;
            }
        };
        IcebergRESTCatalog icebergRESTCatalog = new IcebergRESTCatalog(
                "rest_native_catalog", new Configuration(), new HashMap<>());
        boolean exists = icebergRESTCatalog.tableExists(connectContext, "db1", "tbl1");
        Assertions.assertTrue(exists);
    }

    @Test
    public void testRenameTable(@Mocked RESTSessionCatalog restCatalog) {
        new Expectations() {
            {
                restCatalog.tableExists((SessionContext) any, (TableIdentifier) any);
                result = true;
            }
        };
        IcebergRESTCatalog icebergRESTCatalog = new IcebergRESTCatalog(
                "rest_native_catalog", new Configuration(), new HashMap<>());
        icebergRESTCatalog.renameTable(connectContext, "db", "tb1", "tb2");
        boolean exists = icebergRESTCatalog.tableExists(connectContext, "db", "tbl2");
        Assertions.assertTrue(exists);
    }

    @Test
    public void testShowTableVies(@Mocked RESTSessionCatalog restCatalog) {
        IcebergMetadata metadata = buildIcebergMetadata(restCatalog);

        new Expectations() {
            {
                restCatalog.listTables((SessionContext) any, (Namespace) any);
                result = ImmutableList.of(TableIdentifier.of("db", "tbl1"));
                minTimes = 1;

                restCatalog.listViews((SessionContext) any, (Namespace) any);
                result = ImmutableList.of(TableIdentifier.of("db", "view1"));
                minTimes = 1;
            }
        };

        List<String> tables = metadata.listTableNames(new ConnectContext(), "db");
        Assertions.assertEquals(2, tables.size());
        Assertions.assertEquals(tables, Lists.newArrayList("tbl1", "view1"));
    }

    @Test
    public void testDropView(@Mocked RESTSessionCatalog restCatalog) {
        IcebergMetadata metadata = buildIcebergMetadata(restCatalog);
        new MockUp<IcebergMetadata>() {
            @Mock
            Table getTable(ConnectContext context, String dbName, String tblName) {
                return new IcebergView(1, "iceberg_rest_catalog", "db", "view",
                        Lists.newArrayList(), "mocked", "iceberg_rest_catalog", "db",
                        "location", Maps.newHashMap());
            }
        };

        new Expectations() {
            {
                restCatalog.dropView((SessionContext) any, (TableIdentifier) any);
                result = true;
                minTimes = 1;
            }
        };

        metadata.dropTable(connectContext, new DropTableStmt(false, new TableName("catalog", "db", "view"),
                false));
    }

    @Test
    public void testCreateView(@Mocked RESTSessionCatalog restCatalog, @Mocked BaseView baseView,
                               @Mocked ImmutableSQLViewRepresentation representation) throws Exception {
        IcebergMetadata metadata = buildIcebergMetadata(restCatalog);

        CreateViewStmt stmt = new CreateViewStmt(false, false, new TableName("catalog", "db", "table"),
                Lists.newArrayList(new ColWithComment("k1", "", NodePosition.ZERO)), "", false, null, NodePosition.ZERO);
        stmt.setColumns(Lists.newArrayList(new Column("k1", INT)));
        metadata.createView(connectContext, stmt);

        new Expectations() {
            {
                representation.sql();
                result = "select * from table";
                minTimes = 1;

                baseView.sqlFor("starrocks");
                result = representation;
                minTimes = 1;

                baseView.properties();
                result = ImmutableMap.of("comment", "mocked");
                minTimes = 1;

                baseView.schema();
                result = new Schema(Types.NestedField.optional(1, "k1", Types.IntegerType.get()));
                minTimes = 1;

                baseView.name();
                result = "view";
                minTimes = 1;

                baseView.location();
                result = null;
                minTimes = 1;

                restCatalog.loadView((SessionContext) any, TableIdentifier.of("db", "view"));
                result = baseView;
                minTimes = 1;
            }
        };

        Table table = metadata.getView(connectContext, "db", "view");
        Assertions.assertEquals(ICEBERG_VIEW, table.getType());
        Assertions.assertNull(table.getTableLocation());
    }

    @Test
    public void testCatalogOperationsWithException(@Mocked RESTSessionCatalog restCatalog) {
        IcebergMetadata metadata = buildIcebergMetadata(restCatalog);

        new Expectations() {
            {
                restCatalog.listNamespaces((SessionContext) any);
                result = new StarRocksConnectorException("Failed to list all namespaces using REST Catalog",
                        new RuntimeException("Failed to rename view using REST Catalog, exception:"));
                minTimes = 1;

                restCatalog.listTables((SessionContext) any, (Namespace) any);
                result = new StarRocksConnectorException("Failed to list tables using REST Catalog",
                        new RuntimeException("Failed to list tables using REST Catalog, exception:"));
                minTimes = 1;
            }
        };
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Failed to list all namespaces using REST Catalog",
                () -> metadata.listDbNames(new ConnectContext()));
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Failed to list tables using REST Catalog",
                () -> metadata.listTableNames(new ConnectContext(), "db"));

        new Expectations() {
            {
                restCatalog.listTables((SessionContext) any, (Namespace) any);
                result = ImmutableList.of(TableIdentifier.of(Namespace.of("db"), "tbl1"));
                minTimes = 1;

                restCatalog.listViews((SessionContext) any, (Namespace) any);
                result = new StarRocksConnectorException("Failed to list views using REST Catalog",
                        new RuntimeException("Failed to list views using REST Catalog, exception:"));
                minTimes = 1;
            }
        };

        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Failed to list views using REST Catalog",
                () -> metadata.listTableNames(new ConnectContext(), "db"));


        new Expectations() {
            {
                restCatalog.listNamespaces((SessionContext) any);
                result = ImmutableList.of(Namespace.of("db1"));
                minTimes = 1;

                restCatalog.createNamespace((SessionContext) any, (Namespace) any, (Map<String, String>) any);
                result = new StarRocksConnectorException("Failed to create namespace using REST Catalog",
                        new RuntimeException("Failed to create namespace using REST Catalog, exception:"));
                minTimes = 1;
            }
        };

        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Failed to create namespace using REST Catalog",
                () -> metadata.createDb(connectContext, "db2", Map.of()));

        new Expectations() {
            {
                restCatalog.buildTable((SessionContext) any, (TableIdentifier) any, (Schema) any);
                result = new StarRocksConnectorException("Failed to create table using REST Catalog",
                        new RuntimeException("Failed to create table using REST Catalog, exception:"));
                minTimes = 1;
            }
        };

        IcebergRESTCatalog icebergRESTCatalog = new IcebergRESTCatalog(restCatalog, new Configuration());
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Failed to create table using REST Catalog",
                () -> icebergRESTCatalog.createTable(connectContext, "db", "tbl", null, null, null,
                        null, Maps.newHashMap()));

        new Expectations() {
            {
                restCatalog.buildView((SessionContext) any, (TableIdentifier) any);
                result = new StarRocksConnectorException("Failed to create view using REST Catalog",
                        new RuntimeException("Failed to create view using REST Catalog, exception:"));
                minTimes = 1;
            }
        };
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Failed to create view using REST Catalog",
                () -> icebergRESTCatalog.createView(connectContext, "catalog", new ConnectorViewDefinition(
                        "catalog", "db", "view", "comment",
                        Lists.newArrayList(new Column("k1", INT)), "select * from t",
                        AlterViewStmt.AlterDialectType.NONE, Maps.newHashMap()), false));
    }

    @Test
    public void testBuildContextWithJwtSecurity(@Mocked RESTSessionCatalog restCatalog) {
        // Test when security is set to JWT and auth token is provided
        Map<String, String> properties = new HashMap<>();
        properties.put(ICEBERG_CATALOG_SECURITY, "JWT");

        IcebergRESTCatalog catalog = new IcebergRESTCatalog(
                "test_catalog", new Configuration(), properties);

        // Create a mock ConnectContext with auth token using MockUp
        new MockUp<ConnectContext>() {
            @Mock
            public String getQualifiedUser() {
                return "test_user";
            }

            @Mock
            public String getSessionId() {
                return "test_session";
            }

            @Mock
            public String getAuthToken() {
                return "test_token";
            }

            @Mock
            public UserIdentity getCurrentUserIdentity() {
                return new UserIdentity("test_user", "%");
            }
        };
        SessionCatalog.SessionContext sessionContext = Deencapsulation.invoke(catalog, "buildContext", connectContext);
        assertNotNull(sessionContext);
        assertTrue(sessionContext.credentials().containsKey("token"));
        assertEquals("test_token", sessionContext.credentials().get("token"));


        properties = new HashMap<>();
        properties.put(ICEBERG_CATALOG_SECURITY, "Jwt");
        catalog = new IcebergRESTCatalog(
                "test_catalog", new Configuration(), properties);
        sessionContext = Deencapsulation.invoke(catalog, "buildContext", connectContext);
        assertNotNull(sessionContext);
        assertTrue(sessionContext.credentials().containsKey("token"));
        assertEquals("test_token", sessionContext.credentials().get("token"));
    }

    @Test
    public void testListTablesWithViewEndpointsEnabled(@Mocked RESTSessionCatalog restCatalog) {
        IcebergRESTCatalog icebergRESTCatalog = new IcebergRESTCatalog(restCatalog, new Configuration());

        new Expectations() {
            {
                restCatalog.listTables((SessionContext) any, (Namespace) any);
                result = ImmutableList.of(TableIdentifier.of("db", "tbl1"));
                minTimes = 0;

                // When viewEndpointsEnabled is true (default), listViews should be called
                restCatalog.listViews((SessionContext) any, (Namespace) any);
                result = ImmutableList.of(TableIdentifier.of("db", "view1"));
                minTimes = 0;
            }
        };

        List<String> tables = icebergRESTCatalog.listTables(connectContext, "db");
        // Should include both tables and views
        Assertions.assertEquals(2, tables.size());
        Assertions.assertTrue(tables.contains("tbl1"));
        Assertions.assertTrue(tables.contains("view1"));
    }

    @Test
    public void testListTablesWithViewEndpointsDisabled(@Mocked RESTSessionCatalog restCatalog) {
        // Create catalog with viewEndpointsEnabled = false
        Map<String, String> properties = ImmutableMap.of(
                "iceberg.catalog.rest.view-endpoints-enabled", "false");
        IcebergRESTCatalog icebergRESTCatalog = new IcebergRESTCatalog(
                "test_catalog", new Configuration(), properties);

        new Expectations() {
            {
                restCatalog.listTables((SessionContext) any, (Namespace) any);
                result = ImmutableList.of(TableIdentifier.of("db", "tbl1"));
                minTimes = 0;

                // When viewEndpointsEnabled is false, listViews should NOT be called
                // So we don't set any expectations for restCatalog.listViews()
            }
        };

        List<String> tables = icebergRESTCatalog.listTables(connectContext, "db");
        // Should only include tables, not views
        Assertions.assertEquals(1, tables.size());
        Assertions.assertTrue(tables.contains("tbl1"));
    }

    @Test
    public void testGetViewWithViewEndpointsDisabled(@Mocked RESTSessionCatalog restCatalog) {
        // Create catalog with viewEndpointsEnabled = false
        Map<String, String> properties = ImmutableMap.of(
                "iceberg.catalog.rest.view-endpoints-enabled", "false");
        IcebergRESTCatalog icebergRESTCatalog = new IcebergRESTCatalog(
                "test_catalog", new Configuration(), properties);

        // When viewEndpointsEnabled is false, getView should throw exception
        StarRocksConnectorException exception = Assertions.assertThrows(
                StarRocksConnectorException.class,
                () -> icebergRESTCatalog.getView(connectContext, "db", "view"));

        Assertions.assertTrue(exception.getMessage().contains("View operations are disabled for this catalog"));
    }

    @Test
    public void testGetCatalogProperties(@Mocked RESTSessionCatalog restCatalog) {
        Map<String, String> catalogProperties = ImmutableMap.of(
                "s3.access-key-id", "AKIA_TEST_KEY",
                "s3.secret-access-key", "test_secret_key",
                "s3.session-token", "test_session_token",
                "client.region", "us-east-1"
        );

        new Expectations() {
            {
                restCatalog.properties();
                result = catalogProperties;
                times = 1;
            }
        };

        IcebergRESTCatalog icebergRESTCatalog = new IcebergRESTCatalog(restCatalog, new Configuration());
        Map<String, String> result = icebergRESTCatalog.getCatalogProperties();

        Assertions.assertEquals("AKIA_TEST_KEY", result.get("s3.access-key-id"));
        Assertions.assertEquals("test_secret_key", result.get("s3.secret-access-key"));
        Assertions.assertEquals("test_session_token", result.get("s3.session-token"));
        Assertions.assertEquals("us-east-1", result.get("client.region"));
    }

    @Test
    public void testGetCatalogPropertiesEmpty(@Mocked RESTSessionCatalog restCatalog) {
        new Expectations() {
            {
                restCatalog.properties();
                result = ImmutableMap.of();
                times = 1;
            }
        };

        IcebergRESTCatalog icebergRESTCatalog = new IcebergRESTCatalog(restCatalog, new Configuration());
        Map<String, String> result = icebergRESTCatalog.getCatalogProperties();

        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testBuildContextWithNonJwtSecurityAndOidcAuth(@Mocked RESTSessionCatalog restCatalog) {
        // Test that user's OIDC token is NOT passed to REST Catalog when security mode is NONE or OAUTH2.
        // This is a regression test for the bug where user's OIDC token was incorrectly passed
        // to REST Catalog regardless of the catalog's security configuration.

        // Create a mock ConnectContext with auth token (simulating OIDC authenticated user)
        new MockUp<ConnectContext>() {
            @Mock
            public String getQualifiedUser() {
                return "oidc_user";
            }

            @Mock
            public String getSessionId() {
                return "oidc_session";
            }

            @Mock
            public String getAuthToken() {
                return "user_oidc_access_token";
            }

            @Mock
            public UserIdentity getCurrentUserIdentity() {
                return new UserIdentity("oidc_user", "%");
            }
        };

        // Test with security=NONE (default)
        Map<String, String> propertiesNone = new HashMap<>();
        IcebergRESTCatalog catalogNone = new IcebergRESTCatalog(
                "test_catalog", new Configuration(), propertiesNone);
        SessionCatalog.SessionContext sessionContextNone =
                Deencapsulation.invoke(catalogNone, "buildContext", connectContext);
        // Should return empty context, NOT passing user's OIDC token
        // credentials() may be null for empty context, which is also acceptable
        assertTrue(sessionContextNone.credentials() == null || sessionContextNone.credentials().isEmpty(),
                "When security=NONE, user's OIDC token should NOT be passed to REST Catalog");

        // Test with explicit security=NONE
        Map<String, String> propertiesExplicitNone = new HashMap<>();
        propertiesExplicitNone.put(ICEBERG_CATALOG_SECURITY, "NONE");
        IcebergRESTCatalog catalogExplicitNone = new IcebergRESTCatalog(
                "test_catalog", new Configuration(), propertiesExplicitNone);
        SessionCatalog.SessionContext sessionContextExplicitNone =
                Deencapsulation.invoke(catalogExplicitNone, "buildContext", connectContext);
        assertTrue(sessionContextExplicitNone.credentials() == null || sessionContextExplicitNone.credentials().isEmpty(),
                "When security=NONE (explicit), user's OIDC token should NOT be passed to REST Catalog");

        // Test with security=OAUTH2 (catalog has its own OAuth2 credentials)
        Map<String, String> propertiesOauth2 = new HashMap<>();
        propertiesOauth2.put(ICEBERG_CATALOG_SECURITY, "OAUTH2");
        propertiesOauth2.put("iceberg.catalog.oauth2.credential", "catalog_client_id:catalog_client_secret");
        IcebergRESTCatalog catalogOauth2 = new IcebergRESTCatalog(
                "test_catalog", new Configuration(), propertiesOauth2);
        SessionCatalog.SessionContext sessionContextOauth2 =
                Deencapsulation.invoke(catalogOauth2, "buildContext", connectContext);
        // Should return empty context, NOT passing user's OIDC token
        // (REST Catalog should use its own oauth2.credential instead)
        assertTrue(sessionContextOauth2.credentials() == null || sessionContextOauth2.credentials().isEmpty(),
                "When security=OAUTH2, user's OIDC token should NOT be passed to REST Catalog. " +
                        "Catalog should use its own configured oauth2.credential instead");
    }
}