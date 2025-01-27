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
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergView;
import com.starrocks.catalog.Table;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.iceberg.rest.IcebergRESTCatalog;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static com.starrocks.catalog.Table.TableType.ICEBERG_VIEW;
import static com.starrocks.catalog.Type.INT;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CATALOG_TYPE;

public class IcebergRESTCatalogTest {
    private static final String CATALOG_NAME = "iceberg_rest_catalog";
    public static final Map<String, String> DEFAULT_CONFIG = new HashMap<>();
    public static final IcebergCatalogProperties DEFAULT_CATALOG_PROPERTIES;

    public static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment();

    static {
        DEFAULT_CONFIG.put(ICEBERG_CATALOG_TYPE, "rest");
        DEFAULT_CATALOG_PROPERTIES = new IcebergCatalogProperties(DEFAULT_CONFIG);
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    public IcebergMetadata buildIcebergMetadata(RESTCatalog restCatalog) {
        IcebergRESTCatalog icebergRESTCatalog = new IcebergRESTCatalog(restCatalog, new Configuration());
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(
                CATALOG_NAME, icebergRESTCatalog, DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());

        return new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, cachingIcebergCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(),
                new IcebergCatalogProperties(DEFAULT_CONFIG));
    }

    @Test
    public void testListAllDatabases(@Mocked RESTCatalog restCatalog) {
        new Expectations() {
            {
                restCatalog.listNamespaces();
                result = ImmutableList.of(Namespace.of("db1"), Namespace.of("db2"));
                times = 1;
            }
        };

        Map<String, String> icebergProperties = new HashMap<>();
        IcebergRESTCatalog icebergRESTCatalog = new IcebergRESTCatalog(
                "rest_native_catalog", new Configuration(), icebergProperties);
        List<String> dbs = icebergRESTCatalog.listAllDatabases();
        Assert.assertEquals(Arrays.asList("db1", "db2"), dbs);
    }

    @Test
    public void testTableExists(@Mocked RESTCatalog restCatalog) {
        new Expectations() {
            {
                restCatalog.tableExists((TableIdentifier) any);
                result = true;
            }
        };
        IcebergRESTCatalog icebergRESTCatalog = new IcebergRESTCatalog(
                "rest_native_catalog", new Configuration(), new HashMap<>());
        boolean exists = icebergRESTCatalog.tableExists("db1", "tbl1");
        Assert.assertTrue(exists);
    }

    @Test
    public void testRenameTable(@Mocked RESTCatalog restCatalog) {
        new Expectations() {
            {
                restCatalog.tableExists((TableIdentifier) any);
                result = true;
            }
        };
        IcebergRESTCatalog icebergRESTCatalog = new IcebergRESTCatalog(
                "rest_native_catalog", new Configuration(), new HashMap<>());
        icebergRESTCatalog.renameTable("db", "tb1", "tb2");
        boolean exists = icebergRESTCatalog.tableExists("db", "tbl2");
        Assert.assertTrue(exists);
    }

    @Test
    public void testShowTableVies(@Mocked RESTCatalog restCatalog) {
        IcebergMetadata metadata = buildIcebergMetadata(restCatalog);

        new Expectations() {
            {
                restCatalog.listTables((Namespace) any);
                result = ImmutableList.of(TableIdentifier.of("db", "tbl1"));
                minTimes = 1;

                restCatalog.listViews((Namespace) any);
                result = ImmutableList.of(TableIdentifier.of("db", "view1"));
                minTimes = 1;
            }
        };

        List<String> tables = metadata.listTableNames("db");
        Assert.assertEquals(2, tables.size());
        Assert.assertEquals(tables, Lists.newArrayList("tbl1", "view1"));
    }

    @Test
    public void testDropView(@Mocked RESTCatalog restCatalog) {
        IcebergMetadata metadata = buildIcebergMetadata(restCatalog);
        new MockUp<IcebergMetadata>() {
            @Mock
            Table getTable(String dbName, String tblName) {
                return new IcebergView(1, "iceberg_rest_catalog", "db", "view",
                        Lists.newArrayList(), "mocked", "iceberg_rest_catalog", "db",
                        "location");
            }
        };

        new Expectations() {
            {
                restCatalog.dropView((TableIdentifier) any);
                result = true;
                minTimes = 1;
            }
        };

        metadata.dropTable(new DropTableStmt(false, new TableName("catalog", "db", "view"), false));
    }

    @Test
    public void testCreateView(@Mocked RESTCatalog restCatalog, @Mocked BaseView baseView,
                               @Mocked ImmutableSQLViewRepresentation representation) throws Exception {
        IcebergMetadata metadata = buildIcebergMetadata(restCatalog);

        new Expectations() {
            {
                restCatalog.loadNamespaceMetadata(Namespace.of("db"));
                result = ImmutableMap.of("location", "xxxxx");
                minTimes = 1;
            }
        };

        CreateViewStmt stmt = new CreateViewStmt(false, false, new TableName("catalog", "db", "table"),
                Lists.newArrayList(new ColWithComment("k1", "", NodePosition.ZERO)), "", false, null, NodePosition.ZERO);
        stmt.setColumns(Lists.newArrayList(new Column("k1", INT)));
        metadata.createView(stmt);

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
                result = "xxx";
                minTimes = 1;

                restCatalog.loadView(TableIdentifier.of("db", "view"));
                result = baseView;
                minTimes = 1;
            }
        };

        Table table = metadata.getView("db", "view");
        Assert.assertEquals(ICEBERG_VIEW, table.getType());
        Assert.assertEquals("xxx", table.getTableLocation());
    }
}
