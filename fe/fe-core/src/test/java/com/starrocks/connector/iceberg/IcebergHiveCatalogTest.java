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
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.ColWithComment;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
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
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.HIVE_METASTORE_URIS;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CATALOG_TYPE;

public class IcebergHiveCatalogTest {
    private static final String CATALOG_NAME = "iceberg_hive_catalog";
    public static final Map<String, String> DEFAULT_CONFIG = new HashMap<>();
    public static final IcebergCatalogProperties DEFAULT_CATALOG_PROPERTIES;

    public static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment();
    static {
        DEFAULT_CONFIG.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732"); // non-exist ip, prevent to connect local service
        DEFAULT_CONFIG.put(ICEBERG_CATALOG_TYPE, "hive");
        DEFAULT_CATALOG_PROPERTIES = new IcebergCatalogProperties(DEFAULT_CONFIG);
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    public IcebergMetadata buildIcebergMetadata(HiveCatalog hiveCatalog) {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(hiveCatalog, new Configuration());
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(
                CATALOG_NAME, icebergHiveCatalog, DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());

        return new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, cachingIcebergCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(),
                new IcebergCatalogProperties(DEFAULT_CONFIG));
    }

    @Test
    public void testListAllDatabases(@Mocked HiveCatalog hiveCatalog) {
        new Expectations() {
            {
                hiveCatalog.listNamespaces();
                result = ImmutableList.of(Namespace.of("db1"), Namespace.of("db2"));
                times = 1;
            }
        };

        Map<String, String> icebergProperties = new HashMap<>();
        icebergProperties.put("hive.metastore.uris", "thrift://129.1.2.3:9876");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(
                "hive_native_catalog", new Configuration(), icebergProperties);
        List<String> dbs = icebergHiveCatalog.listAllDatabases();
        Assert.assertEquals(Arrays.asList("db1", "db2"), dbs);
    }

    @Test
    public void testRenameTable(@Mocked HiveCatalog hiveCatalog) {
        new Expectations() {
            {
                hiveCatalog.tableExists((TableIdentifier) any);
                result = true;
            }
        };
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(
                "catalog", new Configuration(), ImmutableMap.of("hive.metastore.uris", "thrift://129.1.2.3:9876"));
        icebergHiveCatalog.renameTable("db", "tb1", "tb2");
        boolean exists = icebergHiveCatalog.tableExists("db", "tbl2");
        Assert.assertTrue(exists);
    }

    @Test
    public void testCreateView(@Mocked HiveCatalog hiveCatalog, @Mocked BaseView baseView,
                               @Mocked ImmutableSQLViewRepresentation representation) throws Exception {
        IcebergMetadata metadata = buildIcebergMetadata(hiveCatalog);

        new Expectations() {
            {
                hiveCatalog.loadNamespaceMetadata(Namespace.of("db"));
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

                hiveCatalog.loadView(TableIdentifier.of("db", "view"));
                result = baseView;
                minTimes = 1;
            }
        };

        Table table = metadata.getView("db", "view");
        Assert.assertEquals(ICEBERG_VIEW, table.getType());
        Assert.assertEquals("xxx", table.getTableLocation());
        Assert.assertEquals("view", table.getName());
        IcebergView icebergView = (IcebergView) table;
        Assert.assertEquals("select * from table", icebergView.getInlineViewDef());
        Assert.assertEquals("mocked", icebergView.getComment());
    }
}
