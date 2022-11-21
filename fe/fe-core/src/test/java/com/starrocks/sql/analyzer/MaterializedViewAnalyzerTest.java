// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergResource;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.ResourceMgr;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.iceberg.IcebergCatalog;
import com.starrocks.connector.iceberg.IcebergCatalogType;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class MaterializedViewAnalyzerTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testRefreshMaterializedView() throws Exception {
        Config.enable_experimental_mv = true;
        StarRocksAssert starRocksAssert = AnalyzeTestUtil.getStarRocksAssert();
        starRocksAssert.useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values [('2022-02-01'),('2022-02-16')),\n" +
                        "    PARTITION p2 values [('2022-02-16'),('2022-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withNewMaterializedView("create materialized view mv\n" +
                        "PARTITION BY k1\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh async\n" +
                        "as select k1, k2, sum(v1) as total from tbl1 group by k1, k2;");

        analyzeSuccess("refresh materialized view mv");
        Database testDb = starRocksAssert.getCtx().getGlobalStateMgr().getDb("test");
        Table table = testDb.getTable("mv");
        Assert.assertNotNull(table);
        Assert.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        mv.setActive(false);
        analyzeFail("refresh materialized view mv");
    }

    @Test
    public void testMaterializedViewAnalyzeWithResourceExternalTable(@Mocked GlobalStateMgr globalStateMgr,
                                                                     @Mocked ResourceMgr resourceMgr,
                                                                     @Mocked IcebergResource icebergResource,
                                                                     @Mocked IcebergCatalog icebergCatalog)
            throws Exception {
        Config.enable_experimental_mv = true;
        String resourceName = "iceberg0";
        String dbName = "iceberg_ssb_1g_parquet_snappy";
        String tblName = "customer";

        Map<String, String> properties = new HashMap<>();
        properties.put(IcebergTable.ICEBERG_DB, dbName);
        properties.put(IcebergTable.ICEBERG_TABLE, tblName);
        properties.put(IcebergTable.ICEBERG_RESOURCE, resourceName);

        new MockUp<IcebergUtil>() {
            @Mock
            public IcebergCatalog getIcebergHiveCatalog(String uris, Map<String, String> icebergProperties) {
                return icebergCatalog;
            }
        };

        new Expectations(resourceMgr, icebergResource) {
            {
                globalStateMgr.getResourceMgr();
                result = resourceMgr;

                resourceMgr.getResource(resourceName);
                result = icebergResource;

                icebergResource.getType();
                result = Resource.ResourceType.ICEBERG;

                icebergResource.getCatalogType();
                result = IcebergCatalogType.HIVE_CATALOG;
            }
        };

        IcebergTable icebergTable = new IcebergTable(123, tblName, Lists.newArrayList(), properties);
        boolean result = Deencapsulation.invoke(new MaterializedViewAnalyzer.MaterializedViewAnalyzerVisitor(),
                "isExternalTableFromResource", icebergTable);
        Assert.assertTrue(result);
    }
}
