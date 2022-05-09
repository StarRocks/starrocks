// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.connector.ConnectorMgr;
import com.starrocks.qe.DdlExecutor;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CatalogStmtTest {
    private static StarRocksAssert starRocksAssert;
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        String createTbl = "create table db1.tbl1(k1 varchar(32), catalog varchar(32), external varchar(32), k4 int) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1')";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db1").useDatabase("tbl1");
        starRocksAssert.withTable(createTbl);
    }

    @Test
    public void testCreateCatalogParserAndAnalyzer() {
        String sql_1 = "CREATE EXTERNAL CATALOG catalog_1 PROPERTIES(\"type\"=\"hive\", \"hive.metastore.uris\"=\"thrift://127.0.0.1:9083\")";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql_1);
        Assert.assertTrue(stmt instanceof CreateCatalogStmt);
        String sql_2 = "CREATE EXTERNAL CATALOG catalog_2";
        AnalyzeTestUtil.analyzeFail(sql_2);
        String sql_3 = "CREATE EXTERNAL CATALOG catalog_3 properties(\"type\"=\"xxx\")";
        AnalyzeTestUtil.analyzeFail(sql_3);
        String sql_4 = "CREATE EXTERNAL CATALOG catalog_4 properties(\"aaa\"=\"bbb\")";
        AnalyzeTestUtil.analyzeFail(sql_4);
        String sql_5 = "CREATE EXTERNAL CATALOG default PROPERTIES(\"type\"=\"hive\", \"hive.metastore.uris\"=\"thrift://127.0.0.1:9083\")";
        AnalyzeTestUtil.analyzeFail(sql_5);
        String sql_6 = "CREATE EXTERNAL CATALOG catalog_5 properties(\"type\"=\"hive\")";
        StatementBase stmt2 = AnalyzeTestUtil.analyzeSuccess(sql_6);
        Assert.assertEquals("CREATE EXTERNAL CATALOG 'catalog_5' PROPERTIES(\"type\"  =  \"hive\")", stmt2.toSql());
    }

    @Test
    public void testSelectNonReservedCol() {
        String sql_1 = "select * from db1.tbl1";
        AnalyzeTestUtil.analyzeSuccess(sql_1);
        String sql_3 = "select k1, catalog, external from db1.tbl1";
        AnalyzeTestUtil.analyzeSuccess(sql_3);
    }

    @Test
    public void testCreateCatalog() throws Exception {
        String sql = "CREATE EXTERNAL CATALOG hive_catalog PROPERTIES(\"type\"=\"hive\", \"hive.metastore.uris\"=\"thrift://127.0.0.1:9083\")";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof CreateCatalogStmt);
        CreateCatalogStmt statement = (CreateCatalogStmt) stmt;
        DdlExecutor.execute(GlobalStateMgr.getCurrentState(), statement);
        CatalogMgr catalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();
        ConnectorMgr connectorMgr = GlobalStateMgr.getCurrentState().getConnectorMgr();
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        Assert.assertTrue(catalogMgr.catalogExists("hive_catalog"));
        Assert.assertTrue(connectorMgr.connectorExists("hive_catalog"));
        Assert.assertTrue(metadataMgr.connectorMetadataExists("hive_catalog"));

        try {
            DdlExecutor.execute(GlobalStateMgr.getCurrentState(), statement);
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("exists"));
        }

        catalogMgr.dropCatalog("hive_catalog");
        Assert.assertFalse(catalogMgr.catalogExists("hive_catalog"));
        Assert.assertFalse(connectorMgr.connectorExists("hive_catalog"));
        Assert.assertFalse(metadataMgr.connectorMetadataExists("hive_catalog"));
    }

}
