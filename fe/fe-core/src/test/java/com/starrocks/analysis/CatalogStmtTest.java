// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.connector.ConnectorMgr;
import com.starrocks.qe.DdlExecutor;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
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
    public void testDropCatalogParserAndAnalyzer() {
        // test drop ddl DROP CATALOG catalog_name
        String sql_1 = "DROP CATALOG catalog_1";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql_1);
        Assert.assertTrue(stmt instanceof DropCatalogStmt);
        String sql_2 = "DROP CATALOG";
        AnalyzeTestUtil.analyzeFail(sql_2);
        String sql_3 = "DROP CATALOG default";
        AnalyzeTestUtil.analyzeFail(sql_3);
        Assert.assertEquals("DROP CATALOG 'catalog_1'", stmt.toSql());

        // test drop ddl DROP CATALOG 'catalog_name'
        String sql_4 = "DROP CATALOG 'catalog_1'";
        StatementBase stmt2 = AnalyzeTestUtil.analyzeSuccess(sql_4);
        Assert.assertTrue(stmt2 instanceof DropCatalogStmt);
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

        catalogMgr.dropCatalog(new DropCatalogStmt("hive_catalog"));
        Assert.assertFalse(catalogMgr.catalogExists("hive_catalog"));
        Assert.assertFalse(connectorMgr.connectorExists("hive_catalog"));
        Assert.assertFalse(metadataMgr.connectorMetadataExists("hive_catalog"));
    }

    @Test
    public void testDropCatalog() throws Exception {
        // test drop ddl DROP CATALOG catalog_name
        String createSql = "CREATE EXTERNAL CATALOG hive_catalog PROPERTIES(\"type\"=\"hive\", \"hive.metastore.uris\"=\"thrift://127.0.0.1:9083\")";
        String dropSql = "DROP CATALOG hive_catalog";


        CatalogMgr catalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();
        ConnectorMgr connectorMgr = GlobalStateMgr.getCurrentState().getConnectorMgr();
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();

        StatementBase createStmtBase = AnalyzeTestUtil.analyzeSuccess(createSql);
        Assert.assertTrue(createStmtBase instanceof CreateCatalogStmt);
        CreateCatalogStmt createCatalogStmt = (CreateCatalogStmt) createStmtBase;
        DdlExecutor.execute(GlobalStateMgr.getCurrentState(), createCatalogStmt);
        Assert.assertTrue(catalogMgr.catalogExists("hive_catalog"));
        Assert.assertTrue(connectorMgr.connectorExists("hive_catalog"));
        Assert.assertTrue(metadataMgr.connectorMetadataExists("hive_catalog"));

        StatementBase dropStmtBase = AnalyzeTestUtil.analyzeSuccess(dropSql);
        Assert.assertTrue(dropStmtBase instanceof DropCatalogStmt);
        DropCatalogStmt dropCatalogStmt = (DropCatalogStmt) dropStmtBase;
        DdlExecutor.execute(GlobalStateMgr.getCurrentState(), dropCatalogStmt);
        Assert.assertFalse(catalogMgr.catalogExists("hive_catalog"));
        Assert.assertFalse(connectorMgr.connectorExists("hive_catalog"));
        Assert.assertFalse(metadataMgr.connectorMetadataExists("hive_catalog"));

        // test drop ddl DROP CATALOG 'catalog_name'
        String dropSql_2 = "DROP CATALOG 'hive_catalog'";

        DdlExecutor.execute(GlobalStateMgr.getCurrentState(), createCatalogStmt);
        Assert.assertTrue(catalogMgr.catalogExists("hive_catalog"));
        Assert.assertTrue(connectorMgr.connectorExists("hive_catalog"));
        Assert.assertTrue(metadataMgr.connectorMetadataExists("hive_catalog"));

        StatementBase dropStmtBase_2 = AnalyzeTestUtil.analyzeSuccess(dropSql_2);
        Assert.assertTrue(dropStmtBase_2 instanceof DropCatalogStmt);
        DropCatalogStmt dropCatalogStmt_2 = (DropCatalogStmt) dropStmtBase;
        DdlExecutor.execute(GlobalStateMgr.getCurrentState(), dropCatalogStmt_2);
        Assert.assertFalse(catalogMgr.catalogExists("hive_catalog"));
        Assert.assertFalse(connectorMgr.connectorExists("hive_catalog"));
        Assert.assertFalse(metadataMgr.connectorMetadataExists("hive_catalog"));
    }
}
