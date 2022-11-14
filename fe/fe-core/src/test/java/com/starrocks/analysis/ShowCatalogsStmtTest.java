// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.ShowCatalogsStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ShowCatalogsStmtTest {
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext ctx;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        GlobalStateMgr globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);
        AnalyzeTestUtil.init();
        String createCatalog = "CREATE EXTERNAL CATALOG hive_catalog_1 COMMENT \"hive_catalog\" PROPERTIES(\"type\"=\"hive\", \"hive.metastore.uris\"=\"thrift://127.0.0.1:9083\");";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(createCatalog);
        Assert.assertTrue(stmt instanceof CreateCatalogStmt);
        ConnectContext connectCtx = new ConnectContext();
        connectCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        CreateCatalogStmt statement = (CreateCatalogStmt) stmt;
        DDLStmtExecutor.execute(statement, connectCtx);
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db1").useDatabase("tbl1");

        ctx = new ConnectContext(null);
        ctx.setGlobalStateMgr(AccessTestUtil.fetchAdminCatalog());
    }

    @Test
    public void testShowCatalogsParserAndAnalyzer() {
        String sql_1 = "SHOW CATALOGS";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql_1);
        Assert.assertTrue(stmt instanceof ShowCatalogsStmt);
    }

    @Test
    public void testShowCatalogsNormal() throws AnalysisException, DdlException {
        ShowCatalogsStmt stmt = new ShowCatalogsStmt();
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();
        ShowResultSetMetaData metaData = resultSet.getMetaData();
        Assert.assertEquals("Catalog", metaData.getColumn(0).getName());
        Assert.assertEquals("Type", metaData.getColumn(1).getName());
        Assert.assertEquals("Comment", metaData.getColumn(2).getName());
        Assert.assertEquals("[default_catalog, Internal, Internal Catalog]", resultSet.getResultRows().get(0).toString());
        Assert.assertEquals("[hive_catalog_1, hive, hive_catalog]", resultSet.getResultRows().get(1).toString());
    }
}
