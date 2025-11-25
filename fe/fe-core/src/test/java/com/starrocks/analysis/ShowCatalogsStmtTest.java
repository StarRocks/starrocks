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

import com.google.common.collect.Sets;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.UserIdentity;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodName.class)
public class ShowCatalogsStmtTest {
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext ctx;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        GlobalStateMgr globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);
        AnalyzeTestUtil.init();
        String createCatalog = "CREATE EXTERNAL CATALOG hive_catalog_1 COMMENT \"hive_catalog\" PROPERTIES(\"type\"=\"hive\", \"hive.metastore.uris\"=\"thrift://127.0.0.1:9083\");";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(createCatalog);
        Assertions.assertTrue(stmt instanceof CreateCatalogStmt);
        ConnectContext connectCtx = new ConnectContext();
        connectCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        CreateCatalogStmt statement = (CreateCatalogStmt) stmt;
        DDLStmtExecutor.execute(statement, connectCtx);
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db1").useDatabase("tbl1");

        ctx = new ConnectContext(null);
        ctx.setGlobalStateMgr(AccessTestUtil.fetchAdminCatalog());
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
    }

    @Test
    public void testShowCatalogsNormal() throws AnalysisException, DdlException {
        ShowCatalogsStmt stmt = new ShowCatalogsStmt(null);
        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        ShowResultSetMetaData metaData = resultSet.getMetaData();
        Assertions.assertEquals("Catalog", metaData.getColumn(0).getName());
        Assertions.assertEquals("Type", metaData.getColumn(1).getName());
        Assertions.assertEquals("Comment", metaData.getColumn(2).getName());
        Assertions.assertEquals("[default_catalog, Internal, An internal catalog contains this cluster's self-managed tables.]",
                resultSet.getResultRows().get(0).toString());
        Assertions.assertEquals("[hive_catalog_1, Hive, hive_catalog]", resultSet.getResultRows().get(1).toString());
    }

    @Test
    public void testShowCatalogsParserAndAnalyzer() {
        String sql_1 = "SHOW CATALOGS";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql_1);
        Assertions.assertTrue(stmt instanceof ShowCatalogsStmt);
    }
}
