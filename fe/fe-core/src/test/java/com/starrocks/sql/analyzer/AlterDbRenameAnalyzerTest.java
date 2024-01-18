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

package com.starrocks.sql.analyzer;

import com.google.common.collect.Sets;
import com.starrocks.analysis.AccessTestUtil;
import com.starrocks.authz.authorization.PrivilegeBuiltinConstants;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterDatabaseRenameStatement;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class AlterDbRenameAnalyzerTest {


    private static ConnectContext ctx;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

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
        ctx = new ConnectContext(null);
        ctx.setGlobalStateMgr(AccessTestUtil.fetchAdminCatalog());
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
    }

    @Test
    public void testAnalyze() throws Exception {
        String sql = "alter database `db1` rename db2";
        ctx.setCurrentCatalog("hive_catalog_1");
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Getting analyzing error. Detail message: " +
                "Unsupported operation rename db under external catalog.");
        AlterDatabaseRenameStatement statement = (AlterDatabaseRenameStatement) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
    }

}
