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

package com.starrocks.qe;

import com.starrocks.authorization.MockedLocalMetaStore;
import com.starrocks.authorization.RBACMockedMetadataMgr;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.UseDbStmt;
import com.starrocks.sql.parser.SqlParser;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UseDbTest {
    @Test
    public void testUseDbWithoutCurrentCatalog() throws Exception {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        MockedLocalMetaStore localMetastore = new MockedLocalMetaStore(globalStateMgr, globalStateMgr.getRecycleBin(), null);
        localMetastore.init();
        globalStateMgr.setLocalMetastore(localMetastore);

        RBACMockedMetadataMgr metadataMgr =
                new RBACMockedMetadataMgr(localMetastore, globalStateMgr.getConnectorMgr());
        globalStateMgr.setMetadataMgr(metadataMgr);

        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setCurrentRoleIds(UserIdentity.ROOT);
        context.setQueryId(UUIDUtil.genUUID());

        String sql = "use db1;";
        UseDbStmt useDbStmt = (UseDbStmt) SqlParser.parseSingleStatement(sql, context.getSessionVariable().getSqlMode());
        Analyzer.analyze(useDbStmt, context);

        StmtExecutor stmtExecutor = new StmtExecutor(context, useDbStmt);
        stmtExecutor.execute();

        Assertions.assertEquals("default_catalog", context.getCurrentCatalog());
        Assertions.assertEquals("db1", context.getDatabase());
    }

    @Test
    public void testUseDbWithCatalog() throws Exception {
        new MockUp<CatalogMgr>() {
            @Mock
            public boolean catalogExists(String catalogName) {
                if (catalogName.equals("catalog1")) {
                    return true;
                } else {
                    return false;
                }
            }
        };

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        MockedLocalMetaStore localMetastore = new MockedLocalMetaStore(globalStateMgr, globalStateMgr.getRecycleBin(), null);
        localMetastore.init();
        globalStateMgr.setLocalMetastore(localMetastore);

        RBACMockedMetadataMgr metadataMgr =
                new RBACMockedMetadataMgr(localMetastore, globalStateMgr.getConnectorMgr());
        globalStateMgr.setMetadataMgr(metadataMgr);

        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setCurrentRoleIds(UserIdentity.ROOT);
        context.setQueryId(UUIDUtil.genUUID());
        context.setCurrentCatalog("default_catalog");

        String sql = "use catalog1.db1;";
        UseDbStmt useDbStmt = (UseDbStmt) SqlParser.parseSingleStatement(sql, context.getSessionVariable().getSqlMode());
        Analyzer.analyze(useDbStmt, context);

        StmtExecutor stmtExecutor = new StmtExecutor(context, useDbStmt);
        stmtExecutor.execute();

        Assertions.assertEquals("catalog1", context.getCurrentCatalog());
        Assertions.assertEquals("db1", context.getDatabase());
    }

    /**
     * Test case: Use database with non-existent catalog
     * Test point: Verify that using a non-existent catalog throws appropriate error
     */
    @Test
    public void testUseDbWithNonExistentCatalog() throws Exception {
        new MockUp<CatalogMgr>() {
            @Mock
            public boolean catalogExists(String catalogName) {
                return false; // All catalogs are non-existent
            }
        };

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        MockedLocalMetaStore localMetastore = new MockedLocalMetaStore(globalStateMgr, globalStateMgr.getRecycleBin(), null);
        localMetastore.init();
        globalStateMgr.setLocalMetastore(localMetastore);

        RBACMockedMetadataMgr metadataMgr =
                new RBACMockedMetadataMgr(localMetastore, globalStateMgr.getConnectorMgr());
        globalStateMgr.setMetadataMgr(metadataMgr);

        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setCurrentRoleIds(UserIdentity.ROOT);
        context.setQueryId(UUIDUtil.genUUID());
        context.setCurrentCatalog("default_catalog");

        String sql = "use nonexistent_catalog.db1;";
        UseDbStmt useDbStmt = (UseDbStmt) SqlParser.parseSingleStatement(sql, context.getSessionVariable().getSqlMode());
        Analyzer.analyze(useDbStmt, context);

        StmtExecutor stmtExecutor = new StmtExecutor(context, useDbStmt);
        stmtExecutor.execute();

        // Should have error state due to non-existent catalog
        Assertions.assertTrue(context.getState().isError());
        Assertions.assertTrue(context.getState().getErrorMessage().contains("Unknown catalog"));
    }

    /**
     * Test case: Use database statement execution state
     * Test point: Verify that execution state is set correctly after successful execution
     */
    @Test
    public void testUseDbExecutionState() throws Exception {
        new MockUp<MetadataMgr>() {
            @Mock
            public Database getDb(ConnectContext context, String catalogName, String dbName) {
                if (dbName.equals("db1")) {
                    return new Database(1, "db1");
                }
                return null;
            }
        };

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        MockedLocalMetaStore localMetastore = new MockedLocalMetaStore(globalStateMgr, globalStateMgr.getRecycleBin(), null);
        localMetastore.init();
        globalStateMgr.setLocalMetastore(localMetastore);

        RBACMockedMetadataMgr metadataMgr =
                new RBACMockedMetadataMgr(localMetastore, globalStateMgr.getConnectorMgr());
        globalStateMgr.setMetadataMgr(metadataMgr);

        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setCurrentRoleIds(UserIdentity.ROOT);
        context.setQueryId(UUIDUtil.genUUID());
        context.setCurrentCatalog("default_catalog");

        String sql = "use db1;";
        UseDbStmt useDbStmt = (UseDbStmt) SqlParser.parseSingleStatement(sql, context.getSessionVariable().getSqlMode());
        Analyzer.analyze(useDbStmt, context);

        StmtExecutor stmtExecutor = new StmtExecutor(context, useDbStmt);
        stmtExecutor.execute();

        // Execution state should be OK after successful execution
        Assertions.assertEquals(QueryState.MysqlStateType.OK, context.getState().getStateType());
        Assertions.assertFalse(context.getState().isError());
    }
}
