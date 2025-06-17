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

import com.google.common.collect.Sets;
import com.starrocks.authorization.IdGenerator;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorReportException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.ShowDataStmt;
import com.starrocks.sql.ast.ShowTableStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ShowTablesTest {
    private static ConnectContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        UtFrameUtils.setUpForPersistTest();

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        IdGenerator idGenerator = new IdGenerator();

        MockedLocalMetastore mockedLocalMetastore = new MockedLocalMetastore(idGenerator,
                globalStateMgr, null, null);
        mockedLocalMetastore.init();

        globalStateMgr.setLocalMetastore(mockedLocalMetastore);

        ShowTableMockMeta metadataMgr =
                new ShowTableMockMeta(idGenerator, mockedLocalMetastore, globalStateMgr.getConnectorMgr());
        metadataMgr.init();
        globalStateMgr.setMetadataMgr(metadataMgr);

        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));

        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user test_user", ctx);
        globalStateMgr.getAuthenticationMgr().createUser(createUserStmt);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testShowTable() throws Exception {
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));

        ShowTableStmt stmt = new ShowTableStmt("testDb", false, null);
        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testMv", resultSet.getString(0));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testTbl", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowTableVerbose() throws Exception {
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));

        ShowTableStmt stmt = new ShowTableStmt("testDb", true, null);
        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testMv", resultSet.getString(0));
        Assert.assertEquals("VIEW", resultSet.getString(1));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testTbl", resultSet.getString(0));
        Assert.assertEquals("BASE TABLE", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testExternal() throws Exception {
        ctx.setCurrentCatalog("hive_catalog");
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%"));
        ShowTableStmt stmt = new ShowTableStmt("hive_db", true, null);
        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        Assert.assertFalse(resultSet.next());

        Assert.assertThrows(ErrorReportException.class,
                () -> ctx.changeCatalog("hive_catalog"));
        Assert.assertThrows(ErrorReportException.class,
                () -> ctx.changeCatalogDb("hive_catalog.hive_db"));

        String sql = "grant usage on catalog hive_catalog to test_user";
        GrantPrivilegeStmt grantPrivilegeStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(grantPrivilegeStmt, ctx);
        Assert.assertThrows(ErrorReportException.class,
                () -> ctx.changeCatalogDb("hive_catalog.hive_db"));

        ctx.setCurrentCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
    }

    @Test
    public void testShowData() {
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%"));
        ShowDataStmt stmt = new ShowDataStmt("test", "testTbl", null);
        Assert.assertThrows(ErrorReportException.class, () -> ShowExecutor.execute(stmt, ctx));
    }

    static class MockedLocalMetastore extends LocalMetastore {
        private final IdGenerator idGenerator;
        private final Map<String, Database> databaseSet;
        private final Map<String, Table> tableMap;

        public MockedLocalMetastore(
                IdGenerator idGenerator,
                GlobalStateMgr globalStateMgr, CatalogRecycleBin recycleBin,
                ColocateTableIndex colocateTableIndex) {
            super(globalStateMgr, recycleBin, colocateTableIndex);
            this.databaseSet = new HashMap<>();
            this.tableMap = new HashMap<>();
            this.idGenerator = idGenerator;
        }

        public void init() throws DdlException {
            Database db = new Database(idGenerator.getNextId(), "testDb");
            databaseSet.put("testDb", db);

            Database db2 = new Database(idGenerator.getNextId(), "test");
            databaseSet.put("test", db2);

            OlapTable t0 = new OlapTable();
            t0.setId(idGenerator.getNextId());
            t0.setName("testTbl");
            tableMap.put("testTbl", t0);

            MaterializedView mv = new MaterializedView();
            mv.setId(idGenerator.getNextId());
            mv.setName("testMv");
            tableMap.put("testMv", mv);
        }

        @Override
        public Database getDb(String dbName) {
            return databaseSet.get(dbName);
        }

        @Override
        public Database getDb(long databaseId) {
            for (Database database : databaseSet.values()) {
                if (database.getId() == databaseId) {
                    return database;
                }
            }

            return null;
        }

        @Override
        public ConcurrentHashMap<String, Database> getFullNameToDb() {
            return new ConcurrentHashMap<>(databaseSet);
        }

        @Override
        public Table getTable(String dbName, String tblName) {
            return tableMap.get(tblName);
        }

        @Override
        public List<Table> getTables(Long dbId) {
            return new ArrayList<>(tableMap.values());
        }
    }
}
