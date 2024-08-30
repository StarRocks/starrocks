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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorReportException;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.connector.hive.HiveCacheUpdateProcessor;
import com.starrocks.connector.hive.HiveMetadata;
import com.starrocks.connector.hive.HiveMetastoreOperations;
import com.starrocks.connector.hive.HiveStatisticsProvider;
import com.starrocks.privilege.IdGenerator;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.server.GlobalStateMgr;
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
import java.util.Optional;
import java.util.concurrent.Executor;

public class ShowTablesTest {
    private static ConnectContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        UtFrameUtils.setUpForPersistTest();

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();

        HiveCacheUpdateProcessor hiveCacheUpdateProcessor = new HiveCacheUpdateProcessor("hive0", null,
                null, null, true, false);
        MockHiveCatalog hiveMetadata = new MockHiveCatalog("hive0", null, null, null, null,
                Optional.of(hiveCacheUpdateProcessor), null, null);

        MockedMetadataMgr metadataMgr = new MockedMetadataMgr(new ShowTableMockMeta(globalStateMgr, null, null),
                GlobalStateMgr.getCurrentState().getConnectorMgr());
        metadataMgr.registerMockedMetadata("hive_catalog", hiveMetadata);
        globalStateMgr.setMetadataMgr(metadataMgr);

        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));

        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user test_user", ctx);
        globalStateMgr.getAuthenticationMgr().createUser(createUserStmt);
    }

    static class MockHiveCatalog extends HiveMetadata {
        private final IdGenerator idGenerator;
        private final Map<String, Database> externalDbSet;
        private final Map<String, Table> externalTbSet;

        public MockHiveCatalog(String catalogName, HdfsEnvironment hdfsEnvironment,
                               HiveMetastoreOperations hmsOps,
                               RemoteFileOperations fileOperations,
                               HiveStatisticsProvider statisticsProvider,
                               Optional<HiveCacheUpdateProcessor> cacheUpdateProcessor, Executor updateExecutor,
                               Executor refreshOthersFeExecutor) throws DdlException {
            super(catalogName, hdfsEnvironment, hmsOps, fileOperations, statisticsProvider, cacheUpdateProcessor, updateExecutor,
                    refreshOthersFeExecutor);

            idGenerator = new IdGenerator();
            this.externalDbSet = new HashMap<>();
            this.externalTbSet = new HashMap<>();

            Database db3 = new Database(idGenerator.getNextId(), "hive_db");
            externalDbSet.put("hive_db", db3);

            HiveTable table = new HiveTable();
            table.setId(idGenerator.getNextId());
            table.setName("hive_test");
            externalTbSet.put("hive_test", table);

            Map<String, String> properties = Maps.newHashMap();
            properties.put("type", "hive");
            properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
            GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog(
                    "hive", "hive_catalog", "", properties);
        }

        public Database getDb(String dbName) {
            return externalDbSet.get(dbName);
        }

        @Override
        public List<String> listDbNames() {
            return new ArrayList<>(externalDbSet.keySet());
        }

        @Override
        public Table getTable(String dbName, String tblName) {
            return externalTbSet.get(tblName);
        }

        @Override
        public List<String> listTableNames(String dbName) {
            return new ArrayList<>(externalTbSet.keySet());
        }
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

        ShowTableStmt stmt = new ShowTableStmt("testDb", true, null, null,
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
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
}
