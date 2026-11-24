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

package com.starrocks.authorization;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.RecoverInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DatabaseWildcardCleanupTest {
    private static ConnectContext ctx;

    @BeforeAll
    public static void setUp() throws Exception {
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.setUpForPersistTest();
        ctx.getGlobalStateMgr().getAuthorizationMgr().initBuiltinRolesAndUsers();
    }

    @AfterAll
    public static void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDatabaseWildcardCleanup(boolean withGrantOption) throws Exception {
        String dbName = "wildcard_db_" + withGrantOption;
        String userName = "wildcard_user_" + withGrantOption;
        String roleName = "wildcard_role_" + withGrantOption;
        execute("CREATE DATABASE " + dbName);
        execute("CREATE USER " + userName);
        execute("CREATE ROLE " + roleName);
        for (String objectType : List.of("TABLES", "VIEWS", "MATERIALIZED VIEWS")) {
            for (String grantee : List.of(userName, "ROLE " + roleName)) {
                execute("GRANT SELECT ON ALL " + objectType + " IN DATABASE " + dbName + " TO " + grantee
                        + (withGrantOption ? " WITH GRANT OPTION" : ""));
            }
        }

        AuthorizationMgr manager = ctx.getGlobalStateMgr().getAuthorizationMgr();
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp(userName, "%");
        UserPrivilegeCollectionV2 userCollection = manager.getUserPrivilegeCollection(user);
        RolePrivilegeCollectionV2 roleCollection = manager.getRolePrivilegeCollection(roleName);
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = metastore.getDb(dbName);
        Assertions.assertTrue(db.getTables().isEmpty());
        manager.removeInvalidObject();
        assertWildcardEntries(userCollection, withGrantOption);
        assertWildcardEntries(roleCollection, withGrantOption);

        execute("DROP DATABASE " + dbName);
        Assertions.assertNull(metastore.getDb(dbName));
        Assertions.assertNotNull(metastore.getDbIncludeRecycleBin(db.getId()));
        manager.removeInvalidObject();
        assertWildcardEntries(userCollection, withGrantOption);
        assertWildcardEntries(roleCollection, withGrantOption);
        UserPrivilegeCollectionV2 savedUser = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(userCollection), UserPrivilegeCollectionV2.class);
        RolePrivilegeCollectionV2 savedRole = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(roleCollection), RolePrivilegeCollectionV2.class);

        metastore.replayEraseDatabase(db.getId());
        Assertions.assertNull(metastore.getDbIncludeRecycleBin(db.getId()));
        manager.removeInvalidObject();
        manager.removeInvalidObject();
        Assertions.assertTrue(userCollection.typeToPrivilegeEntryList.isEmpty());
        Assertions.assertTrue(roleCollection.typeToPrivilegeEntryList.isEmpty());
        for (PrivilegeCollectionV2 collection : List.of(savedUser, savedRole)) {
            collection.removeInvalidObject();
            Assertions.assertTrue(collection.typeToPrivilegeEntryList.isEmpty());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testKeepWildcardGrantsDuringRecovery(boolean replay) throws Exception {
        String dbName = "recovering_db_" + replay;
        String userName = "recovering_user_" + replay;
        String roleName = "recovering_role_" + replay;
        execute("CREATE DATABASE " + dbName);
        execute("CREATE USER " + userName);
        execute("CREATE ROLE " + roleName);
        for (String objectType : List.of("TABLES", "VIEWS", "MATERIALIZED VIEWS")) {
            for (String grantee : List.of(userName, "ROLE " + roleName)) {
                execute("GRANT SELECT ON ALL " + objectType + " IN DATABASE " + dbName + " TO " + grantee
                        + " WITH GRANT OPTION");
            }
        }

        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        AuthorizationMgr manager = state.getAuthorizationMgr();
        UserPrivilegeCollectionV2 userCollection = manager.getUserPrivilegeCollection(
                UserIdentity.createAnalyzedUserIdentWithIp(userName, "%"));
        RolePrivilegeCollectionV2 roleCollection = manager.getRolePrivilegeCollection(roleName);
        LocalMetastore metastore = replay ? Mockito.spy(state.getLocalMetastore()) : state.getLocalMetastore();
        Database db = metastore.getDb(dbName);
        execute("DROP DATABASE " + dbName);

        CountDownLatch registrationPaused = new CountDownLatch(1);
        CountDownLatch resumeRegistration = new CountDownLatch(1);
        CountDownLatch cleanupStarted = new CountDownLatch(1);
        Answer<Object> pauseRegistration = invocation -> {
            registrationPaused.countDown();
            Assertions.assertTrue(resumeRegistration.await(10, TimeUnit.SECONDS));
            return invocation.callRealMethod();
        };
        EditLog originalEditLog = state.getEditLog();
        if (replay) {
            Mockito.doAnswer(pauseRegistration).when(metastore).replayCreateDb(Mockito.any(Database.class));
        } else {
            EditLog editLog = Mockito.spy(originalEditLog);
            Mockito.doAnswer(pauseRegistration).when(editLog).logRecoverDb(Mockito.any(RecoverInfo.class), Mockito.any());
            state.setEditLog(editLog);
        }

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> recovery = executor.submit(() -> {
                if (replay) {
                    metastore.replayRecoverDatabase(new RecoverInfo(db.getId(), -1L, -1L));
                } else {
                    metastore.recoverDatabase(new RecoverDbStmt(dbName));
                }
                return null;
            });
            Assertions.assertTrue(registrationPaused.await(10, TimeUnit.SECONDS));
            Assertions.assertNull(metastore.getDbIncludeRecycleBin(db.getId()));
            Future<?> cleanup = executor.submit(() -> {
                cleanupStarted.countDown();
                manager.removeInvalidObject();
            });
            Assertions.assertTrue(cleanupStarted.await(10, TimeUnit.SECONDS));
            Assertions.assertThrows(TimeoutException.class, () -> cleanup.get(500, TimeUnit.MILLISECONDS));
            resumeRegistration.countDown();
            recovery.get(10, TimeUnit.SECONDS);
            cleanup.get(10, TimeUnit.SECONDS);
            Assertions.assertSame(db, metastore.getDb(db.getId()));
            assertWildcardEntries(userCollection, true);
            assertWildcardEntries(roleCollection, true);
        } finally {
            resumeRegistration.countDown();
            executor.shutdown();
            try {
                Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            } finally {
                state.setEditLog(originalEditLog);
            }
        }
    }

    @Test
    public void testKeepGlobalAndExternalWildcards() throws Exception {
        PrivilegeCollectionV2 collection = new PrivilegeCollectionV2();
        String allDatabases = PrivilegeBuiltinConstants.ALL_DATABASES_UUID;
        String allTables = PrivilegeBuiltinConstants.ALL_TABLES_UUID;
        collection.grant(ObjectType.TABLE, List.of(PrivilegeType.SELECT), List.of(
                new TablePEntryObject(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID, allDatabases, allTables),
                new TablePEntryObject(PrivilegeBuiltinConstants.ALL_CATALOGS_ID, allDatabases, allTables),
                new TablePEntryObject(100, "external_db", allTables),
                new TablePEntryObject(100, allDatabases, allTables)), false);
        collection.grant(ObjectType.VIEW, List.of(PrivilegeType.SELECT),
                List.of(new ViewPEntryObject(allDatabases, allTables)), false);
        collection.grant(ObjectType.MATERIALIZED_VIEW, List.of(PrivilegeType.SELECT),
                List.of(new MaterializedViewPEntryObject(allDatabases, allTables)), false);
        collection.grant(ObjectType.DATABASE, List.of(PrivilegeType.CREATE_TABLE),
                List.of(new DbPEntryObject(allDatabases)), false);
        String beforeCleanup = GsonUtils.GSON.toJson(collection);
        collection.removeInvalidObject();
        collection.removeInvalidObject();
        Assertions.assertEquals(beforeCleanup, GsonUtils.GSON.toJson(collection));
    }

    private static void assertWildcardEntries(PrivilegeCollectionV2 collection, boolean withGrantOption) {
        Assertions.assertEquals(3, collection.typeToPrivilegeEntryList.size());
        for (ObjectType objectType : List.of(ObjectType.TABLE, ObjectType.VIEW, ObjectType.MATERIALIZED_VIEW)) {
            List<PrivilegeEntry> entries = collection.typeToPrivilegeEntryList.get(objectType);
            Assertions.assertEquals(1, entries.size());
            Assertions.assertEquals(withGrantOption, entries.get(0).withGrantOption);
        }
    }

    private static void execute(String sql) throws Exception {
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);
    }
}
