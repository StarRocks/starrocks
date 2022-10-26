// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.RolePrivilegeCollectionInfo;
import com.starrocks.persist.UserPrivilegeCollectionInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.SetRoleExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.sql.ast.ShowDbStmt;
import com.starrocks.sql.ast.ShowTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class PrivilegeManagerTest {
    private ConnectContext ctx;
    private static final String DB_NAME = "db";
    private static final String TABLE_NAME_0 = "tbl0";
    private static final String TABLE_NAME_1 = "tbl1";
    private UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");

    @Before
    public void setUp() throws Exception {
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.setUpForPersistTest();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        // create db.tbl0 ~ tbl3
        String createTblStmtStr = "(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        starRocksAssert.withDatabase(DB_NAME);
        starRocksAssert.withDatabase(DB_NAME + "1");
        for (int i = 0; i < 4; ++ i) {
            starRocksAssert.withTable("create table db.tbl" + i + createTblStmtStr);
        }
        GlobalStateMgr globalStateMgr = starRocksAssert.getCtx().getGlobalStateMgr();
        starRocksAssert.getCtx().setRemoteIP("localhost");

        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user test_user", ctx);
        globalStateMgr.getAuthenticationManager().createUser(createUserStmt);
    }

    @After
    public void tearDown() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testTable() throws Exception {
        ctx.setCurrentUserIdentity(testUser);
        ctx.setQualifiedUser(testUser.getQualifiedUser());

        PrivilegeManager manager = ctx.getGlobalStateMgr().getPrivilegeManager();
        Assert.assertFalse(PrivilegeManager.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.SELECT));
        Assert.assertFalse(PrivilegeManager.checkAnyActionInDb(ctx, DB_NAME));
        Assert.assertFalse(PrivilegeManager.checkAnyActionInTable(ctx, DB_NAME, TABLE_NAME_1));
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        Assert.assertTrue(PrivilegeManager.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.SELECT));
        Assert.assertTrue(PrivilegeManager.checkAnyActionInDb(ctx, DB_NAME));
        Assert.assertTrue(PrivilegeManager.checkAnyActionInTable(ctx, DB_NAME, TABLE_NAME_1));

        String sql = "grant select on table db.tbl1 to test_user";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantStmt);

        ctx.setCurrentUserIdentity(testUser);
        Assert.assertTrue(PrivilegeManager.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.SELECT));
        Assert.assertTrue(PrivilegeManager.checkAnyActionInDb(ctx, DB_NAME));
        Assert.assertTrue(PrivilegeManager.checkAnyActionInTable(ctx, DB_NAME, TABLE_NAME_1));

        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        sql = "revoke select on db.tbl1 from test_user";
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.revoke(revokeStmt);

        ctx.setCurrentUserIdentity(testUser);
        Assert.assertFalse(PrivilegeManager.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.SELECT));
        Assert.assertFalse(PrivilegeManager.checkAnyActionInDb(ctx, DB_NAME));
        Assert.assertFalse(PrivilegeManager.checkAnyActionInTable(ctx, DB_NAME, TABLE_NAME_1));

        // grant many priveleges
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        sql = "grant select, insert, delete on table db.tbl1 to test_user with grant option";
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertTrue(grantStmt.isWithGrantOption());
        manager.grant(grantStmt);

        ctx.setCurrentUserIdentity(testUser);
        Assert.assertTrue(PrivilegeManager.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.SELECT));
        Assert.assertTrue(PrivilegeManager.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.INSERT));
        Assert.assertTrue(PrivilegeManager.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.DELETE));
        Assert.assertTrue(PrivilegeManager.checkAnyActionInDb(ctx, DB_NAME));
        Assert.assertTrue(PrivilegeManager.checkAnyActionInTable(ctx, DB_NAME, TABLE_NAME_1));

        // revoke only select
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        sql = "revoke select on db.tbl1 from test_user";
        revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertFalse(revokeStmt.isWithGrantOption());
        manager.revoke(revokeStmt);

        ctx.setCurrentUserIdentity(testUser);
        Assert.assertFalse(PrivilegeManager.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.SELECT));
        Assert.assertTrue(PrivilegeManager.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.INSERT));
        Assert.assertTrue(PrivilegeManager.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.DELETE));
        Assert.assertTrue(PrivilegeManager.checkAnyActionInDb(ctx, DB_NAME));
        Assert.assertTrue(PrivilegeManager.checkAnyActionInTable(ctx, DB_NAME, TABLE_NAME_1));

        // revoke all
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        sql = "revoke ALL on table db.tbl1 from test_user";
        revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.revoke(revokeStmt);

        ctx.setCurrentUserIdentity(testUser);
        Assert.assertFalse(PrivilegeManager.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.SELECT));
        Assert.assertFalse(PrivilegeManager.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.INSERT));
        Assert.assertFalse(PrivilegeManager.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.DELETE));
        Assert.assertFalse(PrivilegeManager.checkAnyActionInDb(ctx, DB_NAME));
        Assert.assertFalse(PrivilegeManager.checkAnyActionInTable(ctx, DB_NAME, TABLE_NAME_1));
    }

    @Test
    public void testPersist() throws Exception {
        GlobalStateMgr masterGlobalStateMgr = ctx.getGlobalStateMgr();
        PrivilegeManager masterManager = masterGlobalStateMgr.getPrivilegeManager();

        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        masterManager.save(emptyImage.getDataOutputStream());

        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        String sql = "grant select on db.tbl1 to test_user";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.grant(grantStmt);

        ctx.setCurrentUserIdentity(testUser);
        Assert.assertTrue(PrivilegeManager.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.SELECT));

        UtFrameUtils.PseudoImage grantImage = new UtFrameUtils.PseudoImage();
        masterManager.save(grantImage.getDataOutputStream());

        sql = "revoke select on db.tbl1 from test_user";
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.revoke(revokeStmt);
        ctx.setCurrentUserIdentity(testUser);
        Assert.assertFalse(PrivilegeManager.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.SELECT));
        UtFrameUtils.PseudoImage revokeImage = new UtFrameUtils.PseudoImage();
        masterManager.save(revokeImage.getDataOutputStream());

        // start to replay
        PrivilegeManager followerManager = PrivilegeManager.load(
                emptyImage.getDataInputStream(), masterGlobalStateMgr, null);

        UserPrivilegeCollectionInfo info = (UserPrivilegeCollectionInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_USER_PRIVILEGE_V2);
        followerManager.replayUpdateUserPrivilegeCollection(
                info.getUserIdentity(), info.getPrivilegeCollection(), info.getPluginId(), info.getPluginVersion());
        PrivilegeCollection collection = followerManager.mergePrivilegeCollection(ctx);
        Assert.assertTrue(followerManager.checkTableAction(
                collection, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.SELECT));

        info = (UserPrivilegeCollectionInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_USER_PRIVILEGE_V2);
        followerManager.replayUpdateUserPrivilegeCollection(
                info.getUserIdentity(), info.getPrivilegeCollection(), info.getPluginId(), info.getPluginVersion());
        collection = followerManager.mergePrivilegeCollection(ctx);
        Assert.assertFalse(followerManager.checkTableAction(
                collection, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.SELECT));

        // check image
        PrivilegeManager imageManager = PrivilegeManager.load(
                grantImage.getDataInputStream(), masterGlobalStateMgr, null);
        collection = imageManager.mergePrivilegeCollection(ctx);
        Assert.assertTrue(imageManager.checkTableAction(
                collection, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.SELECT));

        imageManager = PrivilegeManager.load(
                revokeImage.getDataInputStream(), masterGlobalStateMgr, null);
        collection = imageManager.mergePrivilegeCollection(ctx);
        Assert.assertFalse(imageManager.checkTableAction(
                collection, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.SELECT));
    }

    @Test
    public void testRole() throws Exception {
        String sql = "create role test_role";
        PrivilegeManager manager = ctx.getGlobalStateMgr().getPrivilegeManager();
        Assert.assertFalse(manager.checkRoleExists("test_role"));

        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);
        Assert.assertTrue(manager.checkRoleExists("test_role"));

        // can't create twice
        try {
            DDLStmtExecutor.execute(stmt, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Role test_role already exists!"));
        }

        sql = "drop role test_role";
        stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);
        Assert.assertFalse(manager.checkRoleExists("test_role"));

        // can't drop twice
        try {
            DDLStmtExecutor.execute(stmt, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Role test_role doesn't exist!"));
        }
    }

    // used in testPersistRole
    private void assertTableSelectOnTest(PrivilegeManager manager, boolean canSelectTbl0, boolean canSelectTbl1)
            throws PrivilegeException {
        ctx.setCurrentUserIdentity(testUser);
        PrivilegeCollection collection = manager.mergePrivilegeCollection(ctx);
        if (canSelectTbl0) {
            Assert.assertTrue(manager.checkTableAction(
                    collection, DB_NAME, TABLE_NAME_0, PrivilegeTypes.TableActions.SELECT));
        } else {
            Assert.assertFalse(manager.checkTableAction(
                    collection, DB_NAME, TABLE_NAME_0, PrivilegeTypes.TableActions.SELECT));
        }

        if (canSelectTbl1) {
            Assert.assertTrue(manager.checkTableAction(
                    collection, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.SELECT));
        } else {
            Assert.assertFalse(manager.checkTableAction(
                    collection, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.SELECT));
        }
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
    }

    @Test
    public void testPersistRole() throws Exception {
        GlobalStateMgr masterGlobalStateMgr = ctx.getGlobalStateMgr();
        PrivilegeManager masterManager = masterGlobalStateMgr.getPrivilegeManager();
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();

        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        masterManager.save(emptyImage.getDataOutputStream());
        Assert.assertFalse(masterManager.checkRoleExists("test_persist_role0"));
        Assert.assertFalse(masterManager.checkRoleExists("test_persist_role1"));

        // 1. create 2 roles
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        for (int i = 0; i != 2; ++ i) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "create role test_persist_role" + i, ctx), ctx);
            Assert.assertTrue(masterManager.checkRoleExists("test_persist_role" + i));
        }
        UtFrameUtils.PseudoImage createRoleImage = new UtFrameUtils.PseudoImage();
        masterManager.save(createRoleImage.getDataOutputStream());

        // 2. grant select on db.tbl<i> to role
        for (int i = 0; i != 2; ++ i) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "grant select on db.tbl" + i + " to role test_persist_role" + i, ctx), ctx);
        }
        UtFrameUtils.PseudoImage grantPrivsToRoleImage = new UtFrameUtils.PseudoImage();
        masterManager.save(grantPrivsToRoleImage.getDataOutputStream());
        assertTableSelectOnTest(masterManager, false, false);

        // 3. grant test_persist_role0 to test_user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant test_persist_role0 to test_user", ctx), ctx);
        assertTableSelectOnTest(masterManager, true, false);
        UtFrameUtils.PseudoImage grantRoleToUserImage = new UtFrameUtils.PseudoImage();
        masterManager.save(grantRoleToUserImage.getDataOutputStream());

        // 4. grant test_persist_role1 to role test_persist_role0
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant test_persist_role1 to role test_persist_role0", ctx), ctx);
        assertTableSelectOnTest(masterManager, true, true);
        UtFrameUtils.PseudoImage grantRoleToRoleImage = new UtFrameUtils.PseudoImage();
        masterManager.save(grantRoleToRoleImage.getDataOutputStream());

        // 5. revoke test_persist_role1 from role test_persist_role0
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke test_persist_role1 from role test_persist_role0", ctx), ctx);
        assertTableSelectOnTest(masterManager, true, false);
        UtFrameUtils.PseudoImage revokeRoleFromRoleImage = new UtFrameUtils.PseudoImage();
        masterManager.save(revokeRoleFromRoleImage.getDataOutputStream());

        // 6. revoke test_persist_role0 from test_user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke test_persist_role0 from test_user", ctx), ctx);
        assertTableSelectOnTest(masterManager, false, false);
        UtFrameUtils.PseudoImage revokeRoleFromUserImage = new UtFrameUtils.PseudoImage();
        masterManager.save(revokeRoleFromUserImage.getDataOutputStream());

        // 7. drop 2 roles
        for (int i = 0; i != 2; ++ i) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "drop role test_persist_role" + i, ctx), ctx);
            Assert.assertFalse(masterManager.checkRoleExists("test_persist_role" + i));
        }
        UtFrameUtils.PseudoImage dropRoleImage = new UtFrameUtils.PseudoImage();
        masterManager.save(dropRoleImage.getDataOutputStream());

        //
        // start to replay
        //
        PrivilegeManager followerManager = PrivilegeManager.load(
                emptyImage.getDataInputStream(), masterGlobalStateMgr, null);
        Assert.assertFalse(followerManager.checkRoleExists("test_persist_role0"));
        Assert.assertFalse(followerManager.checkRoleExists("test_persist_role1"));

        // 1. replay create 2 roles
        for (int i = 0; i != 2; ++ i) {
            RolePrivilegeCollectionInfo info = (RolePrivilegeCollectionInfo)
                    UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
            followerManager.replayUpdateRolePrivilegeCollection(info);
            Assert.assertTrue(followerManager.checkRoleExists("test_persist_role" + i));
        }
        // 2. replay grant insert on db.tbl<i> to role
        for (int i = 0; i != 2; ++ i) {
            RolePrivilegeCollectionInfo info = (RolePrivilegeCollectionInfo)
                    UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
            followerManager.replayUpdateRolePrivilegeCollection(info);
        }
        // 3. replay grant test_persist_role0 to test_user
        assertTableSelectOnTest(followerManager, false, false);
        {
            UserPrivilegeCollectionInfo info = (UserPrivilegeCollectionInfo)
                    UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_USER_PRIVILEGE_V2);
            followerManager.replayUpdateUserPrivilegeCollection(
                    info.getUserIdentity(), info.getPrivilegeCollection(), info.getPluginId(), info.getPluginVersion());
        }
        assertTableSelectOnTest(followerManager, true, false);

        // 4. replay grant test_persist_role1 to role test_persist_role0
        {
            RolePrivilegeCollectionInfo info = (RolePrivilegeCollectionInfo)
                    UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
            followerManager.replayUpdateRolePrivilegeCollection(info);
        }
        assertTableSelectOnTest(followerManager, true, true);

        // 5. replay revoke test_persist_role1 from role test_persist_role0
        {
            RolePrivilegeCollectionInfo info = (RolePrivilegeCollectionInfo)
                    UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
            followerManager.replayUpdateRolePrivilegeCollection(info);
        }
        assertTableSelectOnTest(followerManager, true, false);

        // 6. replay revoke test_persist_role0 from test_user
        {
            UserPrivilegeCollectionInfo info = (UserPrivilegeCollectionInfo)
                    UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_USER_PRIVILEGE_V2);
            followerManager.replayUpdateUserPrivilegeCollection(
                    info.getUserIdentity(), info.getPrivilegeCollection(), info.getPluginId(), info.getPluginVersion());
        }
        assertTableSelectOnTest(followerManager, false, false);

        // 7. replay drop 2 roles
        for (int i = 0; i != 2; ++ i) {
            RolePrivilegeCollectionInfo info = (RolePrivilegeCollectionInfo)
                    UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_DROP_ROLE_V2);
            followerManager.replayDropRole(info);
            Assert.assertFalse(followerManager.checkRoleExists("test_persist_role" + i));
        }

        //
        // check image
        //
        // 1. check image after create role
        PrivilegeManager imageManager = PrivilegeManager.load(
                createRoleImage.getDataInputStream(), masterGlobalStateMgr, null);
        Assert.assertTrue(imageManager.checkRoleExists("test_persist_role0"));
        Assert.assertTrue(imageManager.checkRoleExists("test_persist_role1"));

        // 2. check image after grant select on db.tbl<i> to role
        imageManager = PrivilegeManager.load(
                grantPrivsToRoleImage.getDataInputStream(), masterGlobalStateMgr, null);
        assertTableSelectOnTest(imageManager, false, false);

        // 3. check image after grant test_persist_role0 to test_user
        imageManager = PrivilegeManager.load(
                grantRoleToUserImage.getDataInputStream(), masterGlobalStateMgr, null);
        assertTableSelectOnTest(imageManager, true, false);

        // 4. check image after grant test_persist_role1 to role test_persist_role0
        imageManager = PrivilegeManager.load(
                grantRoleToRoleImage.getDataInputStream(), masterGlobalStateMgr, null);
        assertTableSelectOnTest(imageManager, true, true);

        // 5. check image after revoke test_persist_role1 from role test_persist_role0
        imageManager = PrivilegeManager.load(
                revokeRoleFromRoleImage.getDataInputStream(), masterGlobalStateMgr, null);
        assertTableSelectOnTest(imageManager, true, false);

        // 6. check image after revoke test_persist_role0 from test_user
        imageManager = PrivilegeManager.load(
                revokeRoleFromUserImage.getDataInputStream(), masterGlobalStateMgr, null);
        assertTableSelectOnTest(imageManager, false, false);

        // 7. check image after drop 2 roles
        imageManager = PrivilegeManager.load(
                dropRoleImage.getDataInputStream(), masterGlobalStateMgr, null);
        Assert.assertFalse(imageManager.checkRoleExists("test_persist_role0"));
        Assert.assertFalse(imageManager.checkRoleExists("test_persist_role1"));
    }

    @Test
    public void testRemoveInvalidateObject() throws Exception {
        PrivilegeManager manager = ctx.getGlobalStateMgr().getPrivilegeManager();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        // 1. add validate entry: select on db.tbl1 to test_user
        String sql = "grant select on db.tbl1 to test_user";
        GrantPrivilegeStmt grantTableStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantTableStmt);
        Assert.assertEquals(1, grantTableStmt.getObjectList().size());
        TablePEntryObject goodTableObject = (TablePEntryObject) grantTableStmt.getObjectList().get(0);
        // 2. add validate entry: create_table + drop on db to test_user
        sql = "grant create_table, drop on db to test_user";
        GrantPrivilegeStmt grantDbStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantDbStmt);
        List<PEntryObject> objects = Arrays.asList(goodTableObject);
        // 3. add invalidate entry: select on invalidatedb.table
        objects = Arrays.asList(new TablePEntryObject(-1, goodTableObject.tableId));
        manager.grantToUser(grantTableStmt.getTypeId(), grantTableStmt.getActionList(), objects, false, testUser);
        // 4. add invalidate entry: select on db.invalidatetable
        objects = Arrays.asList(new TablePEntryObject(goodTableObject.databaseId, -1));
        manager.grantToUser(grantTableStmt.getTypeId(), grantTableStmt.getActionList(), objects, false, testUser);
        // 5. add invalidate entry: create_table, drop on invalidatedb
        objects = Arrays.asList(new DbPEntryObject(-1));
        manager.grantToUser(grantDbStmt.getTypeId(), grantDbStmt.getActionList(), objects, false, testUser);
        // 6. add valid entry: ALL databases
        objects = Arrays.asList(new DbPEntryObject(DbPEntryObject.ALL_DATABASE_ID));
        manager.grantToUser(grantDbStmt.getTypeId(), grantDbStmt.getActionList(), objects, false, testUser);
        // 7. add valid user
        sql = "grant impersonate on root to test_user";
        GrantPrivilegeStmt grantUserStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantUserStmt);
        // 8. add invalidate entry: bad impersonate user
        objects = Arrays.asList(new UserPEntryObject(UserIdentity.createAnalyzedUserIdentWithIp("bad", "%")));
        manager.grantToUser(grantUserStmt.getTypeId(), grantUserStmt.getActionList(), objects, false, testUser);

        // check before clean up:
        System.out.println(GsonUtils.GSON.toJson(manager.userToPrivilegeCollection));
        int numTablePEntries = manager.userToPrivilegeCollection.get(testUser).
                typeToPrivilegeEntryList.get(grantTableStmt.getTypeId()).size();
        int numDbPEntires = manager.userToPrivilegeCollection.get(testUser).
                typeToPrivilegeEntryList.get(grantDbStmt.getTypeId()).size();
        int numUserPEntires = manager.userToPrivilegeCollection.get(testUser).
                typeToPrivilegeEntryList.get(grantUserStmt.getTypeId()).size();

        manager.removeInvalidObject();

        // check after clean up
        System.out.println(GsonUtils.GSON.toJson(manager.userToPrivilegeCollection));
        Assert.assertEquals(numTablePEntries - 2, manager.userToPrivilegeCollection.get(testUser).
                typeToPrivilegeEntryList.get(grantTableStmt.getTypeId()).size());
        Assert.assertEquals(numDbPEntires - 1, manager.userToPrivilegeCollection.get(testUser).
                typeToPrivilegeEntryList.get(grantDbStmt.getTypeId()).size());
        Assert.assertEquals(numUserPEntires - 1, manager.userToPrivilegeCollection.get(testUser).
                typeToPrivilegeEntryList.get(grantUserStmt.getTypeId()).size());
    }

    @Test
    public void testGrantAll() throws Exception {
        UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
        ctx.setCurrentUserIdentity(testUser);
        PrivilegeManager manager = ctx.getGlobalStateMgr().getPrivilegeManager();
        Assert.assertFalse(PrivilegeManager.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.SELECT));

        List<List<String>> sqls = Arrays.asList(
                Arrays.asList(
                        "GRANT SELECT ON ALL TABLES IN ALL DATABASES TO test_user",
                        "REVOKE SELECT ON ALL TABLES IN ALL DATABASES FROM test_user"),
                Arrays.asList(
                        "GRANT SELECT ON ALL TABLES IN DATABASE db TO test_user",
                        "REVOKE SELECT ON ALL TABLES IN DATABASE db FROM test_user")
        );
        for (List<String> sqlPair : sqls) {
            ctx.setCurrentUserIdentity(UserIdentity.ROOT);
            GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sqlPair.get(0), ctx);
            manager.grant(grantStmt);
            ctx.setCurrentUserIdentity(testUser);
            Assert.assertTrue(PrivilegeManager.checkTableAction(
                    ctx, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.SELECT));

            ctx.setCurrentUserIdentity(UserIdentity.ROOT);
            RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sqlPair.get(1), ctx);
            manager.revoke(revokeStmt);
            ctx.setCurrentUserIdentity(testUser);
            Assert.assertFalse(PrivilegeManager.checkTableAction(
                    ctx, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.SELECT));
        }

        Assert.assertFalse(PrivilegeManager.checkDbAction(
                ctx, DB_NAME, PrivilegeTypes.DbActions.CREATE_TABLE));

        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT CREATE_TABLE ON ALL DATABASES TO test_user", ctx);
        manager.grant(grantStmt);

        ctx.setCurrentUserIdentity(testUser);
        Assert.assertTrue(PrivilegeManager.checkDbAction(
                ctx, DB_NAME, PrivilegeTypes.DbActions.CREATE_TABLE));

        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "REVOKE CREATE_TABLE ON ALL DATABASES FROM test_user", ctx);
        manager.revoke(revokeStmt);

        ctx.setCurrentUserIdentity(testUser);
        Assert.assertFalse(PrivilegeManager.checkDbAction(
                ctx, DB_NAME, PrivilegeTypes.DbActions.CREATE_TABLE));
    }

    @Test
    public void testImpersonate() throws Exception {
        PrivilegeManager manager = ctx.getGlobalStateMgr().getPrivilegeManager();

        ctx.setCurrentUserIdentity(testUser);
        Assert.assertFalse(manager.canExecuteAs(ctx, UserIdentity.ROOT));

        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT IMPERSONATE ON USER root, test_user TO test_user", ctx);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        manager.grant(grantStmt);

        ctx.setCurrentUserIdentity(testUser);
        Assert.assertTrue(manager.canExecuteAs(ctx, UserIdentity.ROOT));

        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "REVOKE IMPERSONATE ON root FROM test_user", ctx);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        manager.revoke(revokeStmt);

        ctx.setCurrentUserIdentity(testUser);
        Assert.assertFalse(manager.canExecuteAs(ctx, UserIdentity.ROOT));
    }

    @Test
    public void testShowDbsTables() throws Exception {
        PrivilegeManager manager = ctx.getGlobalStateMgr().getPrivilegeManager();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);

        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user user_with_table_priv", ctx);
        ctx.getGlobalStateMgr().getAuthenticationManager().createUser(createUserStmt);
        String sql = "grant select on db.tbl1 to user_with_table_priv";
        GrantPrivilegeStmt grantTableStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantTableStmt);
        UserIdentity userWithTablePriv = UserIdentity.createAnalyzedUserIdentWithIp("user_with_table_priv", "%");

        createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user user_with_db_priv", ctx);
        ctx.getGlobalStateMgr().getAuthenticationManager().createUser(createUserStmt);
        sql = "grant drop on db to user_with_db_priv";
        grantTableStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantTableStmt);
        UserIdentity userWithDbPriv = UserIdentity.createAnalyzedUserIdentWithIp("user_with_db_priv", "%");


        // show tables in db:
        // root can see two tables
        ShowTableStmt showTableStmt = new ShowTableStmt("db", false, null);
        ShowExecutor executor = new ShowExecutor(ctx, showTableStmt);
        ShowResultSet resultSet = executor.execute();
        Set<String> allTables = resultSet.getResultRows().stream().map(k -> k.get(0)).collect(Collectors.toSet());
        Assert.assertEquals(new HashSet<>(Arrays.asList("tbl0", "tbl1", "tbl2", "tbl3")), allTables);

        // user with table priv can only see tbl1
        ctx.setCurrentUserIdentity(userWithTablePriv);
        executor = new ShowExecutor(ctx, showTableStmt);
        resultSet = executor.execute();
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("tbl1", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());

        // show databases
        ShowDbStmt showDbStmt = new ShowDbStmt(null);

        // root can see db && db1
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        executor = new ShowExecutor(ctx, showDbStmt);
        resultSet = executor.execute();
        Set<String> set = new HashSet<>();
        while (resultSet.next()) {
            set.add(resultSet.getString(0));
        }
        Assert.assertTrue(set.contains("db"));
        Assert.assertTrue(set.contains("db1"));

        // user with table priv can only see db
        ctx.setCurrentUserIdentity(userWithTablePriv);
        executor = new ShowExecutor(ctx, showDbStmt);
        resultSet = executor.execute();
        set.clear();
        while (resultSet.next()) {
            set.add(resultSet.getString(0));
        }
        Assert.assertTrue(set.contains("db"));
        Assert.assertFalse(set.contains("db1"));

        // user with table priv can only see db
        ctx.setCurrentUserIdentity(userWithDbPriv);
        executor = new ShowExecutor(ctx, showDbStmt);
        resultSet = executor.execute();
        set.clear();
        while (resultSet.next()) {
            set.add(resultSet.getString(0));
        }
        Assert.assertTrue(set.contains("db"));
        Assert.assertFalse(set.contains("db1"));
    }

    private void assertDbActionsOnTest(boolean canCreateTable, boolean canDrop, UserIdentity testUser) {
        ctx.setCurrentUserIdentity(testUser);
        if (canCreateTable) {
            Assert.assertTrue(PrivilegeManager.checkDbAction(ctx, "db", PrivilegeTypes.DbActions.CREATE_TABLE));
        } else {
            Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db", PrivilegeTypes.DbActions.CREATE_TABLE));
        }

        if (canDrop) {
            Assert.assertTrue(PrivilegeManager.checkDbAction(ctx, "db", PrivilegeTypes.DbActions.DROP));
        } else {
            Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db", PrivilegeTypes.DbActions.DROP));
        }
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
    }

    @Test
    public void testGrantRoleToUser() throws Exception {
        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user test_role_user", ctx);
        ctx.getGlobalStateMgr().getAuthenticationManager().createUser(createUserStmt);
        UserIdentity testUser = createUserStmt.getUserIdent();
        PrivilegeManager manager = ctx.getGlobalStateMgr().getPrivilegeManager();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);

        // grant create_table on database db to role1
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create role role1;", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant create_table on database db to role role1;", ctx), ctx);

        // can't create_table
        assertDbActionsOnTest(false, false, testUser);

        // grant role1 to test_user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role1 to test_role_user", ctx), ctx);

        // can create_table but can't drop
        assertDbActionsOnTest(true, false, testUser);

        // grant role2 to test_user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create role role2;", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant drop on database db to role role2;", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role2 to test_role_user;", ctx), ctx);

        // can create_table & drop
        assertDbActionsOnTest(true, true, testUser);

        // grant drop to test_user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant drop on database db to test_role_user;", ctx), ctx);

        // still, can create_table & drop
        assertDbActionsOnTest(true, true, testUser);

        // revoke role1 from test_user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke role1 from test_role_user;", ctx), ctx);

        // can drop but can't create_table
        assertDbActionsOnTest(false, true, testUser);

        // grant role1 to test_user; revoke create_table from role1
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role1 to test_role_user", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke create_table on database db from role role1", ctx), ctx);

        // can drop but can't create_table
        assertDbActionsOnTest(false, true, testUser);

        // revoke empty role role1
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke role1 from test_role_user;", ctx), ctx);

        // can drop but can't create_table
        assertDbActionsOnTest(false, true, testUser);

        // revoke role2 from test_user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke role2 from test_role_user;", ctx), ctx);

        // can drop
        assertDbActionsOnTest(false, true, testUser);

        // grant role2 to test_user; revoke drop from role2
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role2 to test_role_user", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke drop on database db from role role2", ctx), ctx);

        // can drop
        assertDbActionsOnTest(false, true, testUser);

        // grant drop on role2; revoke drop from user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant drop on database db to role role2", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke drop on database db from test_role_user", ctx), ctx);

        assertDbActionsOnTest(false, true, testUser);

        // revoke role2 from test_user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke role2 from test_role_user;", ctx), ctx);

        // can't drop
        assertDbActionsOnTest(false, false, testUser);
    }

    @Test
    public void testRoleInheritanceDepth() throws Exception {
        PrivilegeManager manager = ctx.getGlobalStateMgr().getPrivilegeManager();
        // create role0 ~ role4
        long[] roleIds = new long[5];
        for (int i = 0; i != 5; ++ i) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    String.format("create role role%d;", i), ctx), ctx);
            roleIds[i] = manager.getRoleIdByNameNoLock("role" + i);
        }
        // create user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("create user user_test_role_inheritance"), ctx), ctx);

        // role0 -> role1 -> role2
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role0 to role role1", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role1 to role role2", ctx), ctx);

        // role inheritance depth
        Assert.assertEquals(0, manager.getMaxRoleInheritanceDepthInner(0, roleIds[2]));
        Assert.assertEquals(1, manager.getMaxRoleInheritanceDepthInner(0, roleIds[1]));
        Assert.assertEquals(2, manager.getMaxRoleInheritanceDepthInner(0, roleIds[0]));
        // role predecessor
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0])),
                manager.getAllPredecessorsUnlocked(roleIds[0]));
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1])),
                manager.getAllPredecessorsUnlocked(roleIds[1]));
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1], roleIds[2])),
                manager.getAllPredecessorsUnlocked(roleIds[2]));

        // role0 -> role1 -> role2
        //    \       ^
        //     \      |
        //      \-> role3
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role0 to role role3", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role3 to role role1", ctx), ctx);
        // role inheritance depth
        Assert.assertEquals(3, manager.getMaxRoleInheritanceDepthInner(0, roleIds[0]));
        Assert.assertEquals(1, manager.getMaxRoleInheritanceDepthInner(0, roleIds[1]));
        Assert.assertEquals(0, manager.getMaxRoleInheritanceDepthInner(0, roleIds[2]));
        Assert.assertEquals(2, manager.getMaxRoleInheritanceDepthInner(0, roleIds[3]));
        // role predecessor
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0])),
                manager.getAllPredecessorsUnlocked(roleIds[0]));
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1], roleIds[3])),
                manager.getAllPredecessorsUnlocked(roleIds[1]));
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1], roleIds[2], roleIds[3])),
                manager.getAllPredecessorsUnlocked(roleIds[2]));
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[3])),
                manager.getAllPredecessorsUnlocked(roleIds[3]));

        // role0 -> role1 -> role2 -> role4
        //    \       ^
        //     \      |
        //      \-> role3
        int oldValue = Config.privilege_max_role_depth;
        // simulate exception
        Config.privilege_max_role_depth = 3;
        try {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "grant role2 to role role4", ctx), ctx);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("role inheritance depth for role0"));
            Assert.assertTrue(e.getMessage().contains("is 4 > 3"));
        }
        Assert.assertEquals(3, manager.getMaxRoleInheritanceDepthInner(0, roleIds[0]));
        Config.privilege_max_role_depth = oldValue;
        // normal cases
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role2 to role role4", ctx), ctx);
        Assert.assertEquals(4, manager.getMaxRoleInheritanceDepthInner(0, roleIds[0]));

        // grant too many roles to user
        oldValue = Config.privilege_max_total_roles_per_user;
        Config.privilege_max_total_roles_per_user = 4;
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("user_test_role_inheritance", "%");
        UserPrivilegeCollection collection = manager.getUserPrivilegeCollectionUnlocked(user);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role1 to user_test_role_inheritance", ctx), ctx);
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1], roleIds[3])),
                manager.getAllPredecessorsUnlocked(collection));
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role0 to user_test_role_inheritance", ctx), ctx);
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1], roleIds[3])),
                manager.getAllPredecessorsUnlocked(collection));
        // exception:
        try {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "grant role4 to user_test_role_inheritance", ctx), ctx);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("'user_test_role_inheritance'@'%' has total 5 predecessor roles > 4"));
        }
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1], roleIds[3])),
                manager.getAllPredecessorsUnlocked(collection));
        // normal grant
        Config.privilege_max_total_roles_per_user = oldValue;
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role4 to user_test_role_inheritance", ctx), ctx);
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1], roleIds[2], roleIds[3], roleIds[4])),
                manager.getAllPredecessorsUnlocked(collection));


        // grant role with circle: bad case
        // grant role4 to role0
        //    ---------------------------
        //   |                           |
        //   V                           |
        // role0 -> role1 -> role2 -> role4
        //    \       ^
        //     \      |
        //      \-> role3
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1], roleIds[2], roleIds[3], roleIds[4])),
                manager.getAllPredecessorsUnlocked(roleIds[4]));
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0])), manager.getAllPredecessorsUnlocked(roleIds[0]));
        try {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "grant role4 to role role0", ctx), ctx);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("role role0"));
            Assert.assertTrue(e.getMessage().contains("is already a predecessor role of role4"));
        }
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1], roleIds[2], roleIds[3], roleIds[4])),
                manager.getAllPredecessorsUnlocked(roleIds[4]));
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0])), manager.getAllPredecessorsUnlocked(roleIds[0]));
    }

    private void assertTableSelectOnTest(UserIdentity userIdentity, boolean ... canSelectOnTbls) throws Exception {
        boolean[] args = canSelectOnTbls;
        Assert.assertEquals(4, args.length);
        ctx.setCurrentUserIdentity(userIdentity);
        PrivilegeManager manager = ctx.getGlobalStateMgr().getPrivilegeManager();
        PrivilegeCollection collection = manager.mergePrivilegeCollection(ctx);
        for (int i = 0; i != 4; ++ i) {
            if (args[i]) {
                Assert.assertTrue("cannot select tbl" + i, manager.checkTableAction(
                        collection, DB_NAME, "tbl" + i, PrivilegeTypes.TableActions.SELECT));
            } else {
                Assert.assertFalse("can select tbl" + i, manager.checkTableAction(
                        collection, DB_NAME, "tbl" + i, PrivilegeTypes.TableActions.SELECT));
            }
        }
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
    }

    @Test
    public void dropRoleWithInheritance() throws Exception {
        PrivilegeManager manager = ctx.getGlobalStateMgr().getPrivilegeManager();
        // create role0 ~ role3
        long[] roleIds = new long[4];
        for (int i = 0; i != 4; ++ i) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    String.format("create role test_drop_role_%d;", i), ctx), ctx);
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "grant select on db.tbl" + i + " to role test_drop_role_" + i, ctx), ctx);
            roleIds[i] = manager.getRoleIdByNameNoLock("test_drop_role_" + i);
        }
        // create user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("create user user_test_drop_role_inheritance"), ctx), ctx);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("user_test_drop_role_inheritance", "%");
        UserPrivilegeCollection collection = manager.getUserPrivilegeCollectionUnlocked(user);

        // role0 -> role1[user] -> role2
        // role3[user]
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant test_drop_role_0 to role test_drop_role_1", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant test_drop_role_1 to role test_drop_role_2", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant test_drop_role_1 to user_test_drop_role_inheritance", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant test_drop_role_3 to user_test_drop_role_inheritance", ctx), ctx);

        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1], roleIds[3])),
                manager.getAllPredecessorsUnlocked(collection));
        assertTableSelectOnTest(user, true, true, false, true);

        // role0 -> role1[user] -> role2
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "drop role test_drop_role_3;", ctx), ctx);
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1])),
                manager.getAllPredecessorsUnlocked(collection));
        Assert.assertEquals(2, manager.getMaxRoleInheritanceDepthInner(0, roleIds[0]));
        Assert.assertEquals(1, manager.getMaxRoleInheritanceDepthInner(0, roleIds[1]));
        assertTableSelectOnTest(user, true, true, false, false);

        // role0 -> role1[user]
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "drop role test_drop_role_2;", ctx), ctx);
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1])),
                manager.getAllPredecessorsUnlocked(collection));
        Assert.assertEquals(1, manager.getMaxRoleInheritanceDepthInner(0, roleIds[0]));
        Assert.assertEquals(0, manager.getMaxRoleInheritanceDepthInner(0, roleIds[1]));
        assertTableSelectOnTest(user, true, true, false, false);

        // role1[user]
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "drop role test_drop_role_0;", ctx), ctx);
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[1])),
                manager.getAllPredecessorsUnlocked(collection));
        Assert.assertEquals(0, manager.getMaxRoleInheritanceDepthInner(0, roleIds[1]));
        assertTableSelectOnTest(user, false, true, false, false);

        // clean invalidate roles
        int userNumRoles = manager.userToPrivilegeCollection.get(user).getAllRoles().size();
        int role1NumParents = manager.roleIdToPrivilegeCollection.get(roleIds[1]).getParentRoleIds().size();
        int role1NumSubs = manager.roleIdToPrivilegeCollection.get(roleIds[1]).getSubRoleIds().size();
        manager.removeInvalidObject();
        Assert.assertEquals(userNumRoles - 1, manager.userToPrivilegeCollection.get(user).getAllRoles().size());
        Assert.assertEquals(role1NumParents - 1,
                manager.roleIdToPrivilegeCollection.get(roleIds[1]).getParentRoleIds().size());
        Assert.assertEquals(role1NumSubs - 1,
                manager.roleIdToPrivilegeCollection.get(roleIds[1]).getSubRoleIds().size());

        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[1])), manager.getAllPredecessorsUnlocked(collection));
        assertTableSelectOnTest(user, false, true, false, false);
    }

    @Test
    public void testSetRole() throws Exception {
        PrivilegeManager manager = ctx.getGlobalStateMgr().getPrivilegeManager();
        // create user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("create user user_test_set_role"), ctx), ctx);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("user_test_set_role", "%");
        // create role0 ~ role3
        // grant select on tblx to rolex
        // grant role0, role1, role2 to user
        long[] roleIds = new long[4];
        for (int i = 0; i != 4; ++ i) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    String.format("create role test_set_role_%d;", i), ctx), ctx);
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "grant select on db.tbl" + i + " to role test_set_role_" + i, ctx), ctx);
            if (i != 3) {
                DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                        "grant test_set_role_" + i + " to user_test_set_role", ctx), ctx);
            }
            roleIds[i] = manager.getRoleIdByNameNoLock("test_set_role_" + i);
        }

        // default: user can select all tables
        assertTableSelectOnTest(user, true, true, true, false);

        // set one role
        ctx.setCurrentUserIdentity(user);
        Assert.assertNull(ctx.getCurrentRoleIds());
        new StmtExecutor(ctx, UtFrameUtils.parseStmtWithNewParser(
                String.format("set role 'test_set_role_0'"), ctx)).execute();
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0])), ctx.getCurrentRoleIds());
        assertTableSelectOnTest(user, true, false, false, false);

        // set on other 3 roles
        ctx.setCurrentUserIdentity(user);
        new StmtExecutor(ctx, UtFrameUtils.parseStmtWithNewParser(
                String.format("set role 'test_set_role_1', 'test_set_role_2'"), ctx)).execute();
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[1], roleIds[2])), ctx.getCurrentRoleIds());
        assertTableSelectOnTest(user, false, true, true, false);

        // bad case: role not exists
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("create role bad_role", ctx), ctx);
        SetRoleStmt stmt = (SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                "set role 'test_set_role_1', 'test_set_role_2', 'bad_role'", ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("drop role bad_role", ctx), ctx);
        ctx.setCurrentUserIdentity(user);
        try {
            SetRoleExecutor.execute(stmt, ctx);
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Cannot find role bad_role"));
        }
        try {
            SetRoleExecutor.execute((SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                    "set role 'test_set_role_1', 'test_set_role_3'", ctx), ctx);
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Role test_set_role_3 is not granted"));
        }

        try {
            SetRoleExecutor.execute((SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                    "set role all except 'test_set_role_1', 'test_set_role_3'", ctx), ctx);
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Role test_set_role_3 is not granted"));
        }

        // drop role1
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "drop role test_set_role_1;", ctx), ctx);
        assertTableSelectOnTest(user, false, false, true, false);

        ctx.setCurrentUserIdentity(user);
        new StmtExecutor(ctx, UtFrameUtils.parseStmtWithNewParser(
                String.format("set role all"), ctx)).execute();
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[2])), ctx.getCurrentRoleIds());
        assertTableSelectOnTest(user, true, false, true, false);

        ctx.setCurrentUserIdentity(user);
        new StmtExecutor(ctx, UtFrameUtils.parseStmtWithNewParser(
                String.format("set role all except 'test_set_role_2'"), ctx)).execute();
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0])), ctx.getCurrentRoleIds());
        assertTableSelectOnTest(user, true, false, false, false);

        // predecessors
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant test_set_role_3 to role test_set_role_0;", ctx), ctx);
        assertTableSelectOnTest(user, true, false, false, true);
    }
}
