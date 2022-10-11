// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.RolePrivilegeCollectionInfo;
import com.starrocks.persist.UserPrivilegeCollectionInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class PrivilegeManagerTest {
    private static final List<String> DB_TBL_TOKENS = Arrays.asList("db", "tbl");
    private ConnectContext ctx;

    @Before
    public void setUp() throws Exception {
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.setUpForPersistTest();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        String createTblStmtStr = "create table db.tbl(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("db");
        starRocksAssert.withTable(createTblStmtStr);
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
        UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
        ctx.setCurrentUserIdentity(testUser);
        ctx.setQualifiedUser(testUser.getQualifiedUser());

        PrivilegeManager manager = ctx.getGlobalStateMgr().getPrivilegeManager();
        Assert.assertFalse(manager.hasType(ctx, PrivilegeTypes.TABLE.toString()));
        Assert.assertFalse(manager.checkAnyObject(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString()));
        Assert.assertFalse(manager.check(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString(),
                DB_TBL_TOKENS));

        String sql = "grant select on table db.tbl to test_user";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantStmt);

        Assert.assertTrue(manager.hasType(ctx, PrivilegeTypes.TABLE.toString()));
        Assert.assertTrue(manager.checkAnyObject(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString()));
        Assert.assertTrue(manager.check(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString(),
                DB_TBL_TOKENS));

        sql = "revoke select on db.tbl from test_user";
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.revoke(revokeStmt);

        Assert.assertFalse(manager.hasType(ctx, PrivilegeTypes.TABLE.toString()));
        Assert.assertFalse(manager.checkAnyObject(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString()));
        Assert.assertFalse(manager.check(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString(),
                DB_TBL_TOKENS));

        // grant many priveleges
        sql = "grant select, insert, delete on table db.tbl to test_user";
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantStmt);
        Assert.assertTrue(manager.check(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString(),
                DB_TBL_TOKENS));
        Assert.assertTrue(manager.check(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.INSERT.toString(),
                DB_TBL_TOKENS));
        Assert.assertTrue(manager.check(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.DELETE.toString(),
                DB_TBL_TOKENS));

        // revoke only select
        sql = "revoke select on db.tbl from test_user";
        revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.revoke(revokeStmt);
        Assert.assertFalse(manager.check(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString(),
                DB_TBL_TOKENS));
        Assert.assertTrue(manager.check(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.INSERT.toString(),
                DB_TBL_TOKENS));
        Assert.assertTrue(manager.check(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.DELETE.toString(),
                DB_TBL_TOKENS));

        // revoke all
        sql = "revoke insert, delete, select on table db.tbl from test_user";
        revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.revoke(revokeStmt);
        Assert.assertFalse(manager.check(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString(),
                DB_TBL_TOKENS));
        Assert.assertFalse(manager.check(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.INSERT.toString(),
                DB_TBL_TOKENS));
        Assert.assertFalse(manager.check(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.DELETE.toString(),
                DB_TBL_TOKENS));
    }

    @Test
    public void testPersist() throws Exception {
        GlobalStateMgr masterGlobalStateMgr = ctx.getGlobalStateMgr();
        PrivilegeManager masterManager = masterGlobalStateMgr.getPrivilegeManager();
        UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");

        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        masterManager.save(emptyImage.getDataOutputStream());

        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        String sql = "grant select on db.tbl to test_user";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.grant(grantStmt);
        ctx.setCurrentUserIdentity(testUser);
        Assert.assertTrue(masterManager.check(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString(),
                DB_TBL_TOKENS));
        UtFrameUtils.PseudoImage grantImage = new UtFrameUtils.PseudoImage();
        masterManager.save(grantImage.getDataOutputStream());

        sql = "revoke select on db.tbl from test_user";
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.revoke(revokeStmt);
        ctx.setCurrentUserIdentity(testUser);
        Assert.assertFalse(masterManager.check(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString(),
                DB_TBL_TOKENS));
        UtFrameUtils.PseudoImage revokeImage = new UtFrameUtils.PseudoImage();
        masterManager.save(revokeImage.getDataOutputStream());

        // start to replay
        PrivilegeManager followerManager = PrivilegeManager.load(
                emptyImage.getDataInputStream(), masterGlobalStateMgr, null);

        UserPrivilegeCollectionInfo info = (UserPrivilegeCollectionInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_USER_PRIVILEGE_V2);
        followerManager.replayUpdateUserPrivilegeCollection(
                info.getUserIdentity(), info.getPrivilegeCollection(), info.getPluginId(), info.getPluginVersion());
        Assert.assertTrue(followerManager.check(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString(),
                DB_TBL_TOKENS));

        info = (UserPrivilegeCollectionInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_USER_PRIVILEGE_V2);
        followerManager.replayUpdateUserPrivilegeCollection(
                info.getUserIdentity(), info.getPrivilegeCollection(), info.getPluginId(), info.getPluginVersion());
        Assert.assertFalse(followerManager.check(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString(),
                DB_TBL_TOKENS));

        // check image
        ctx.setCurrentUserIdentity(testUser);
        PrivilegeManager imageManager = PrivilegeManager.load(
                grantImage.getDataInputStream(), masterGlobalStateMgr, null);
        Assert.assertTrue(imageManager.check(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString(),
                DB_TBL_TOKENS));
        imageManager = PrivilegeManager.load(
                revokeImage.getDataInputStream(), masterGlobalStateMgr, null);
        Assert.assertFalse(imageManager.check(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString(),
                DB_TBL_TOKENS));

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
            Assert.assertTrue(e.getMessage().contains("Role test_role already exists! id ="));
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
            Assert.assertTrue(e.getMessage().contains("Role test_role doesn't exist! id ="));
        }
    }

    @Test
    public void testPersistRole() throws Exception {
        GlobalStateMgr masterGlobalStateMgr = ctx.getGlobalStateMgr();
        PrivilegeManager masterManager = masterGlobalStateMgr.getPrivilegeManager();
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();

        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        masterManager.save(emptyImage.getDataOutputStream());
        Assert.assertFalse(masterManager.checkRoleExists("test_role"));

        String sql = "create role test_role";
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        CreateRoleStmt createStmt = (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.createRole(createStmt);
        Assert.assertTrue(masterManager.checkRoleExists("test_role"));

        UtFrameUtils.PseudoImage createRoleImage = new UtFrameUtils.PseudoImage();
        masterManager.save(createRoleImage.getDataOutputStream());

        sql = "drop role test_role";
        DropRoleStmt dropRoleStmt = (DropRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.dropRole(dropRoleStmt);
        Assert.assertFalse(masterManager.checkRoleExists("test_role"));

        UtFrameUtils.PseudoImage dropRoleImage = new UtFrameUtils.PseudoImage();
        masterManager.save(dropRoleImage.getDataOutputStream());

        // start to replay
        PrivilegeManager followerManager = PrivilegeManager.load(
                emptyImage.getDataInputStream(), masterGlobalStateMgr, null);
        Assert.assertFalse(followerManager.checkRoleExists("test_role"));

        RolePrivilegeCollectionInfo info = (RolePrivilegeCollectionInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
        followerManager.replayUpdateRolePrivilegeCollection(
                info.getRoleId(), info.getPrivilegeCollection(), info.getPluginId(), info.getPluginVersion());
        Assert.assertTrue(followerManager.checkRoleExists("test_role"));

        info = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(
                OperationType.OP_DROP_ROLE_V2);
        followerManager.replayDropRole(
                info.getRoleId(), info.getPrivilegeCollection(), info.getPluginId(), info.getPluginVersion());
        Assert.assertFalse(followerManager.checkRoleExists("test_role"));

        // check image
        PrivilegeManager imageManager = PrivilegeManager.load(
                createRoleImage.getDataInputStream(), masterGlobalStateMgr, null);
        Assert.assertTrue(imageManager.checkRoleExists("test_role"));
        imageManager = PrivilegeManager.load(
                dropRoleImage.getDataInputStream(), masterGlobalStateMgr, null);
        Assert.assertFalse(imageManager.checkRoleExists("test_role"));
    }

    @Test
    public void testRemoveInvalidateObject() throws Exception {
        PrivilegeManager manager = ctx.getGlobalStateMgr().getPrivilegeManager();
        UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        // 1. add validate entry: select on db.tbl to test_user
        String sql = "grant select on db.tbl to test_user";
        GrantPrivilegeStmt grantTableStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantTableStmt);
        TablePEntryObject goodTableObject = (TablePEntryObject) grantTableStmt.getObject();
        // 2. add validate entry: create_table + drop on db to test_user
        sql = "grant create_table, drop on db to test_user";
        GrantPrivilegeStmt grantDbStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantDbStmt);
        // 3. add invalidate entry: select on db.tbl to invalidate user
        UserIdentity badUser = UserIdentity.createAnalyzedUserIdentWithIp("xxx", "10.1.1.1");
        List<PEntryObject> objects = Arrays.asList(goodTableObject);
        manager.onCreateUser(badUser);
        manager.grantToUser(grantTableStmt.getTypeId(), grantTableStmt.getActionList(), objects, false, badUser);
        // 4. add invalidate entry: select on invalidatedb.table
        objects = Arrays.asList(new TablePEntryObject(-1, goodTableObject.id));
        manager.grantToUser(grantTableStmt.getTypeId(), grantTableStmt.getActionList(), objects, false, testUser);
        // 5. add invalidate entry: select on db.invalidatetable
        objects = Arrays.asList(new TablePEntryObject(goodTableObject.databaseId, -1));
        manager.grantToUser(grantTableStmt.getTypeId(), grantTableStmt.getActionList(), objects, false, testUser);
        // 6. add invalidate entry: create_table, drop on invalidatedb
        objects = Arrays.asList(new DbPEntryObject(-1));
        manager.grantToUser(grantDbStmt.getTypeId(), grantDbStmt.getActionList(), objects, false, testUser);

        // check before clean up:
        System.out.println(GsonUtils.GSON.toJson(manager.userToPrivilegeCollection));
        int numUser = manager.userToPrivilegeCollection.size();
        int numTablePEntries = manager.userToPrivilegeCollection.get(testUser).
                typeToPrivilegeEntryList.get(grantTableStmt.getTypeId()).size();
        int numDbPentires = manager.userToPrivilegeCollection.get(testUser).
                typeToPrivilegeEntryList.get(grantDbStmt.getTypeId()).size();

        manager.removeInvalidObject();

        // check after clean up
        System.out.println(GsonUtils.GSON.toJson(manager.userToPrivilegeCollection));
        Assert.assertEquals(numUser - 1, manager.userToPrivilegeCollection.size());
        Assert.assertEquals(numTablePEntries - 2, manager.userToPrivilegeCollection.get(testUser).
                typeToPrivilegeEntryList.get(grantTableStmt.getTypeId()).size());
        Assert.assertEquals(numDbPentires - 1, manager.userToPrivilegeCollection.get(testUser).
                typeToPrivilegeEntryList.get(grantDbStmt.getTypeId()).size());
    }

}
