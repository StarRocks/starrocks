// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.RolePrivilegeCollectionInfo;
import com.starrocks.persist.UserPrivilegeCollectionInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
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

public class PrivilegeManagerTest {
    private ConnectContext ctx;
    private static final String DB_NAME = "db";
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
        // create db.tbl0, db1.tbl1
        String createTblStmtStr = "(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        starRocksAssert.withDatabase(DB_NAME);
        starRocksAssert.withDatabase(DB_NAME + "1");
        for (int i = 0; i < 2; ++ i) {
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
        System.err.println(sql);
        System.err.println(GsonUtils.GSON.toJson(manager.userToPrivilegeCollection.get(testUser)));

        ctx.setCurrentUserIdentity(testUser);
        Assert.assertTrue(PrivilegeManager.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeTypes.TableActions.SELECT));
        Assert.assertTrue(PrivilegeManager.checkAnyActionInDb(ctx, DB_NAME));
        Assert.assertTrue(PrivilegeManager.checkAnyActionInTable(ctx, DB_NAME, TABLE_NAME_1));

        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        sql = "revoke select on db.tbl1 from test_user";
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.revoke(revokeStmt);
        System.err.println(sql);
        System.err.println(GsonUtils.GSON.toJson(manager.userToPrivilegeCollection.get(testUser)));

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
        // 3. add invalidate entry: select on db.tbl1 to invalidate user
        UserIdentity badUser = UserIdentity.createAnalyzedUserIdentWithIp("xxx", "10.1.1.1");
        List<PEntryObject> objects = Arrays.asList(goodTableObject);
        manager.onCreateUser(badUser);
        manager.grantToUser(grantTableStmt.getTypeId(), grantTableStmt.getActionList(), objects, false, badUser);
        // 4. add invalidate entry: select on invalidatedb.table
        objects = Arrays.asList(new TablePEntryObject(-1, goodTableObject.tableId));
        manager.grantToUser(grantTableStmt.getTypeId(), grantTableStmt.getActionList(), objects, false, testUser);
        // 5. add invalidate entry: select on db.invalidatetable
        objects = Arrays.asList(new TablePEntryObject(goodTableObject.databaseId, -1));
        manager.grantToUser(grantTableStmt.getTypeId(), grantTableStmt.getActionList(), objects, false, testUser);
        // 6. add invalidate entry: create_table, drop on invalidatedb
        objects = Arrays.asList(new DbPEntryObject(-1));
        manager.grantToUser(grantDbStmt.getTypeId(), grantDbStmt.getActionList(), objects, false, testUser);
        // 7. add valid entry: ALL databases
        objects = Arrays.asList(new DbPEntryObject(DbPEntryObject.ALL_DATABASE_ID));
        manager.grantToUser(grantDbStmt.getTypeId(), grantDbStmt.getActionList(), objects, false, testUser);
        // 8. add valid user
        sql = "grant impersonate on root to test_user";
        GrantPrivilegeStmt grantUserStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantUserStmt);
        // 8. add invalidate entry: bad user
        objects = Arrays.asList(new UserPEntryObject(UserIdentity.createAnalyzedUserIdentWithIp("bad", "%")));
        manager.grantToUser(grantUserStmt.getTypeId(), grantUserStmt.getActionList(), objects, false, testUser);

        // check before clean up:
        System.out.println(GsonUtils.GSON.toJson(manager.userToPrivilegeCollection));
        int numUser = manager.userToPrivilegeCollection.size();
        int numTablePEntries = manager.userToPrivilegeCollection.get(testUser).
                typeToPrivilegeEntryList.get(grantTableStmt.getTypeId()).size();
        int numDbPEntires = manager.userToPrivilegeCollection.get(testUser).
                typeToPrivilegeEntryList.get(grantDbStmt.getTypeId()).size();
        int numUserPEntires = manager.userToPrivilegeCollection.get(testUser).
                typeToPrivilegeEntryList.get(grantUserStmt.getTypeId()).size();

        manager.removeInvalidObject();

        // check after clean up
        System.out.println(GsonUtils.GSON.toJson(manager.userToPrivilegeCollection));
        Assert.assertEquals(numUser - 1, manager.userToPrivilegeCollection.size());
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
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("tbl0", resultSet.getString(0));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("tbl1", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());

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

}
