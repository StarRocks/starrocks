// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.authentication;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.persist.AlterUserInfo;
import com.starrocks.persist.CreateUserInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class AuthenticationManagerTest {
    static ConnectContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
    }

    @AfterClass
    public static void teardown() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testInitDefault() throws Exception {
        AuthenticationManager manager = ctx.getGlobalStateMgr().getAuthenticationManager();
        Assert.assertTrue(manager.doesUserExist(UserIdentity.ROOT));
        Assert.assertFalse(manager.doesUserExist(UserIdentity.createAnalyzedUserIdentWithIp("fake", "%")));
        Assert.assertEquals(new UserProperty().getMaxConn(), manager.getMaxConn(AuthenticationManager.ROOT_USER));
    }

    @Test
    public void testCreateUserPersist() throws Exception {
        UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test", "%");
        UserIdentity testUserWithIp = UserIdentity.createAnalyzedUserIdentWithIp("test", "10.1.1.1");
        byte[] seed = "petals on a wet black bough".getBytes(StandardCharsets.UTF_8);
        byte[] scramble = MysqlPassword.scramble(seed, "abc");

        AuthenticationManager masterManager = new AuthenticationManager();
        Assert.assertNull(masterManager.checkPassword(
                testUserWithIp.getQualifiedUser(), testUserWithIp.getHost(), scramble, seed));
        Assert.assertFalse(masterManager.doesUserExist(testUser));
        Assert.assertFalse(masterManager.doesUserExist(testUserWithIp));
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        masterManager.save(emptyImage.getDataOutputStream());

        // master create test@%; no password
        String sql = "create user test";
        CreateUserStmt stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.createUser(stmt);
        Assert.assertTrue(masterManager.doesUserExist(testUser));
        Assert.assertFalse(masterManager.doesUserExist(testUserWithIp));
        UserIdentity user = masterManager.checkPassword(testUser.getQualifiedUser(), "10.1.1.1", new byte[0], new byte[0]);
        Assert.assertEquals(user, testUser);

        // create twice fail
        try {
            masterManager.createUser(stmt);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("failed to create user"));
        }

        // master create test@10.1.1.1
        sql = "create user 'test'@'10.1.1.1' identified by 'abc'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.createUser(stmt);
        Assert.assertTrue(masterManager.doesUserExist(testUser));
        Assert.assertTrue(masterManager.doesUserExist(testUserWithIp));
        user = masterManager.checkPassword(testUser.getQualifiedUser(), testUserWithIp.getHost(), scramble, seed);
        Assert.assertEquals(user, testUserWithIp);

        // make final snapshot
        UtFrameUtils.PseudoImage finalImage = new UtFrameUtils.PseudoImage();
        masterManager.save(finalImage.getDataOutputStream());

        // login from 10.1.1.2 with password will fail
        user = masterManager.checkPassword(testUser.getQualifiedUser(), "10.1.1.2", scramble, seed);
        Assert.assertNull(user);

        // start to replay
        AuthenticationManager followerManager = AuthenticationManager.load(emptyImage.getDataInputStream());
        Assert.assertFalse(followerManager.doesUserExist(testUser));
        Assert.assertFalse(followerManager.doesUserExist(testUserWithIp));

        // replay create test@%; no password
        CreateUserInfo info = (CreateUserInfo) UtFrameUtils.PseudoJournalReplayer.replayNextJournal();
        followerManager.replayCreateUser(
                info.getUserIdentity(),
                info.getAuthenticationInfo(),
                info.getUserProperty(),
                info.getUserPrivilegeCollection(),
                info.getPluginId(),
                info.getPluginVersion());
        Assert.assertTrue(followerManager.doesUserExist(testUser));
        Assert.assertFalse(followerManager.doesUserExist(testUserWithIp));
        user = followerManager.checkPassword(testUser.getQualifiedUser(), "10.1.1.1", new byte[0], new byte[0]);
        Assert.assertEquals(user, testUser);

        // replay create test@10.1.1.1
        info = (CreateUserInfo) UtFrameUtils.PseudoJournalReplayer.replayNextJournal();
        followerManager.replayCreateUser(
                info.getUserIdentity(),
                info.getAuthenticationInfo(),
                info.getUserProperty(),
                info.getUserPrivilegeCollection(),
                info.getPluginId(),
                info.getPluginVersion());
        Assert.assertTrue(followerManager.doesUserExist(testUser));
        Assert.assertTrue(followerManager.doesUserExist(testUserWithIp));
        user = followerManager.checkPassword(testUser.getQualifiedUser(), "10.1.1.1", scramble, seed);
        Assert.assertEquals(user, testUserWithIp);

        // login from 10.1.1.2 with password will fail
        user = followerManager.checkPassword(testUser.getQualifiedUser(), "10.1.1.2", scramble, seed);
        Assert.assertNull(user);

        // purely loaded from image
        AuthenticationManager imageManager = AuthenticationManager.load(finalImage.getDataInputStream());
        Assert.assertTrue(imageManager.doesUserExist(testUser));
        Assert.assertTrue(imageManager.doesUserExist(testUserWithIp));
        user = imageManager.checkPassword(testUser.getQualifiedUser(), "10.1.1.1", scramble, seed);
        Assert.assertEquals(user, testUserWithIp);
        user = imageManager.checkPassword(testUser.getQualifiedUser(), "10.1.1.2", scramble, seed);
        Assert.assertNull(user);
    }

    @Test
    public void testDropAlterUser() throws Exception {
        UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test", "%");
        UserIdentity testUserWithIp = UserIdentity.createAnalyzedUserIdentWithIp("test", "10.1.1.1");
        byte[] seed = "petals on a wet black bough".getBytes(StandardCharsets.UTF_8);
        byte[] scramble = MysqlPassword.scramble(seed, "abc");

        AuthenticationManager manager = ctx.getGlobalStateMgr().getAuthenticationManager();
        Assert.assertNull(manager.checkPassword(
                testUser.getQualifiedUser(), testUser.getHost(), scramble, seed));
        Assert.assertFalse(manager.doesUserExist(testUser));
        Assert.assertFalse(manager.doesUserExist(testUserWithIp));

        String sql = "create user test;";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);
        Assert.assertTrue(manager.doesUserExist(testUser));
        sql = "create user 'test'@'10.1.1.1' identified by 'abc'";
        stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);
        Assert.assertNull(manager.checkPassword(
                testUser.getQualifiedUser(), testUser.getHost(), scramble, seed));
        Assert.assertTrue(manager.doesUserExist(testUserWithIp));

        sql = "alter user test identified by 'abc'";
        stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);
        Assert.assertEquals(testUser,
                manager.checkPassword(testUser.getQualifiedUser(), testUser.getHost(), scramble, seed));
        Assert.assertTrue(manager.doesUserExist(testUser));

        StatementBase dropStmt = UtFrameUtils.parseStmtWithNewParser("drop user test", ctx);
        DDLStmtExecutor.execute(dropStmt, ctx);
        Assert.assertNull(manager.checkPassword(
                testUser.getQualifiedUser(), testUser.getHost(), scramble, seed));
        Assert.assertFalse(manager.doesUserExist(testUser));

        // can drop twice
        DDLStmtExecutor.execute(dropStmt, ctx);

        // cannot alter twice
        try {
            DDLStmtExecutor.execute(stmt, ctx);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("failed to alter user 'test'@'%'"));
        }

        // still has max connection
        Assert.assertNotEquals(0, manager.getMaxConn("test"));

        sql = "drop user 'test'@'10.1.1.1' ";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);
        DDLStmtExecutor.execute(dropStmt, ctx);

        // can't get max connection after all test user are dropped
        Assert.assertThrows(NullPointerException.class, () -> manager.getMaxConn("test"));
    }

    @Test
    public void testDropAlterPersist() throws Exception {
        UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test", "%");
        byte[] seed = "petals on a wet black bough".getBytes(StandardCharsets.UTF_8);
        byte[] scramble = MysqlPassword.scramble(seed, "abc");

        AuthenticationManager masterManager = ctx.getGlobalStateMgr().getAuthenticationManager();
        Assert.assertFalse(masterManager.doesUserExist(testUser));
        Assert.assertTrue(masterManager.doesUserExist(UserIdentity.ROOT));

        // 1. create empty image
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        masterManager.save(emptyImage.getDataOutputStream());

        // 2. create user
        String sql = "create user test";
        CreateUserStmt createStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.createUser(createStmt);
        Assert.assertTrue(masterManager.doesUserExist(testUser));
        Assert.assertEquals(testUser, masterManager.checkPassword(
                testUser.getQualifiedUser(), "10.1.1.1", new byte[0], null));

        // 3. alter user
        sql = "alter user test identified by 'abc'";
        AlterUserStmt alterUserStmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.alterUser(alterUserStmt);
        Assert.assertEquals(testUser, masterManager.checkPassword(
                testUser.getQualifiedUser(), "10.1.1.1", scramble, seed));

        // 4. save image after alter
        UtFrameUtils.PseudoImage alterImage = new UtFrameUtils.PseudoImage();
        masterManager.save(alterImage.getDataOutputStream());

        // 5. drop user
        sql = "drop user test";
        DropUserStmt dropStmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.dropUser(dropStmt);
        Assert.assertFalse(masterManager.doesUserExist(testUser));

        // 6. save final image
        UtFrameUtils.PseudoImage finalImage = new UtFrameUtils.PseudoImage();
        masterManager.save(finalImage.getDataOutputStream());

        // 7. verify replay...
        AuthenticationManager followerManager = AuthenticationManager.load(emptyImage.getDataInputStream());
        Assert.assertFalse(followerManager.doesUserExist(testUser));
        // 7.1 replay create user
        CreateUserInfo createInfo = (CreateUserInfo) UtFrameUtils.PseudoJournalReplayer.replayNextJournal();
        followerManager.replayCreateUser(
                createInfo.getUserIdentity(), createInfo.getAuthenticationInfo(), createInfo.getUserProperty(),
                createInfo.getUserPrivilegeCollection(), createInfo.getPluginId(), createInfo.getPluginVersion());
        Assert.assertTrue(followerManager.doesUserExist(testUser));
        Assert.assertEquals(testUser, followerManager.checkPassword(
                testUser.getQualifiedUser(), "10.1.1.1", new byte[0], null));
        // 7.2 replay alter user
        AlterUserInfo alterInfo = (AlterUserInfo) UtFrameUtils.PseudoJournalReplayer.replayNextJournal();
        followerManager.replayAlterUser(alterInfo.getUserIdentity(), alterInfo.getAuthenticationInfo());
        Assert.assertEquals(testUser, followerManager.checkPassword(
                testUser.getQualifiedUser(), "10.1.1.1", scramble, seed));
        // 7.3 replay drop user
        UserIdentity dropInfo = (UserIdentity) UtFrameUtils.PseudoJournalReplayer.replayNextJournal();
        followerManager.replayDropUser(dropInfo);
        Assert.assertFalse(followerManager.doesUserExist(testUser));
        Assert.assertTrue(followerManager.doesUserExist(UserIdentity.ROOT));

        // 8. verify alter image
        AuthenticationManager alterManager = AuthenticationManager.load(alterImage.getDataInputStream());
        Assert.assertTrue(alterManager.doesUserExist(testUser));
        Assert.assertEquals(testUser, alterManager.checkPassword(
                testUser.getQualifiedUser(), "10.1.1.1", scramble, seed));
        Assert.assertTrue(alterManager.doesUserExist(UserIdentity.ROOT));

        // 9. verify final image
        AuthenticationManager finalManager = AuthenticationManager.load(finalImage.getDataInputStream());
        Assert.assertFalse(finalManager.doesUserExist(testUser));
        Assert.assertTrue(finalManager.doesUserExist(UserIdentity.ROOT));
    }
}
