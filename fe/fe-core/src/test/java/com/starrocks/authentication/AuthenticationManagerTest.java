// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.authentication;

import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.persist.CreateUserInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class AuthenticationManagerTest {
    ConnectContext ctx;

    @Before
    public void setUp() throws Exception {
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
    }

    @Test
    public void testInitDefault() throws Exception {
        AuthenticationManager manager = new AuthenticationManager();
        manager.init();
        Assert.assertTrue(manager.doesUserExist(UserIdentity.ROOT));
        Assert.assertFalse(manager.doesUserExist(UserIdentity.createAnalyzedUserIdentWithIp("fake", "%")));
        Assert.assertEquals(new UserProperty().getMaxConn(), manager.getMaxConn(AuthenticationManager.ROOT_USER));
    }

    @Test
    public void testCreateUserAndReplay() throws Exception {
        AuthenticationManager masterMaanger = new AuthenticationManager();
        masterMaanger.init();
        UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test", "%");
        UserIdentity testUserWithIp = UserIdentity.createAnalyzedUserIdentWithIp("test", "10.1.1.1");
        Assert.assertFalse(masterMaanger.doesUserExist(testUser));
        Assert.assertFalse(masterMaanger.doesUserExist(testUserWithIp));
        UtFrameUtils.setUpForPersistTest();
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();

        // master create test@%; no password
        String sql = "create user test";
        CreateUserStmt stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterMaanger.createUser(stmt);
        Assert.assertTrue(masterMaanger.doesUserExist(testUser));
        Assert.assertFalse(masterMaanger.doesUserExist(testUserWithIp));
        UserIdentity user = masterMaanger.checkPassword(testUser.getQualifiedUser(), "10.1.1.1", new byte[0], new byte[0]);
        Assert.assertEquals(user, testUser);

        // master create test@10.1.1.1
        sql = "create user 'test'@'10.1.1.1' identified by 'abc'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterMaanger.createUser(stmt);
        Assert.assertTrue(masterMaanger.doesUserExist(testUser));
        Assert.assertTrue(masterMaanger.doesUserExist(testUserWithIp));
        byte[] seed = "petals on a wet black bough".getBytes(StandardCharsets.UTF_8);
        byte[] scramble = MysqlPassword.scramble(seed, "abc");
        user = masterMaanger.checkPassword(testUser.getQualifiedUser(), testUserWithIp.getHost(), scramble, seed);
        Assert.assertEquals(user, testUserWithIp);

        // login from 10.1.1.2 with password will fail
        user = masterMaanger.checkPassword(testUser.getQualifiedUser(), "10.1.1.2", scramble, seed);
        Assert.assertNull(user);

        // start to replay
        AuthenticationManager followerManager = new AuthenticationManager();
        followerManager.init();
        Assert.assertFalse(followerManager.doesUserExist(testUser));
        Assert.assertFalse(followerManager.doesUserExist(testUserWithIp));

        // replay create test@%; no password
        CreateUserInfo info = (CreateUserInfo) UtFrameUtils.PseudoJournalReplayer.replayNextJournal();
        followerManager.replayCreateUser(info.getUserIdentity(), info.getAuthenticationInfo(), info.getUserProperty());
        Assert.assertTrue(followerManager.doesUserExist(testUser));
        Assert.assertFalse(followerManager.doesUserExist(testUserWithIp));
        user = followerManager.checkPassword(testUser.getQualifiedUser(), "10.1.1.1", new byte[0], new byte[0]);
        Assert.assertEquals(user, testUser);

        // replay create test@10.1.1.1
        info = (CreateUserInfo) UtFrameUtils.PseudoJournalReplayer.replayNextJournal();
        followerManager.replayCreateUser(info.getUserIdentity(), info.getAuthenticationInfo(), info.getUserProperty());
        Assert.assertTrue(followerManager.doesUserExist(testUser));
        Assert.assertTrue(followerManager.doesUserExist(testUserWithIp));
        user = followerManager.checkPassword(testUser.getQualifiedUser(), "10.1.1.1", scramble, seed);
        Assert.assertEquals(user, testUserWithIp);

        // login from 10.1.1.2 with password will fail
        user = followerManager.checkPassword(testUser.getQualifiedUser(), "10.1.1.2", scramble, seed);
        Assert.assertNull(user);

        UtFrameUtils.tearDownForPersisTest();
    }
}
