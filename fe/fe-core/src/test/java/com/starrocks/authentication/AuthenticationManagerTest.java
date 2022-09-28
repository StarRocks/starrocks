// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.authentication;

import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.DdlException;
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
        UtFrameUtils.setUpForPersistTest();
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

        // pure from image
        AuthenticationManager imageManager = AuthenticationManager.load(finalImage.getDataInputStream());
        Assert.assertTrue(followerManager.doesUserExist(testUser));
        Assert.assertTrue(followerManager.doesUserExist(testUserWithIp));
        user = followerManager.checkPassword(testUser.getQualifiedUser(), "10.1.1.1", scramble, seed);
        Assert.assertEquals(user, testUserWithIp);
        user = followerManager.checkPassword(testUser.getQualifiedUser(), "10.1.1.2", scramble, seed);
        Assert.assertNull(user);

        UtFrameUtils.tearDownForPersisTest();
    }
}
