// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.mysql.privilege;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.persist.AuthUpgradeInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.PrivilegeCheckerV2;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;

public class AuthUpgraderTest {
    private ConnectContext ctx;
    private long roleUserId = 0;

    private UtFrameUtils.PseudoImage executeAndUpgrade(String...sqls) throws Exception {
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        // 1. execute old grant
        for (String sql : sqls) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);
        }
        // 2. save image
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        ctx.getGlobalStateMgr().getAuth().saveAuth(image.getDataOutputStream(), -1);
        ctx.getGlobalStateMgr().getAuth().writeAsGson(image.getDataOutputStream(), -1);

        // 3. load image
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        DataInputStream dis = image.getDataInputStream();
        Auth auth = Auth.read(dis);
        auth.readAsGson(dis, -1);
        ctx.getGlobalStateMgr().initAuth(true);
        AuthUpgrader authUpgrader = new AuthUpgrader(
                auth,
                ctx.getGlobalStateMgr().getAuthenticationManager(),
                ctx.getGlobalStateMgr().getPrivilegeManager(),
                ctx.getGlobalStateMgr());
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        authUpgrader.upgradeAsLeader();
        return image;
    }

    private void replayUpgrade(UtFrameUtils.PseudoImage image) throws Exception {
        // pretend it's a old privilege
        GlobalStateMgr.getCurrentState().initAuth(false);
        // 1. load image
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        DataInputStream dis = image.getDataInputStream();
        Auth auth = Auth.read(dis);
        auth.readAsGson(dis, -1);
        ctx.getGlobalStateMgr().setAuth(auth);
        AuthUpgradeInfo info = (AuthUpgradeInfo) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(
                OperationType.OP_AUTH_UPGRDE_V2);
        ctx.getGlobalStateMgr().replayAuthUpgrade(info);
    }

    private void checkPrivilegeAsUser(UserIdentity user, String... verifiedSqls) throws Exception {
        ctx.setCurrentUserIdentity(user);
        ctx.setQualifiedUser(user.getQualifiedUser());
        for (String sql : verifiedSqls) {
            PrivilegeCheckerV2.check(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);
        }
    }

    private void checkBadPrivilegeAsUser(UserIdentity user, String badSql, String expectError) {
        ctx.setCurrentUserIdentity(user);
        ctx.setQualifiedUser(user.getQualifiedUser());
        try {
            PrivilegeCheckerV2.check(UtFrameUtils.parseStmtWithNewParser(badSql, ctx), ctx);
            Assert.fail();
        } catch (Exception e) {
            System.err.println("got exception as expect: " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains(expectError));
        }
    }

    private UserIdentity createUserByRole(String roleName) throws Exception {
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        // create a user & grant
        String user = "test_role_user_" + roleUserId;
        roleUserId += 1;
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create user " + user, ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant " + roleName + " to  " + user, ctx), ctx);
        return UserIdentity.createAnalyzedUserIdentWithIp(user, "%");
    }

    @Before
    public void init() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        // default is old privilege
        GlobalStateMgr.getCurrentState().initAuth(false);
        UtFrameUtils.setUpForPersistTest();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        StarRocksAssert starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db0");
        starRocksAssert.withDatabase("db1");
        for (int i = 0; i != 2; ++ i) {
            for (int j = 0; j != 2; ++ j) {
                String createTblStmtStr = "create table db" + i + ".tbl" + j
                        + "(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                        +
                        "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
                starRocksAssert.withTable(createTblStmtStr);
            }
        }
        ctx = starRocksAssert.getCtx();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        Config.ignore_invalid_privilege_authentications = true;
    }

    @After
    public void cleanUp() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testSelect() throws Exception {
        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                "create user globalSelect",
                "GRANT select_priv on *.* TO globalSelect",
                "create user dbSelect",
                "GRANT select_priv on db0.* TO dbSelect",
                "create user tblSelect",
                "GRANT select_priv on db0.tbl0 TO tblSelect",
                "create role globalSelect",
                "GRANT select_priv on *.* TO role globalSelect",
                "create role dbSelect",
                "GRANT select_priv on db0.* TO role dbSelect",
                "create role tblSelect",
                "GRANT select_priv on db0.tbl0 TO role tblSelect");
        // check twice, the second time is as follower
        for (int i = 0; i != 2; ++ i) {
            if (i == 1) {
                replayUpgrade(image);
            }
            checkPrivilegeAsUser(
                    UserIdentity.createAnalyzedUserIdentWithIp("globalSelect", "%"),
                    "select * from db1.tbl1",
                    "select * from db0.tbl0");
            UserIdentity user = createUserByRole("globalSelect");
            checkPrivilegeAsUser(user, "select * from db1.tbl1", "select * from db0.tbl0");

            user = UserIdentity.createAnalyzedUserIdentWithIp("dbSelect", "%");
            checkPrivilegeAsUser(user, "select * from db0.tbl0", "select * from db0.tbl1");
            checkBadPrivilegeAsUser(user, "select * from db1.tbl1", "SELECT command denied to user 'dbSelect'");
            user = createUserByRole("dbSelect");
            checkPrivilegeAsUser(user, "select * from db0.tbl0", "select * from db0.tbl1");
            checkBadPrivilegeAsUser(user, "select * from db1.tbl1", "SELECT command denied to user");

            user = UserIdentity.createAnalyzedUserIdentWithIp("tblSelect", "%");
            checkPrivilegeAsUser(user, "select * from db0.tbl0");
            checkBadPrivilegeAsUser(user, "select * from db1.tbl1", "SELECT command denied to user 'tblSelect'");
            user = createUserByRole("tblSelect");
            checkPrivilegeAsUser(user, "select * from db0.tbl0");
            checkBadPrivilegeAsUser(user, "select * from db1.tbl1", "SELECT command denied to user");
        }
    }

    @Test
    public void testImpersonate() throws Exception {
        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                "create user harry",
                "create user gregory",
                "GRANT impersonate on gregory TO harry",
                "create role harry",
                "GRANT impersonate on gregory TO ROLE harry");

        // check twice, the second time is as follower
        for (int i = 0; i != 2; ++ i) {
            if (i == 1) {
                replayUpgrade(image);
            }
            ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("harry", "%"));
            ctx.getGlobalStateMgr().getPrivilegeManager().canExecuteAs(
                    ctx, UserIdentity.createAnalyzedUserIdentWithIp("gregory", "%"));

            UserIdentity user = createUserByRole("harry");
            ctx.setCurrentUserIdentity(user);
            ctx.getGlobalStateMgr().getPrivilegeManager().canExecuteAs(
                    ctx, UserIdentity.createAnalyzedUserIdentWithIp("gregory", "%"));
        }

    }

}