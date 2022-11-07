// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.mysql.privilege;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.persist.AuthUpgradeInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.privilege.PrivilegeManager;
import com.starrocks.privilege.PrivilegeType;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AuthUpgraderTest {
    private ConnectContext ctx;
    private long roleUserId = 0;

    private UtFrameUtils.PseudoImage executeAndUpgrade(boolean onlyUpgradeJournal, String...sqls) throws Exception {
        GlobalStateMgr.getCurrentState().initAuth(false);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        // 1. execute old grant
        for (String sql : sqls) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);
        }
        Map<String, Set<String>> resolvedIPsMap = new HashMap<>();
        resolvedIPsMap.put("localhost", new HashSet<>(Arrays.asList("127.0.0.1")));
        ctx.getGlobalStateMgr().getAuth().refreshUserPrivEntriesByResovledIPs(resolvedIPsMap);
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
        if (onlyUpgradeJournal) {
            UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        }
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
        String createResourceStmt = "create external resource 'hive0' PROPERTIES(" +
                "\"type\"  =  \"hive\", \"hive.metastore.uris\"  =  \"thrift://127.0.0.1:9083\")";
        starRocksAssert.withResource(createResourceStmt);

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
                true,
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
    public void testNoImage() throws Exception {
        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                false,
                "create user selectUser",
                "GRANT select_priv on db0.tbl0 TO selectUser",
                "create role impersonateRole",
                "GRANT impersonate on selectUser TO ROLE impersonateRole");

        // check upgrade success
        UserIdentity selectUser = UserIdentity.createAnalyzedUserIdentWithIp("selectUser", "%");
        checkPrivilegeAsUser(selectUser, "select * from db0.tbl0");
        UserIdentity user = createUserByRole("impersonateRole");
        ctx.setCurrentUserIdentity(user);
        ctx.getGlobalStateMgr().getPrivilegeManager().canExecuteAs(ctx, selectUser);

        // restart
        ctx.getGlobalStateMgr().initAuth(true);
        // read all journals
        for (short op : Arrays.asList(
                OperationType.OP_CREATE_USER,
                OperationType.OP_GRANT_PRIV,
                OperationType.OP_CREATE_ROLE,
                OperationType.OP_GRANT_IMPERSONATE)) {
            ctx.getGlobalStateMgr().replayOldAuthJournal(op, UtFrameUtils.PseudoJournalReplayer.replayNextJournal(op));
        }
        AuthUpgradeInfo info = (AuthUpgradeInfo) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(
                OperationType.OP_AUTH_UPGRDE_V2);
        ctx.getGlobalStateMgr().replayAuthUpgrade(info);

        // check upgrade success
        checkPrivilegeAsUser(selectUser, "select * from db0.tbl0");
        user = createUserByRole("impersonateRole");
        ctx.setCurrentUserIdentity(user);
        ctx.getGlobalStateMgr().getPrivilegeManager().canExecuteAs(ctx, selectUser);
    }

    @Test
    public void testImpersonate() throws Exception {
        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
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

    @Test
    public void testDomainUser() throws Exception {
        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                "create user domain_user@['localhost']",
                "GRANT select_priv on db1.tbl1 TO domain_user@['localhost']");

        // check twice, the second time is as follower
        for (int i = 0; i != 2; ++ i) {
            if (i == 1) {
                replayUpgrade(image);
            }
            checkPrivilegeAsUser(UserIdentity.createAnalyzedUserIdentWithDomain(
                    "domain_user", "localhost"), "select * from db1.tbl1");
        }
    }

    @Test
    public void testResource() throws Exception {
        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                "create user globalUsageResourceUser",
                "GRANT usage_priv on resource * TO globalUsageResourceUser",
                "create user globalUsageResourceUser1",
                "GRANT usage_priv on * TO globalUsageResourceUser1",
                "create user oneUsageResourceUser",
                "GRANT usage_priv on resource hive0 TO oneUsageResourceUser",
                "create role globalUsageResourceRole",
                "GRANT usage_priv on resource * TO role globalUsageResourceRole",
                "create role globalUsageResourceRole1",
                "GRANT usage_priv on * TO role globalUsageResourceRole1",
                "create role oneUsageResourceRole",
                "GRANT usage_priv on resource hive0 TO role oneUsageResourceRole");
        // check twice, the second time is as follower
        for (int i = 0; i != 2; ++ i) {
            if (i == 1) {
                replayUpgrade(image);
            }
            ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("globalUsageResourceUser", "%"));
            Assert.assertTrue(PrivilegeManager.checkResourceAction(ctx, "hive0", PrivilegeType.ResourceAction.USAGE));
            ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("globalUsageResourceUser1", "%"));
            Assert.assertTrue(PrivilegeManager.checkResourceAction(ctx, "hive0", PrivilegeType.ResourceAction.USAGE));
            ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("oneUsageResourceUser", "%"));
            Assert.assertTrue(PrivilegeManager.checkResourceAction(ctx, "hive0", PrivilegeType.ResourceAction.USAGE));

            ctx.setCurrentUserIdentity(createUserByRole("globalUsageResourceRole"));
            Assert.assertTrue(PrivilegeManager.checkResourceAction(ctx, "hive0", PrivilegeType.ResourceAction.USAGE));
            ctx.setCurrentUserIdentity(createUserByRole("globalUsageResourceRole1"));
            Assert.assertTrue(PrivilegeManager.checkResourceAction(ctx, "hive0", PrivilegeType.ResourceAction.USAGE));
            ctx.setCurrentUserIdentity(createUserByRole("oneUsageResourceRole"));
            Assert.assertTrue(PrivilegeManager.checkResourceAction(ctx, "hive0", PrivilegeType.ResourceAction.USAGE));
        }
    }
}