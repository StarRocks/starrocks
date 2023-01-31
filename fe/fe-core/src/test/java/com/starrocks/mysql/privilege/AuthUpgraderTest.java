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


package com.starrocks.mysql.privilege;

import com.clearspring.analytics.util.Lists;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthUpgraderTest {
    private ConnectContext ctx;
    private long roleUserId = 0;

    private UtFrameUtils.PseudoImage executeAndUpgrade(boolean onlyUpgradeJournal, String... sqls) throws Exception {
        GlobalStateMgr.getCurrentState().initAuth(false);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        // 1. execute old grant
        for (String sql : sqls) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);
        }
        Map<String, Set<String>> resolvedIPsMap = new HashMap<>();
        resolvedIPsMap.put("localhost", new HashSet<>(Arrays.asList("127.0.0.1")));
        ctx.getGlobalStateMgr().getAuth().refreshUserPrivEntriesByResolvedIPs(resolvedIPsMap);
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
        // pretend it's an old privilege
        GlobalStateMgr.getCurrentState().initAuth(false);
        // 1. load image
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        DataInputStream dis = image.getDataInputStream();
        Auth auth = Auth.read(dis);
        auth.readAsGson(dis, -1);
        ctx.getGlobalStateMgr().setAuth(auth);
        AuthUpgradeInfo info = (AuthUpgradeInfo) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(
                OperationType.OP_AUTH_UPGRADE_V2);
        ctx.getGlobalStateMgr().replayAuthUpgrade(info);
    }

    private void checkPrivilegeAsUser(UserIdentity user, String... verifiedSqls) throws Exception {
        ctx.setCurrentUserIdentity(user);
        ctx.setQualifiedUser(user.getQualifiedUser());
        for (String sql : verifiedSqls) {
            System.err.println(sql);
            PrivilegeCheckerV2.check(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);
        }
    }

    private void checkBadPrivilegeAsUser(UserIdentity user, String badSql, String expectError) {
        ctx.setCurrentUserIdentity(user);
        ctx.setQualifiedUser(user.getQualifiedUser());
        try {
            PrivilegeCheckerV2.check(UtFrameUtils.parseStmtWithNewParser(badSql, ctx), ctx);
            Assert.fail(badSql + " should fail");
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

        for (int i = 0; i != 2; ++i) {
            for (int j = 0; j != 2; ++j) {
                String createTblStmtStr = "create table db" + i + ".tbl" + j
                        + "(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                        + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1)"
                        + " buckets 3 properties('replication_num' = '1');";
                starRocksAssert.withTable(createTblStmtStr);
            }
            starRocksAssert.withView("create view db" + i + ".view as select k1, k2 from db" + i + ".tbl0");
        }
        ctx = starRocksAssert.getCtx();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        Config.ignore_invalid_privilege_authentications = false;
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
                "create user viewSelect",
                "GRANT select_priv on db0.view TO viewSelect",
                "create role globalSelect",
                "GRANT select_priv on *.* TO role globalSelect",
                "create role dbSelect",
                "GRANT select_priv on db0.* TO role dbSelect",
                "create role tblSelect",
                "GRANT select_priv on db0.tbl0 TO role tblSelect",
                "create role viewSelect",
                "GRANT select_priv on db0.view TO role viewSelect");
        // check twice, the second time is as follower
        for (int i = 0; i != 2; ++i) {
            if (i == 1) {
                replayUpgrade(image);
            }
            checkPrivilegeAsUser(
                    UserIdentity.createAnalyzedUserIdentWithIp("globalSelect", "%"),
                    "select * from db1.tbl1",
                    "select * from db0.tbl0");
            UserIdentity user = createUserByRole("globalSelect");
            checkPrivilegeAsUser(
                    user, "select * from db1.tbl1", "select * from db0.tbl0", "select * from db0.view");

            user = UserIdentity.createAnalyzedUserIdentWithIp("dbSelect", "%");
            checkPrivilegeAsUser(
                    user, "select * from db0.tbl0", "select * from db0.tbl1", "select * from db0.view");
            checkBadPrivilegeAsUser(user, "select * from db1.tbl1", "SELECT command denied to user 'dbSelect'");
            user = createUserByRole("dbSelect");
            checkPrivilegeAsUser(
                    user, "select * from db0.tbl0", "select * from db0.tbl1", "select * from db0.view");
            checkBadPrivilegeAsUser(user, "select * from db1.tbl1", "SELECT command denied to user");

            user = UserIdentity.createAnalyzedUserIdentWithIp("tblSelect", "%");
            checkPrivilegeAsUser(user, "select * from db0.tbl0");
            checkBadPrivilegeAsUser(user, "select * from db1.tbl1", "SELECT command denied to user 'tblSelect'");
            user = createUserByRole("tblSelect");
            checkPrivilegeAsUser(user, "select * from db0.tbl0");
            checkBadPrivilegeAsUser(user, "select * from db1.tbl1", "SELECT command denied to user");

            user = UserIdentity.createAnalyzedUserIdentWithIp("viewSelect", "%");
            checkPrivilegeAsUser(user, "select * from db0.view");
            checkBadPrivilegeAsUser(user, "select * from db0.tbl0", "SELECT command denied to user 'viewSelect'");
            user = createUserByRole("viewSelect");
            checkPrivilegeAsUser(user, "select * from db0.view");
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
                OperationType.OP_AUTH_UPGRADE_V2);
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
        for (int i = 0; i != 2; ++i) {
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
        for (int i = 0; i != 2; ++i) {
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
        for (int i = 0; i != 2; ++i) {
            if (i == 1) {
                replayUpgrade(image);
            }
            ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("globalUsageResourceUser", "%"));
            Assert.assertTrue(PrivilegeManager.checkResourceAction(ctx, "hive0", PrivilegeType.USAGE));
            ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("globalUsageResourceUser1", "%"));
            Assert.assertTrue(PrivilegeManager.checkResourceAction(ctx, "hive0", PrivilegeType.USAGE));
            ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("oneUsageResourceUser", "%"));
            Assert.assertTrue(PrivilegeManager.checkResourceAction(ctx, "hive0", PrivilegeType.USAGE));

            ctx.setCurrentUserIdentity(createUserByRole("globalUsageResourceRole"));
            Assert.assertTrue(PrivilegeManager.checkResourceAction(ctx, "hive0", PrivilegeType.USAGE));
            ctx.setCurrentUserIdentity(createUserByRole("globalUsageResourceRole1"));
            Assert.assertTrue(PrivilegeManager.checkResourceAction(ctx, "hive0", PrivilegeType.USAGE));
            ctx.setCurrentUserIdentity(createUserByRole("oneUsageResourceRole"));
            Assert.assertTrue(PrivilegeManager.checkResourceAction(ctx, "hive0", PrivilegeType.USAGE));
        }
    }

    @Test
    public void testCreate() throws Exception {
        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                "create user userWithGlobalCreate",
                "grant create_priv on *.* to userWithGlobalCreate",
                "create role userWithGlobalCreate",
                "grant create_priv on *.* to role userWithGlobalCreate",
                "create user userWithResourceAllCreate",
                "grant create_priv on resource * to userWithResourceAllCreate",
                "create user userWithDbCreate",
                "grant create_priv on db1.* to userWithDbCreate",
                "create role userWithDbCreate",
                "grant create_priv on db1.* to role userWithDbCreate",
                "create user tableCreate",
                "grant create_priv on db0.tbl0 to tableCreate",
                "create role tableCreate",
                "grant create_priv on db0.tbl0 to role tableCreate",
                "create user viewCreate",
                "grant create_priv on db0.view to viewCreate",
                "create role viewCreate",
                "grant create_priv on db0.view to role viewCreate");
        // check twice, the second time is as follower
        for (int i = 0; i != 2; ++i) {
            if (i == 1) {
                replayUpgrade(image);
            }

            List<String> users = Lists.newArrayList();
            users.add("userWithGlobalCreate");
            users.add("userWithResourceAllCreate");
            // check: grant create_priv on *.* | grant create_priv on resource *
            for (String user : users) {
                UserIdentity uid = UserIdentity.createAnalyzedUserIdentWithIp(user, "%");
                ctx.setCurrentUserIdentity(uid);
                Assert.assertTrue(PrivilegeManager.checkDbAction(ctx, "db0", PrivilegeType.CREATE_TABLE));
                Assert.assertTrue(PrivilegeManager.checkDbAction(ctx, "db0", PrivilegeType.CREATE_VIEW));
                Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db0", PrivilegeType.CREATE_FUNCTION));
                Assert.assertTrue(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.CREATE_TABLE));
                Assert.assertTrue(PrivilegeManager.checkDbAction(ctx, "db1",
                        PrivilegeType.CREATE_MATERIALIZED_VIEW));
                // should have CREATE_DATABASE privilege on default_catalog
                Assert.assertTrue(PrivilegeManager.checkCatalogAction(ctx, "default_catalog",
                        PrivilegeType.CREATE_DATABASE));
            }
            ctx.setCurrentUserIdentity(createUserByRole("userWithGlobalCreate"));
            Assert.assertTrue(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.CREATE_TABLE));
            Assert.assertTrue(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.CREATE_VIEW));
            Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.CREATE_FUNCTION));
            Assert.assertTrue(PrivilegeManager.checkDbAction(ctx, "db0", PrivilegeType.CREATE_TABLE));
            Assert.assertTrue(PrivilegeManager.checkDbAction(ctx, "db0",
                    PrivilegeType.CREATE_MATERIALIZED_VIEW));

            // check: grant create_priv on db1.*
            ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("userWithDbCreate", "%"));
            Assert.assertTrue(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.CREATE_TABLE));
            Assert.assertTrue(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.CREATE_VIEW));
            Assert.assertFalse(PrivilegeManager.checkCatalogAction(ctx, "default_catalog",
                    PrivilegeType.CREATE_DATABASE));
            Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db0", PrivilegeType.CREATE_TABLE));
            Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db0", PrivilegeType.CREATE_VIEW));
            ctx.setCurrentUserIdentity(createUserByRole("userWithDbCreate"));
            Assert.assertTrue(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.CREATE_TABLE));
            Assert.assertTrue(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.CREATE_VIEW));
            Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db0", PrivilegeType.CREATE_TABLE));
            Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db0", PrivilegeType.CREATE_VIEW));

            // can't create view or table anymore
            for (UserIdentity userIdentity : Arrays.asList(
                    UserIdentity.createAnalyzedUserIdentWithIp("tableCreate", "%"),
                    createUserByRole("tableCreate"),
                    UserIdentity.createAnalyzedUserIdentWithIp("viewCreate", "%"),
                    createUserByRole("viewCreate"))) {
                ctx.setCurrentUserIdentity(userIdentity);
                Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db0", PrivilegeType.CREATE_TABLE));
                Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db0", PrivilegeType.CREATE_VIEW));
            }
        }
    }

    @Test
    public void testDrop() throws Exception {
        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                "create user userWithGlobalDrop",
                "grant drop_priv on *.* to userWithGlobalDrop",
                "create role userWithGlobalDrop",
                "grant drop_priv on *.* to role userWithGlobalDrop",
                "create user userWithResourceAllDrop",
                "grant drop_priv on resource * to userWithResourceAllDrop",
                "create user userWithDbDrop",
                "grant drop_priv on db1.* to userWithDbDrop",
                "create role userWithDbDrop",
                "grant drop_priv on db1.* to role userWithDbDrop",
                "create user tableDrop",
                "grant drop_priv on db0.tbl0 to tableDrop",
                "create role tableDrop",
                "grant drop_priv on db0.tbl0 to role tableDrop",
                "create user viewDrop",
                "grant drop_priv on db0.view to viewDrop",
                "create role viewDrop",
                "grant drop_priv on db0.view to role viewDrop");
        // check twice, the second time is as follower
        for (int i = 0; i != 2; ++i) {
            if (i == 1) {
                replayUpgrade(image);
            }

            List<String> users = Lists.newArrayList();
            users.add("userWithGlobalDrop");
            users.add("userWithResourceAllDrop");
            // check: grant drop_priv on *.* | grant drop_priv on resource *
            for (String user : users) {
                UserIdentity uid = UserIdentity.createAnalyzedUserIdentWithIp(user, "%");
                ctx.setCurrentUserIdentity(uid);
                Assert.assertTrue(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.CREATE_VIEW));
            }
            ctx.setCurrentUserIdentity(createUserByRole("userWithGlobalDrop"));
            Assert.assertTrue(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.DROP));
            Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.CREATE_VIEW));

            // check: grant drop_priv on db1.*
            for (UserIdentity userIdentity : Arrays.asList(
                    UserIdentity.createAnalyzedUserIdentWithIp("userWithDbDrop", "%"),
                    createUserByRole("userWithDbDrop"))) {
                ctx.setCurrentUserIdentity(userIdentity);
                Assert.assertTrue(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db0", PrivilegeType.DROP));
                Assert.assertTrue(PrivilegeManager.checkTableAction(
                        ctx, "db1", "tbl0", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeManager.checkTableAction(
                        ctx, "db0", "tbl0", PrivilegeType.DROP));
                Assert.assertTrue(PrivilegeManager.checkViewAction(
                        ctx, "db1", "view", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeManager.checkViewAction(
                        ctx, "db0", "view", PrivilegeType.DROP));
            }

            // check drop on table
            for (UserIdentity userIdentity : Arrays.asList(
                    UserIdentity.createAnalyzedUserIdentWithIp("tableDrop", "%"),
                    createUserByRole("tableDrop"))) {
                ctx.setCurrentUserIdentity(userIdentity);
                Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db0", PrivilegeType.DROP));
                Assert.assertTrue(PrivilegeManager.checkTableAction(
                        ctx, "db0", "tbl0", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeManager.checkTableAction(
                        ctx, "db0", "tbl1", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeManager.checkViewAction(
                        ctx, "db0", "view", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeManager.checkViewAction(
                        ctx, "db1", "view", PrivilegeType.DROP));
            }

            // check alter on view
            for (UserIdentity userIdentity : Arrays.asList(
                    UserIdentity.createAnalyzedUserIdentWithIp("viewDrop", "%"),
                    createUserByRole("viewDrop"))) {
                ctx.setCurrentUserIdentity(userIdentity);
                Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db0", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeManager.checkTableAction(
                        ctx, "db0", "tbl0", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeManager.checkTableAction(
                        ctx, "db0", "tbl1", PrivilegeType.DROP));
                Assert.assertTrue(PrivilegeManager.checkViewAction(
                        ctx, "db0", "view", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeManager.checkViewAction(
                        ctx, "db1", "view", PrivilegeType.DROP));
            }
        }
    }

    @Test
    public void testAlter() throws Exception {
        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                "create user userWithGlobalAlter",
                "grant alter_priv on *.* to userWithGlobalAlter",
                "create role userWithGlobalAlter",
                "grant alter_priv on *.* to role userWithGlobalAlter",
                "create user userWithResourceAllAlter",
                "grant alter_priv on resource * to userWithResourceAllAlter",
                "create user userWithDbAlter",
                "grant alter_priv on db1.* to userWithDbAlter",
                "create role userWithDbAlter",
                "grant alter_priv on db1.* to role userWithDbAlter",
                "create user tableAlter",
                "grant alter_priv on db0.tbl0 to tableAlter",
                "create role tableAlter",
                "grant alter_priv on db0.tbl0 to role tableAlter",
                "create user viewAlter",
                "grant alter_priv on db0.view to viewAlter",
                "create role viewAlter",
                "grant alter_priv on db0.view to role viewAlter");
        // check twice, the second time is as follower
        for (int i = 0; i != 2; ++i) {
            if (i == 1) {
                replayUpgrade(image);
            }

            // check global
            List<String> users = Lists.newArrayList();
            users.add("userWithGlobalAlter");
            users.add("userWithResourceAllAlter");
            // check: grant alter_priv on *.* | grant alter_priv on resource *
            for (String user : users) {
                UserIdentity uid = UserIdentity.createAnalyzedUserIdentWithIp(user, "%");
                ctx.setCurrentUserIdentity(uid);
                Assert.assertTrue(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.CREATE_VIEW));
            }
            ctx.setCurrentUserIdentity(createUserByRole("userWithGlobalAlter"));
            Assert.assertTrue(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.ALTER));
            Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.CREATE_VIEW));

            // check: grant alter_priv on db1.*
            for (UserIdentity userIdentity : Arrays.asList(
                    UserIdentity.createAnalyzedUserIdentWithIp("userWithDbAlter", "%"),
                    createUserByRole("userWithDbAlter"))) {
                ctx.setCurrentUserIdentity(userIdentity);
                Assert.assertTrue(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db0", PrivilegeType.ALTER));
                Assert.assertTrue(PrivilegeManager.checkTableAction(
                        ctx, "db1", "tbl0", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeManager.checkTableAction(
                        ctx, "db0", "tbl0", PrivilegeType.ALTER));
                Assert.assertTrue(PrivilegeManager.checkViewAction(
                        ctx, "db1", "view", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeManager.checkViewAction(
                        ctx, "db0", "view", PrivilegeType.ALTER));
            }

            // check alter on table
            for (UserIdentity userIdentity : Arrays.asList(
                    UserIdentity.createAnalyzedUserIdentWithIp("tableAlter", "%"),
                    createUserByRole("tableAlter"))) {
                ctx.setCurrentUserIdentity(userIdentity);
                Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db0", PrivilegeType.ALTER));
                Assert.assertTrue(PrivilegeManager.checkTableAction(
                        ctx, "db0", "tbl0", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeManager.checkTableAction(
                        ctx, "db0", "tbl1", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeManager.checkViewAction(
                        ctx, "db0", "view", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeManager.checkViewAction(
                        ctx, "db1", "view", PrivilegeType.ALTER));
            }

            // check alter on view
            for (UserIdentity userIdentity : Arrays.asList(
                    UserIdentity.createAnalyzedUserIdentWithIp("viewAlter", "%"),
                    createUserByRole("viewAlter"))) {
                ctx.setCurrentUserIdentity(userIdentity);
                Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db1", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeManager.checkDbAction(ctx, "db0", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeManager.checkTableAction(
                        ctx, "db0", "tbl0", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeManager.checkTableAction(
                        ctx, "db0", "tbl1", PrivilegeType.ALTER));
                Assert.assertTrue(PrivilegeManager.checkViewAction(
                        ctx, "db0", "view", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeManager.checkViewAction(
                        ctx, "db1", "view", PrivilegeType.ALTER));
            }
        }
    }

    @Test
    public void testNodeAdmin() throws Exception {
        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                "create user nodeuser",
                "GRANT node_priv on *.* to nodeuser",
                "create role noderole",
                "GRANT node_priv on *.* to role noderole",
                "create user noderesourceuser",
                "GRANT node_priv on resource * to noderesourceuser",
                "create role noderesourcerole",
                "GRANT node_priv on resource * to role noderesourcerole",
                "create user adminuser",
                "GRANT admin_priv on *.* to adminuser",
                "create role adminrole",
                "GRANT admin_priv on *.* to role adminrole");
        // check twice, the second time is as follower
        for (int i = 0; i != 2; ++i) {
            if (i == 1) {
                replayUpgrade(image);
            }
            for (String name : Arrays.asList("node", "noderesource")) {
                ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(name + "user", "%"));
                Assert.assertTrue(PrivilegeManager.checkSystemAction(ctx, PrivilegeType.NODE));
                ctx.setCurrentUserIdentity(createUserByRole(name + "role"));
                Assert.assertTrue(PrivilegeManager.checkSystemAction(ctx, PrivilegeType.NODE));
            }

            UserIdentity user = createUserByRole("adminrole");
            String createResourceStmt = "create external resource 'hive0' PROPERTIES(" +
                    "\"type\"  =  \"hive\", \"hive.metastore.uris\"  =  \"thrift://127.0.0.1:9083\")";
            List<String> sqlList = new ArrayList<>();
            // show node
            sqlList.addAll(Arrays.asList("show backends", "show frontends", "show broker", "show compute nodes"));
            // show txn
            sqlList.addAll(Arrays.asList("SHOW TRANSACTION FROM db WHERE ID=4005;"));
            // admin
            sqlList.addAll(Arrays.asList(
                    "admin set frontend config (\"key\" = \"value\");",
                    "ADMIN SET REPLICA STATUS PROPERTIES(\"tablet_id\" = \"10003\", " +
                            "\"backend_id\" = \"10001\", \"status\" = \"bad\");",
                    "ADMIN SHOW FRONTEND CONFIG;",
                    "ADMIN SHOW REPLICA DISTRIBUTION FROM example_db.example_table PARTITION(p1, p2);",
                    "ADMIN SHOW REPLICA STATUS FROM example_db.example_table;",
                    "ADMIN REPAIR TABLE example_db.example_table PARTITION(p1);",
                    "ADMIN CANCEL REPAIR TABLE example_db.example_table PARTITION(p1);",
                    "ADMIN CHECK TABLET (1, 2) PROPERTIES (\"type\" = \"CONSISTENCY\");"
            ));
            // alter system
            sqlList.addAll(Arrays.asList(
                    "ALTER SYSTEM ADD FOLLOWER \"127.0.0.1:9010\";",
                    "CANCEL DECOMMISSION BACKEND \"27.0.0.1:9010\", \"27.0.0.1:9011\";"
            ));
            // kill, set, show proc
            sqlList.addAll(Arrays.asList(
                    "kill query 1", "show proc '/backends'", "SET PASSWORD FOR 'jack'@'192.%' = PASSWORD('123456');"
            ));
            // select, create
            sqlList.addAll(Arrays.asList(
                    "select * from db1.tbl1",
                    "select * from db0.tbl0",
                    createResourceStmt
            ));
            checkPrivilegeAsUser(user, sqlList.toArray(new String[0]));
            user = createUserByRole("adminrole");
            checkPrivilegeAsUser(user, sqlList.toArray(new String[0]));
        }
    }

    // for test GRANT
    private UtFrameUtils.PseudoImage initGrantImage(String name, String grantPattern) throws Exception {
        String user = name + "_user";
        String role = name + "_role";
        return executeAndUpgrade(
                true,
                "create user " + user,
                "GRANT grant_priv on " + grantPattern + " to " + user,
                "GRANT select_priv on db0.* to " + user,
                "GRANT select_priv on db1.tbl1 to " + user,
                "GRANT usage_priv on resource hive0 to " + user,
                "create role " + role,
                "GRANT grant_priv on " + grantPattern + " to role " + role,
                "GRANT select_priv on db0.* to role " + role,
                "GRANT select_priv on db1.tbl1 to role " + role,
                "GRANT usage_priv on resource hive0 to role " + role);
    }

    // for test GRANT
    private void checkGrant(
            UtFrameUtils.PseudoImage image,
            String name,
            List<String> allowGrantSQLs,
            List<String> denyGrantSQLs) throws Exception {
        // check twice, the second time is as follower
        for (int i = 0; i != 2; ++i) {
            if (i == 1) {
                replayUpgrade(image);
            }

            UserIdentity user1 = UserIdentity.createAnalyzedUserIdentWithIp(name + "_user", "%");
            UserIdentity user2 = createUserByRole(name + "_role");
            for (UserIdentity user : Arrays.asList(user1, user2)) {
                checkPrivilegeAsUser(
                        user,
                        "select * from db1.tbl1",
                        "select * from db0.tbl0");
                checkBadPrivilegeAsUser(
                        user,
                        "select * from db1.tbl0",
                        "SELECT command denied to user '" + user.getQualifiedUser());
                ctx.setCurrentUserIdentity(user);
                Assert.assertTrue(
                        PrivilegeManager.checkResourceAction(ctx, "hive0", PrivilegeType.USAGE));

                for (String sql : allowGrantSQLs) {
                    checkPrivilegeAsUser(user, sql);
                }
                for (String sql : denyGrantSQLs) {
                    checkBadPrivilegeAsUser(
                            user, sql,
                            "Access denied; you need (at least one of) the GRANT privilege(s) for this operation");
                }
            }
        }
    }

    @Test
    public void testGlobalGrant() throws Exception {
        UtFrameUtils.PseudoImage image = initGrantImage("global_grant", "*.*");
        List<String> allows = Arrays.asList(
                "GRANT SELECT ON ALL TABLES IN ALL DATABASES TO role public",
                "GRANT SELECT ON ALL TABLES IN DATABASE db0 TO role public",
                "GRANT SELECT ON db1.tbl1 TO role public",
                "GRANT USAGE ON resource 'hive0' TO role public",
                "grant global_grant_role to global_grant_user",
                "revoke global_grant_role from global_grant_user",
                "create user xxx",
                "drop user global_grant_user",
                "alter user global_grant_user identified by 'asdf'",
                "show roles",
                "create role xxx",
                "drop role global_grant_role",
                "show grants for root",
                "show authentication for root",
                "show property for 'root' ",
                "set property for 'root' 'max_user_connections' = '100'");
        checkGrant(image, "global_grant", allows, new ArrayList<>());
    }

    @Test
    public void testDbGrant() throws Exception {
        UtFrameUtils.PseudoImage image = initGrantImage("db_grant", "db0.*");
        List<String> allows = Arrays.asList(
                "GRANT SELECT ON ALL TABLES IN DATABASE db0 TO role public");
        List<String> denies = Arrays.asList(
                "GRANT SELECT ON ALL TABLES IN ALL DATABASES TO role public",
                "GRANT USAGE ON resource 'hive0' TO role public",
                "GRANT SELECT ON db1.tbl1 TO role public",
                "grant db_grant_role to db_grant_user",
                "revoke db_grant_role from db_grant_user",
                "create user xxx",
                "drop user db_grant_user",
                "alter user db_grant_user identified by 'asdf'",
                "show roles",
                "create role xxx",
                "drop role db_grant_role",
                "show grants for root",
                "show authentication for root",
                "show property for 'root' ",
                "set property for 'root' 'max_user_connections' = '100'");
        checkGrant(image, "db_grant", allows, denies);
    }

    @Test
    public void testTblGrant() throws Exception {
        UtFrameUtils.PseudoImage image = initGrantImage("table_grant", "db1.tbl1");
        List<String> allows = Arrays.asList(
                "GRANT SELECT ON db1.tbl1 TO role public");
        List<String> denies = Arrays.asList(
                "GRANT SELECT ON ALL TABLES IN ALL DATABASES TO role public",
                "GRANT USAGE ON resource 'hive0' TO role public",
                "GRANT SELECT ON ALL TABLES IN DATABASE db0 TO role public",
                "grant table_grant_role to table_grant_user",
                "revoke table_grant_role from table_grant_user",
                "create user xxx",
                "drop user table_grant_user",
                "alter user table_grant_user identified by 'asdf'",
                "show roles",
                "create role xxx",
                "drop role table_grant_role",
                "show grants for root",
                "show authentication for root",
                "show property for 'root' ",
                "set property for 'root' 'max_user_connections' = '100'");
        checkGrant(image, "table_grant", allows, denies);
    }

    @Test
    public void testResourceGrant() throws Exception {
        UtFrameUtils.PseudoImage image = initGrantImage("resource_grant", "resource hive0");
        List<String> allows = Arrays.asList(
                "GRANT USAGE ON resource 'hive0' TO role public");
        List<String> denies = Arrays.asList(
                "GRANT SELECT ON ALL TABLES IN ALL DATABASES TO role public",
                "GRANT SELECT ON ALL TABLES IN DATABASE db0 TO role public",
                "GRANT SELECT ON db1.tbl1 TO role public",
                "grant resource_grant_role to resource_grant_user",
                "revoke resource_grant_role from resource_grant_user",
                "create user xxx",
                "drop user resource_grant_user",
                "alter user resource_grant_user identified by 'asdf'",
                "show roles",
                "create role xxx",
                "drop role resource_grant_role",
                "show grants for root",
                "show authentication for root",
                "show property for 'root' ",
                "set property for 'root' 'max_user_connections' = '100'");
        checkGrant(image, "resource_grant", allows, denies);
    }

    @Test
    public void testResourceGlobal() throws Exception {
        UtFrameUtils.PseudoImage image = initGrantImage("resource_global_grant", "resource *");
        List<String> allows = Arrays.asList(
                "GRANT SELECT ON ALL TABLES IN ALL DATABASES TO role public",
                "GRANT SELECT ON ALL TABLES IN DATABASE db0 TO role public",
                "GRANT SELECT ON db1.tbl1 TO role public",
                "GRANT USAGE ON resource 'hive0' TO role public",
                "grant resource_global_grant_role to resource_global_grant_user",
                "revoke resource_global_grant_role from resource_global_grant_user",
                "create user xxx",
                "drop user resource_global_grant_user",
                "alter user resource_global_grant_user identified by 'asdf'",
                "show roles",
                "create role xxx",
                "drop role resource_global_grant_role",
                "show grants for root",
                "show authentication for root",
                "show property for 'root' ",
                "set property for 'root' 'max_user_connections' = '100'");
        checkGrant(image, "resource_global_grant", allows, new ArrayList<>());
    }

    @Test
    public void testPluginStmts() throws Exception {
        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                "create user pluginUser",
                "GRANT admin_priv on *.* TO pluginUser",
                "create role pluginRole",
                "GRANT admin_priv on *.* TO role pluginRole");
        // check twice, the second time is as follower
        for (int i = 0; i != 2; ++i) {
            if (i == 1) {
                replayUpgrade(image);
            }
            checkPrivilegeAsUser(
                    UserIdentity.createAnalyzedUserIdentWithIp("pluginUser", "%"),
                    "INSTALL PLUGIN FROM \"/home/users/starrocks/auditdemo.zip\"",
                    "UNINSTALL PLUGIN auditdemo",
                    "SHOW PLUGINS");

            checkPrivilegeAsUser(
                    createUserByRole("pluginRole"),
                    "INSTALL PLUGIN FROM \"/home/users/starrocks/auditdemo.zip\"",
                    "UNINSTALL PLUGIN auditdemo",
                    "SHOW PLUGINS");
        }
    }

    @Test
    public void testBlackList() throws Exception {
        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                "create user blacklistUser",
                "GRANT admin_priv on *.* TO blacklistUser",
                "create role blacklistRole",
                "GRANT admin_priv on *.* TO role blacklistRole");
        // check twice, the second time is as follower
        for (int i = 0; i != 2; ++i) {
            if (i == 1) {
                replayUpgrade(image);
            }
            checkPrivilegeAsUser(
                    UserIdentity.createAnalyzedUserIdentWithIp("blacklistUser", "%"),
                    "ADD SQLBLACKLIST \"select count\\\\(\\\\*\\\\) from .+\";",
                    "DELETE SQLBLACKLIST 0",
                    "SHOW SQLBLACKLIST");

            checkPrivilegeAsUser(
                    createUserByRole("blacklistRole"),
                    "ADD SQLBLACKLIST \"select count\\\\(\\\\*\\\\) from .+\";",
                    "DELETE SQLBLACKLIST 0",
                    "SHOW SQLBLACKLIST");
        }
    }

    @Test
    public void testShowFile() throws Exception {
        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                "create user fileAdminUser",
                "GRANT admin_priv on *.* TO fileAdminUser",
                "create role fileAdminRole",
                "GRANT admin_priv on *.* TO role fileAdminRole",
                "create user fileDbUser",
                "GRANT select_priv on db1.* TO fileDbUser",
                "create role fileDbRole",
                "GRANT select_priv on db1.* TO role fileDbRole",
                "create user fileTblUser",
                "GRANT select_priv on db1.tbl1 TO fileTblUser",
                "create role fileTblRole",
                "GRANT select_priv on db1.tbl1 TO role fileTblRole");
        // check twice, the second time is as follower
        for (int i = 0; i != 2; ++i) {
            if (i == 1) {
                replayUpgrade(image);
            }
            List<UserIdentity> allUsers = Arrays.asList(
                    UserIdentity.createAnalyzedUserIdentWithIp("fileAdminUser", "%"),
                    createUserByRole("fileAdminRole"),
                    UserIdentity.createAnalyzedUserIdentWithIp("fileDbUser", "%"),
                    createUserByRole("fileDbRole"),
                    UserIdentity.createAnalyzedUserIdentWithIp("fileTblUser", "%"),
                    createUserByRole("fileTblRole")
            );
            for (UserIdentity user : allUsers) {
                checkPrivilegeAsUser(user, "SHOW FILE FROM db1");
            }
        }
    }
    // TODO test table load
}
