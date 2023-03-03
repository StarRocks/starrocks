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

import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.UserDesc;
import com.starrocks.authentication.AuthenticationManager;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.common.Config;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.persist.AuthUpgradeInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.privilege.PrivilegeActions;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.PrivilegeCheckerV2;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRevokeClause;
import com.starrocks.sql.ast.GrantRevokePrivilegeObjects;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthUpgraderTest {
    private ConnectContext ctx;
    private long roleUserId = 0;

    private StarRocksAssert starRocksAssert;

    private UtFrameUtils.PseudoImage executeAndUpgrade(boolean onlyUpgradeJournal, StatementBase... sqls) throws Exception {
        GlobalStateMgr.getCurrentState().initAuth(false);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        // 1. execute old grant
        Auth auth = GlobalStateMgr.getCurrentState().getAuth();
        for (StatementBase sql : sqls) {
            Analyzer.analyze(sql, ctx);
            if (sql instanceof CreateUserStmt) {
                auth.createUser((CreateUserStmt) sql);
            } else if (sql instanceof CreateRoleStmt) {
                auth.createRole((CreateRoleStmt) sql);
            } else if (sql instanceof GrantPrivilegeStmt) {
                auth.grant((GrantPrivilegeStmt) sql);
            } else if (sql instanceof DropDbStmt) {
                DDLStmtExecutor.execute(sql, ctx);
            } else if (sql instanceof DropTableStmt) {
                DDLStmtExecutor.execute(sql, ctx);
            } else if (sql instanceof AlterUserStmt) {
                DDLStmtExecutor.execute(sql, ctx);
            } else if (sql instanceof DropUserStmt) {
                auth.dropUser((DropUserStmt) sql);
            }
            //DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);
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
        Auth relayAuth = Auth.read(dis);
        relayAuth.readAsGson(dis, -1);
        ctx.getGlobalStateMgr().initAuth(true);
        AuthUpgrader authUpgrader = new AuthUpgrader(
                relayAuth,
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
        starRocksAssert = new StarRocksAssert();
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

    private CreateUserStmt createUserStmt(String user) {
        return new CreateUserStmt(false, new UserDesc(new UserIdentity(user, "%")), Collections.emptyList());
    }

    private CreateRoleStmt createRoleStmt(String role) {
        return new CreateRoleStmt(Lists.newArrayList(role), false);
    }

    private GrantPrivilegeStmt grantPrivilegeStmt(List<String> priv, String objectType, List<String> objects, String user) {
        GrantRevokePrivilegeObjects g1 = new GrantRevokePrivilegeObjects();
        if (objectType.equals("USER")) {
            g1.setUserPrivilegeObjectList(Lists.newArrayList(new UserIdentity(objects.get(0), "%")));
        } else {
            g1.setPrivilegeObjectNameTokensList(Collections.singletonList(objects));
        }
        return new GrantPrivilegeStmt(
                Lists.newArrayList(priv),
                objectType,
                new GrantRevokeClause(new UserIdentity(user, "%"), null),
                g1, false);
    }

    private GrantPrivilegeStmt grantPrivilegeToRoleStmt(List<String> priv, String objectType, List<String> objects, String role) {
        GrantRevokePrivilegeObjects g1 = new GrantRevokePrivilegeObjects();
        if (objectType.equals("USER")) {
            g1.setUserPrivilegeObjectList(Lists.newArrayList(new UserIdentity(objects.get(0), "%")));
        } else {
            g1.setPrivilegeObjectNameTokensList(Collections.singletonList(objects));
        }
        return new GrantPrivilegeStmt(
                Lists.newArrayList(priv),
                objectType,
                new GrantRevokeClause(null, role),
                g1, false);
    }

    @Test
    public void testUpgradeAfterDbDropped() throws Exception {
        starRocksAssert.withDatabase("db2");

        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                createUserStmt("testusefordrop"),
                grantPrivilegeStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db2", "*"), "testusefordrop"),
                grantPrivilegeStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db1", "*"), "testusefordrop"),
                new DropDbStmt(false, "db2", true));

        // check twice, the second time is as follower
        for (int i = 0; i != 2; ++i) {
            if (i == 1) {
                replayUpgrade(image);
            }
            checkPrivilegeAsUser(
                    UserIdentity.createAnalyzedUserIdentWithIp("testusefordrop", "%"),
                    "select * from db1.tbl1");
            starRocksAssert.withDatabase("db2");
            String createTblStmtStr = "create table db2.tbl0 "
                    + "(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                    + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1)"
                    + " buckets 3 properties('replication_num' = '1');";
            starRocksAssert.withTable(createTblStmtStr);
            try {
                checkPrivilegeAsUser(
                        UserIdentity.createAnalyzedUserIdentWithIp("testusefordrop", "%"),
                        "select * from db2.tbl0");
            } catch (SemanticException e) {
                Assert.assertTrue(e.getMessage().contains("SELECT command denied to user 'testusefordrop'"));
            }
        }
    }

    @Test
    public void testUpgradeAfterTableDropped() throws Exception {
        starRocksAssert.withDatabase("db2");
        String createTblStmtStr = "create table db2.tbl0 "
                + "(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1)"
                + " buckets 3 properties('replication_num' = '1');";
        starRocksAssert.withTable(createTblStmtStr);

        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                createUserStmt("testusefordrop2"),
                grantPrivilegeStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db2", "tbl0"), "testusefordrop2"),
                grantPrivilegeStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db1", "*"), "testusefordrop2"),
                new DropTableStmt(false, new TableName("db2", "tbl0"), true));

        checkPrivilegeAsUser(
                UserIdentity.createAnalyzedUserIdentWithIp("testusefordrop2", "%"),
                "select * from db1.tbl1");

        starRocksAssert.withTable(createTblStmtStr);
        try {
            checkPrivilegeAsUser(
                    UserIdentity.createAnalyzedUserIdentWithIp("testusefordrop2", "%"),
                    "select * from db2.tbl0");
        } catch (SemanticException e) {
            Assert.assertTrue(e.getMessage().contains("SELECT command denied to user 'testusefordrop2'"));
        }
    }

    @Test
    public void testSelect() throws Exception {
        List<StatementBase> s = new ArrayList<>();
        s.add(createUserStmt("globalSelect"));


        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                createUserStmt("globalSelect"),
                grantPrivilegeStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("*", "*"), "globalSelect"),
                createUserStmt("dbSelect"),
                grantPrivilegeStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "*"), "dbSelect"),
                createUserStmt("tblSelect"),
                grantPrivilegeStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "tbl0"), "tblSelect"),
                createUserStmt("viewSelect"),
                grantPrivilegeStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "view"), "viewSelect"),
                createRoleStmt("globalSelect"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("*", "*"), "globalSelect"),
                createRoleStmt("dbSelect"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "*"), "dbSelect"),
                createRoleStmt("tblSelect"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "tbl0"), "tblSelect"),
                createRoleStmt("viewSelect"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "view"), "viewSelect")
        );
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
            checkBadPrivilegeAsUser(user, "select * from db1.tbl1",
                    "SELECT command denied to user 'dbSelect'");
            user = createUserByRole("dbSelect");
            checkPrivilegeAsUser(
                    user, "select * from db0.tbl0", "select * from db0.tbl1", "select * from db0.view");
            checkBadPrivilegeAsUser(user, "select * from db1.tbl1", "SELECT command denied to user");

            user = UserIdentity.createAnalyzedUserIdentWithIp("tblSelect", "%");
            checkPrivilegeAsUser(user, "select * from db0.tbl0");
            checkBadPrivilegeAsUser(user, "select * from db1.tbl1",
                    "SELECT command denied to user 'tblSelect'");
            user = createUserByRole("tblSelect");
            checkPrivilegeAsUser(user, "select * from db0.tbl0");
            checkBadPrivilegeAsUser(user, "select * from db1.tbl1", "SELECT command denied to user");

            user = UserIdentity.createAnalyzedUserIdentWithIp("viewSelect", "%");
            checkPrivilegeAsUser(user, "select * from db0.view");
            checkBadPrivilegeAsUser(user, "select * from db0.tbl0",
                    "SELECT command denied to user 'viewSelect'");
            user = createUserByRole("viewSelect");
            checkPrivilegeAsUser(user, "select * from db0.view");
            checkBadPrivilegeAsUser(user, "select * from db1.tbl1", "SELECT command denied to user");
        }
    }


    @Test
    public void testNoImage() throws Exception {
        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                false,
                createUserStmt("selectUser"),
                grantPrivilegeStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "tbl0"), "selectUser"),
                createRoleStmt("impersonateRole"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("IMPERSONATE"), "USER",
                        Lists.newArrayList("selectUser"), "impersonateRole"));

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
                createUserStmt("harry"),
                createUserStmt("gregory"),
                grantPrivilegeStmt(Lists.newArrayList("IMPERSONATE"), "USER",
                        Lists.newArrayList("gregory"), "harry"),
                createRoleStmt("harry"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("IMPERSONATE"), "USER",
                        Lists.newArrayList("gregory"), "harry"));

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
    public void testImpersonateAfterUserDropped() throws Exception {
        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                createUserStmt("testafteruserdropped"),
                createUserStmt("gregory1"),
                grantPrivilegeStmt(Lists.newArrayList("IMPERSONATE"), "USER",
                        Lists.newArrayList("gregory1"), "testafteruserdropped"),
                new DropUserStmt(new UserIdentity("gregory1", "%"), false));

        // check twice, the second time is as follower
        for (int i = 0; i != 2; ++i) {
            if (i == 1) {
                replayUpgrade(image);
            }
            ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("testafteruserdropped", "%"));
            Assert.assertFalse(ctx.getGlobalStateMgr().getPrivilegeManager().canExecuteAs(
                    ctx, UserIdentity.createAnalyzedUserIdentWithIp("gregory1", "%")));
        }
    }

    @Test
    public void testDomainUser() throws Exception {

        GrantRevokePrivilegeObjects g1 = new GrantRevokePrivilegeObjects();
        g1.setPrivilegeObjectNameTokensList(Collections.singletonList(Lists.newArrayList("db1", "tbl1")));
        GrantPrivilegeStmt grantPrivilegeStmt = new GrantPrivilegeStmt(
                Lists.newArrayList("SELECT_PRIV"),
                "TABLE",
                new GrantRevokeClause(new UserIdentity("domain_user", "localhost", true), null),
                g1, false);


        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                new CreateUserStmt(false, new UserDesc(new UserIdentity("domain_user",
                        "localhost", true)), Collections.emptyList()),
                grantPrivilegeStmt);


        // check twice, the second time is as follower
        for (int i = 0; i != 2; ++i) {
            if (i == 1) {
                replayUpgrade(image);
            }
            checkPrivilegeAsUser(UserIdentity.createAnalyzedUserIdentWithDomain(
                    "domain_user", "localhost"), "select * from db1.tbl1");
        }
    }

    private void checkPasswordEquals(String username, String pass) {
        AuthenticationManager authenticationManager = ctx.getGlobalStateMgr().getAuthenticationManager();
        UserIdentity userIdentity = new UserIdentity(username, "%");
        UserAuthenticationInfo info =
                authenticationManager.getUserToAuthenticationInfo().get(userIdentity);
        System.out.println(info.getPassword().length);
        System.out.println(ParseUtil.bytesToHexStr(info.getPassword()));
        Assert.assertArrayEquals(info.getPassword(), ParseUtil.hexStrToBytes(pass));
    }

    @Test
    public void testUserPasswordAfterUpgrade() throws Exception {
        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                createUserStmt("testuserpass1"),
                new CreateUserStmt(false, new UserDesc(new UserIdentity("testuserpass2", "%"),
                        "123456", true), Collections.emptyList()),
                new AlterUserStmt(
                        new UserDesc(new UserIdentity("root", "%"), "123456", true),
                        false
                ));

        // check twice, the second time is as follower
        for (int i = 0; i != 2; ++i) {
            if (i == 1) {
                replayUpgrade(image);
            }
            final String hexPass = "2A36424234383337454237343332393130354545343536384444413744433637454432434132414439";
            checkPasswordEquals("testuserpass1", "");
            checkPasswordEquals("testuserpass2", hexPass);
            checkPasswordEquals("root", hexPass);
        }
    }

    @Test
    public void testResource() throws Exception {
        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                createUserStmt("globalUsageResourceUser"),
                grantPrivilegeStmt(Lists.newArrayList("USAGE_PRIV"), "RESOURCE",
                        Lists.newArrayList("*"), "globalUsageResourceUser"),
                createUserStmt("globalUsageResourceUser1"),
                grantPrivilegeStmt(Lists.newArrayList("USAGE_PRIV"), "DATABASE",
                        Lists.newArrayList("*"), "globalUsageResourceUser1"),
                createUserStmt("oneUsageResourceUser"),
                grantPrivilegeStmt(Lists.newArrayList("USAGE_PRIV"), "RESOURCE",
                        Lists.newArrayList("hive0"), "oneUsageResourceUser"),
                createRoleStmt("globalUsageResourceRole"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("USAGE_PRIV"), "RESOURCE",
                        Lists.newArrayList("*"), "globalUsageResourceRole"),
                createRoleStmt("globalUsageResourceRole1"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("USAGE_PRIV"), "DATABASE",
                        Lists.newArrayList("*"), "globalUsageResourceRole1"),
                createRoleStmt("oneUsageResourceRole"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("USAGE_PRIV"), "RESOURCE",
                        Lists.newArrayList("hive0"), "oneUsageResourceRole"));

        // check twice, the second time is as follower
        for (int i = 0; i != 2; ++i) {
            if (i == 1) {
                replayUpgrade(image);
            }
            ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("globalUsageResourceUser", "%"));
            Assert.assertTrue(PrivilegeActions.checkResourceAction(ctx, "hive0", PrivilegeType.USAGE));
            ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("globalUsageResourceUser1", "%"));
            Assert.assertTrue(PrivilegeActions.checkResourceAction(ctx, "hive0", PrivilegeType.USAGE));
            ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("oneUsageResourceUser", "%"));
            Assert.assertTrue(PrivilegeActions.checkResourceAction(ctx, "hive0", PrivilegeType.USAGE));

            ctx.setCurrentUserIdentity(createUserByRole("globalUsageResourceRole"));
            Assert.assertTrue(PrivilegeActions.checkResourceAction(ctx, "hive0", PrivilegeType.USAGE));
            ctx.setCurrentUserIdentity(createUserByRole("globalUsageResourceRole1"));
            Assert.assertTrue(PrivilegeActions.checkResourceAction(ctx, "hive0", PrivilegeType.USAGE));
            ctx.setCurrentUserIdentity(createUserByRole("oneUsageResourceRole"));
            Assert.assertTrue(PrivilegeActions.checkResourceAction(ctx, "hive0", PrivilegeType.USAGE));
        }
    }

    @Test
    public void testCreate() throws Exception {
        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                createUserStmt("userWithGlobalCreate"),
                grantPrivilegeStmt(Lists.newArrayList("CREATE_PRIV"), "TABLE",
                        Lists.newArrayList("*", "*"), "userWithGlobalCreate"),
                createRoleStmt("userWithGlobalCreate"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("CREATE_PRIV"), "TABLE",
                        Lists.newArrayList("*", "*"), "userWithGlobalCreate"),
                createUserStmt("userWithResourceAllCreate"),
                grantPrivilegeStmt(Lists.newArrayList("CREATE_PRIV"), "RESOURCE",
                        Lists.newArrayList("*"), "userWithResourceAllCreate"),
                createUserStmt("userWithDbCreate"),
                grantPrivilegeStmt(Lists.newArrayList("CREATE_PRIV"), "TABLE",
                        Lists.newArrayList("db1", "*"), "userWithDbCreate"),
                createRoleStmt("userWithDbCreate"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("CREATE_PRIV"), "TABLE",
                        Lists.newArrayList("db1", "*"), "userWithDbCreate"),

                createUserStmt("tableCreate"),
                grantPrivilegeStmt(Lists.newArrayList("CREATE_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "tbl0"), "tableCreate"),
                createRoleStmt("tableCreate"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("CREATE_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "tbl0"), "tableCreate"),
                createUserStmt("viewCreate"),
                grantPrivilegeStmt(Lists.newArrayList("CREATE_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "view"), "viewCreate"),
                createRoleStmt("viewCreate"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("CREATE_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "view"), "viewCreate"));

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
                Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, "db0", PrivilegeType.CREATE_TABLE));
                Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, "db0", PrivilegeType.CREATE_VIEW));
                Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db0", PrivilegeType.CREATE_FUNCTION));
                Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.CREATE_TABLE));
                Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, "db1",
                        PrivilegeType.CREATE_MATERIALIZED_VIEW));
                // should have CREATE_DATABASE privilege on default_catalog
                Assert.assertTrue(PrivilegeActions.checkCatalogAction(ctx, "default_catalog",
                        PrivilegeType.CREATE_DATABASE));
            }
            ctx.setCurrentUserIdentity(createUserByRole("userWithGlobalCreate"));
            Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.CREATE_TABLE));
            Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.CREATE_VIEW));
            Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.CREATE_FUNCTION));
            Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, "db0", PrivilegeType.CREATE_TABLE));
            Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, "db0",
                    PrivilegeType.CREATE_MATERIALIZED_VIEW));

            // check: grant create_priv on db1.*
            ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("userWithDbCreate", "%"));
            Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.CREATE_TABLE));
            Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.CREATE_VIEW));
            Assert.assertFalse(PrivilegeActions.checkCatalogAction(ctx, "default_catalog",
                    PrivilegeType.CREATE_DATABASE));
            Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db0", PrivilegeType.CREATE_TABLE));
            Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db0", PrivilegeType.CREATE_VIEW));
            ctx.setCurrentUserIdentity(createUserByRole("userWithDbCreate"));
            Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.CREATE_TABLE));
            Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.CREATE_VIEW));
            Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db0", PrivilegeType.CREATE_TABLE));
            Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db0", PrivilegeType.CREATE_VIEW));

            // can't create view or table anymore
            for (UserIdentity userIdentity : Arrays.asList(
                    UserIdentity.createAnalyzedUserIdentWithIp("tableCreate", "%"),
                    createUserByRole("tableCreate"),
                    UserIdentity.createAnalyzedUserIdentWithIp("viewCreate", "%"),
                    createUserByRole("viewCreate"))) {
                ctx.setCurrentUserIdentity(userIdentity);
                Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db0", PrivilegeType.CREATE_TABLE));
                Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db0", PrivilegeType.CREATE_VIEW));
            }
        }
    }

    @Test
    public void testDrop() throws Exception {
        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                createUserStmt("userWithGlobalDrop"),
                grantPrivilegeStmt(Lists.newArrayList("DROP_PRIV"), "TABLE",
                        Lists.newArrayList("*", "*"), "userWithGlobalDrop"),
                createRoleStmt("userWithGlobalDrop"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("DROP_PRIV"), "TABLE",
                        Lists.newArrayList("*", "*"), "userWithGlobalDrop"),

                createUserStmt("userWithResourceAllDrop"),
                grantPrivilegeStmt(Lists.newArrayList("DROP_PRIV"), "RESOURCE",
                        Lists.newArrayList("*"), "userWithResourceAllDrop"),

                createUserStmt("userWithDbDrop"),
                grantPrivilegeStmt(Lists.newArrayList("DROP_PRIV"), "TABLE",
                        Lists.newArrayList("db1", "*"), "userWithDbDrop"),

                createRoleStmt("userWithDbDrop"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("DROP_PRIV"), "TABLE",
                        Lists.newArrayList("db1", "*"), "userWithDbDrop"),

                createUserStmt("tableDrop"),
                grantPrivilegeStmt(Lists.newArrayList("DROP_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "tbl0"), "tableDrop"),

                createRoleStmt("tableDrop"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("DROP_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "tbl0"), "tableDrop"),

                createUserStmt("viewDrop"),
                grantPrivilegeStmt(Lists.newArrayList("DROP_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "view"), "viewDrop"),

                createRoleStmt("viewDrop"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("DROP_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "view"), "viewDrop"));

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
                Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.CREATE_VIEW));
            }
            ctx.setCurrentUserIdentity(createUserByRole("userWithGlobalDrop"));
            Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.DROP));
            Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.CREATE_VIEW));

            // check: grant drop_priv on db1.*
            for (UserIdentity userIdentity : Arrays.asList(
                    UserIdentity.createAnalyzedUserIdentWithIp("userWithDbDrop", "%"),
                    createUserByRole("userWithDbDrop"))) {
                ctx.setCurrentUserIdentity(userIdentity);
                Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db0", PrivilegeType.DROP));
                Assert.assertTrue(PrivilegeActions.checkTableAction(
                        ctx, "db1", "tbl0", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeActions.checkTableAction(
                        ctx, "db0", "tbl0", PrivilegeType.DROP));
                Assert.assertTrue(PrivilegeActions.checkViewAction(
                        ctx, "db1", "view", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeActions.checkViewAction(
                        ctx, "db0", "view", PrivilegeType.DROP));
            }

            // check drop on table
            for (UserIdentity userIdentity : Arrays.asList(
                    UserIdentity.createAnalyzedUserIdentWithIp("tableDrop", "%"),
                    createUserByRole("tableDrop"))) {
                ctx.setCurrentUserIdentity(userIdentity);
                Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db0", PrivilegeType.DROP));
                Assert.assertTrue(PrivilegeActions.checkTableAction(
                        ctx, "db0", "tbl0", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeActions.checkTableAction(
                        ctx, "db0", "tbl1", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeActions.checkViewAction(
                        ctx, "db0", "view", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeActions.checkViewAction(
                        ctx, "db1", "view", PrivilegeType.DROP));
            }

            // check alter on view
            for (UserIdentity userIdentity : Arrays.asList(
                    UserIdentity.createAnalyzedUserIdentWithIp("viewDrop", "%"),
                    createUserByRole("viewDrop"))) {
                ctx.setCurrentUserIdentity(userIdentity);
                Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db0", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeActions.checkTableAction(
                        ctx, "db0", "tbl0", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeActions.checkTableAction(
                        ctx, "db0", "tbl1", PrivilegeType.DROP));
                Assert.assertTrue(PrivilegeActions.checkViewAction(
                        ctx, "db0", "view", PrivilegeType.DROP));
                Assert.assertFalse(PrivilegeActions.checkViewAction(
                        ctx, "db1", "view", PrivilegeType.DROP));
            }
        }
    }

    @Test
    public void testAlter() throws Exception {
        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                createUserStmt("userWithGlobalAlter"),
                grantPrivilegeStmt(Lists.newArrayList("ALTER_PRIV"), "TABLE",
                        Lists.newArrayList("*", "*"), "userWithGlobalAlter"),
                createRoleStmt("userWithGlobalAlter"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("ALTER_PRIV"), "TABLE",
                        Lists.newArrayList("*", "*"), "userWithGlobalAlter"),

                createUserStmt("userWithResourceAllAlter"),
                grantPrivilegeStmt(Lists.newArrayList("ALTER_PRIV"), "RESOURCE",
                        Lists.newArrayList("*"), "userWithResourceAllAlter"),

                createUserStmt("userWithDbAlter"),
                grantPrivilegeStmt(Lists.newArrayList("ALTER_PRIV"), "TABLE",
                        Lists.newArrayList("db1", "*"), "userWithDbAlter"),

                createRoleStmt("userWithDbAlter"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("ALTER_PRIV"), "TABLE",
                        Lists.newArrayList("db1", "*"), "userWithDbAlter"),

                createUserStmt("tableAlter"),
                grantPrivilegeStmt(Lists.newArrayList("ALTER_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "tbl0"), "tableAlter"),

                createRoleStmt("tableAlter"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("ALTER_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "tbl0"), "tableAlter"),
                createUserStmt("viewAlter"),
                grantPrivilegeStmt(Lists.newArrayList("ALTER_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "view"), "viewAlter"),
                createRoleStmt("viewAlter"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("ALTER_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "view"), "viewAlter"));

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
                Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.CREATE_VIEW));
            }
            ctx.setCurrentUserIdentity(createUserByRole("userWithGlobalAlter"));
            Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.ALTER));
            Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.CREATE_VIEW));

            // check: grant alter_priv on db1.*
            for (UserIdentity userIdentity : Arrays.asList(
                    UserIdentity.createAnalyzedUserIdentWithIp("userWithDbAlter", "%"),
                    createUserByRole("userWithDbAlter"))) {
                ctx.setCurrentUserIdentity(userIdentity);
                Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db0", PrivilegeType.ALTER));
                Assert.assertTrue(PrivilegeActions.checkTableAction(
                        ctx, "db1", "tbl0", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeActions.checkTableAction(
                        ctx, "db0", "tbl0", PrivilegeType.ALTER));
                Assert.assertTrue(PrivilegeActions.checkViewAction(
                        ctx, "db1", "view", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeActions.checkViewAction(
                        ctx, "db0", "view", PrivilegeType.ALTER));
            }

            // check alter on table
            for (UserIdentity userIdentity : Arrays.asList(
                    UserIdentity.createAnalyzedUserIdentWithIp("tableAlter", "%"),
                    createUserByRole("tableAlter"))) {
                ctx.setCurrentUserIdentity(userIdentity);
                Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db0", PrivilegeType.ALTER));
                Assert.assertTrue(PrivilegeActions.checkTableAction(
                        ctx, "db0", "tbl0", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeActions.checkTableAction(
                        ctx, "db0", "tbl1", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeActions.checkViewAction(
                        ctx, "db0", "view", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeActions.checkViewAction(
                        ctx, "db1", "view", PrivilegeType.ALTER));
            }

            // check alter on view
            for (UserIdentity userIdentity : Arrays.asList(
                    UserIdentity.createAnalyzedUserIdentWithIp("viewAlter", "%"),
                    createUserByRole("viewAlter"))) {
                ctx.setCurrentUserIdentity(userIdentity);
                Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db1", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db0", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeActions.checkTableAction(
                        ctx, "db0", "tbl0", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeActions.checkTableAction(
                        ctx, "db0", "tbl1", PrivilegeType.ALTER));
                Assert.assertTrue(PrivilegeActions.checkViewAction(
                        ctx, "db0", "view", PrivilegeType.ALTER));
                Assert.assertFalse(PrivilegeActions.checkViewAction(
                        ctx, "db1", "view", PrivilegeType.ALTER));
            }
        }
    }

    @Test
    public void testNodeAdmin() throws Exception {
        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                createUserStmt("nodeuser"),
                grantPrivilegeStmt(Lists.newArrayList("NODE_PRIV"), "TABLE",
                        Lists.newArrayList("*", "*"), "nodeuser"),
                createRoleStmt("noderole"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("NODE_PRIV"), "TABLE",
                        Lists.newArrayList("*", "*"), "noderole"),
                createUserStmt("noderesourceuser"),
                grantPrivilegeStmt(Lists.newArrayList("NODE_PRIV"), "RESOURCE",
                        Lists.newArrayList("*"), "noderesourceuser"),
                createRoleStmt("noderesourcerole"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("NODE_PRIV"), "RESOURCE",
                        Lists.newArrayList("*"), "noderesourcerole"),

                createUserStmt("adminuser"),
                grantPrivilegeStmt(Lists.newArrayList("ADMIN_PRIV"), "TABLE",
                        Lists.newArrayList("*", "*"), "adminuser"),

                createRoleStmt("adminrole"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("ADMIN_PRIV", "NODE_PRIV"), "TABLE",
                        Lists.newArrayList("*", "*"), "adminrole")

        );

        // check twice, the second time is as follower
        for (int i = 0; i != 2; ++i) {
            if (i == 1) {
                replayUpgrade(image);
            }
            for (String name : Arrays.asList("node", "noderesource")) {
                ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(name + "user", "%"));
                Assert.assertTrue(PrivilegeActions.checkSystemAction(ctx, PrivilegeType.NODE));
                ctx.setCurrentUserIdentity(createUserByRole(name + "role"));
                Assert.assertTrue(PrivilegeActions.checkSystemAction(ctx, PrivilegeType.NODE));
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
    private UtFrameUtils.PseudoImage initGrantImage(String name, List<String> grantPattern) throws Exception {
        String user = name + "_user";
        String role = name + "_role";

        return executeAndUpgrade(
                true,
                createUserStmt(user),
                grantPrivilegeStmt(Lists.newArrayList("GRANT_PRIV"), "TABLE", grantPattern, user),
                grantPrivilegeStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "*"), user),
                grantPrivilegeStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db1", "tbl1"), user),
                grantPrivilegeStmt(Lists.newArrayList("USAGE_PRIV"), "RESOURCE",
                        Lists.newArrayList("hive0"), user),

                createRoleStmt("" + role),
                grantPrivilegeToRoleStmt(Lists.newArrayList("GRANT_PRIV"), "TABLE", grantPattern, role),
                grantPrivilegeToRoleStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "*"), role),
                grantPrivilegeToRoleStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db1", "tbl1"), role),
                grantPrivilegeToRoleStmt(Lists.newArrayList("USAGE_PRIV"), "RESOURCE",
                        Lists.newArrayList("hive0"), role));
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
                        PrivilegeActions.checkResourceAction(ctx, "hive0", PrivilegeType.USAGE));

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
        UtFrameUtils.PseudoImage image = initGrantImage("global_grant", Lists.newArrayList("*", "*"));
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
        UtFrameUtils.PseudoImage image = initGrantImage("db_grant", Lists.newArrayList("db0", "*"));
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
        UtFrameUtils.PseudoImage image = initGrantImage("table_grant", Lists.newArrayList("db1", "tbl1"));
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
        String name = "resource_grant";
        String user = name + "_user";
        String role = name + "_role";

        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                createUserStmt(user),
                grantPrivilegeStmt(Lists.newArrayList("GRANT_PRIV"), "RESOURCE",
                        Lists.newArrayList("hive0"), user),
                grantPrivilegeStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "*"), user),
                grantPrivilegeStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db1", "tbl1"), user),
                grantPrivilegeStmt(Lists.newArrayList("USAGE_PRIV"), "RESOURCE",
                        Lists.newArrayList("hive0"), user),

                createRoleStmt("" + role),
                grantPrivilegeToRoleStmt(Lists.newArrayList("GRANT_PRIV"), "RESOURCE",
                        Lists.newArrayList("hive0"), role),
                grantPrivilegeToRoleStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "*"), role),
                grantPrivilegeToRoleStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db1", "tbl1"), role),
                grantPrivilegeToRoleStmt(Lists.newArrayList("USAGE_PRIV"), "RESOURCE",
                        Lists.newArrayList("hive0"), role));


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
        String name = "resource_global_grant";
        String user = name + "_user";
        String role = name + "_role";

        UtFrameUtils.PseudoImage image = executeAndUpgrade(
                true,
                createUserStmt(user),
                grantPrivilegeStmt(Lists.newArrayList("GRANT_PRIV"), "RESOURCE",
                        Lists.newArrayList("*"), user),
                grantPrivilegeStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "*"), user),
                grantPrivilegeStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db1", "tbl1"), user),
                grantPrivilegeStmt(Lists.newArrayList("USAGE_PRIV"), "RESOURCE",
                        Lists.newArrayList("hive0"), user),

                createRoleStmt("" + role),
                grantPrivilegeToRoleStmt(Lists.newArrayList("GRANT_PRIV"), "RESOURCE",
                        Lists.newArrayList("*"), role),
                grantPrivilegeToRoleStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db0", "*"), role),
                grantPrivilegeToRoleStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db1", "tbl1"), role),
                grantPrivilegeToRoleStmt(Lists.newArrayList("USAGE_PRIV"), "RESOURCE",
                        Lists.newArrayList("hive0"), role));


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
                createUserStmt("pluginUser"),
                grantPrivilegeStmt(Lists.newArrayList("ADMIN_PRIV"), "TABLE",
                        Lists.newArrayList("*", "*"), "pluginUser"),
                createRoleStmt("pluginRole"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("ADMIN_PRIV"), "TABLE",
                        Lists.newArrayList("*", "*"), "pluginRole"));

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
                createUserStmt("blacklistUser"),
                grantPrivilegeStmt(Lists.newArrayList("ADMIN_PRIV"), "TABLE",
                        Lists.newArrayList("*", "*"), "blacklistUser"),
                createRoleStmt("blacklistRole"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("ADMIN_PRIV"), "TABLE",
                        Lists.newArrayList("*", "*"), "blacklistRole"));

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
                createUserStmt("fileAdminUser"),
                grantPrivilegeStmt(Lists.newArrayList("ADMIN_PRIV"), "TABLE",
                        Lists.newArrayList("*", "*"), "fileAdminUser"),
                createRoleStmt("fileAdminRole"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("ADMIN_PRIV"), "TABLE",
                        Lists.newArrayList("*", "*"), "fileAdminRole"),

                createUserStmt("fileDbUser"),
                grantPrivilegeStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db1", "*"), "fileDbUser"),

                createRoleStmt("fileDbRole"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db1", "*"), "fileDbRole"),

                createUserStmt("fileTblUser"),
                grantPrivilegeStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db1", "tbl1"), "fileTblUser"),

                createRoleStmt("fileTblRole"),
                grantPrivilegeToRoleStmt(Lists.newArrayList("SELECT_PRIV"), "TABLE",
                        Lists.newArrayList("db1", "tbl1"), "fileTblRole"));

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
