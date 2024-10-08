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


package com.starrocks.authentication;

import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.Pair;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UserPropertyTest {

    private static String databaseName = "myDB";

    private static String catalogName = "myCatalog";

    private static ConnectContext connectContext;

    private static StarRocksAssert starRocksAssert;

    private static AuthorizationMgr authorizationManager;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);

        connectContext = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        starRocksAssert = new StarRocksAssert(connectContext);

        authorizationManager = starRocksAssert.getCtx().getGlobalStateMgr().getAuthorizationMgr();
        starRocksAssert.getCtx().setRemoteIP("localhost");
        authorizationManager.initBuiltinRolesAndUsers();

        authorizationManager = starRocksAssert.getCtx().getGlobalStateMgr().getAuthorizationMgr();
        starRocksAssert.getCtx().setRemoteIP("localhost");
        authorizationManager.initBuiltinRolesAndUsers();
        ctxToRoot();
    }

    private static void ctxToRoot() throws PrivilegeException {
        starRocksAssert.getCtx().setCurrentUserIdentity(UserIdentity.ROOT);
        starRocksAssert.getCtx().setCurrentRoleIds(
                starRocksAssert.getCtx().getGlobalStateMgr().getAuthorizationMgr().getRoleIdsByUser(UserIdentity.ROOT));

        starRocksAssert.getCtx().setQualifiedUser(UserIdentity.ROOT.getUser());
    }

    @Before
    public void setUp() throws Exception {
        GlobalStateMgr.getCurrentState().clear();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        if (starRocksAssert.getCtx().getGlobalStateMgr().getCatalogMgr().catalogExists(catalogName)) {
            DropCatalogStmt dropCatalogStmt = (DropCatalogStmt) UtFrameUtils.parseStmtWithNewParser(
                    String.format("DROP CATALOG IF EXISTS %s", catalogName), starRocksAssert.getCtx());
            starRocksAssert.getCtx().getGlobalStateMgr().getCatalogMgr().dropCatalog(dropCatalogStmt);
        }
    }

    @Test
    public void testUpdate_WithMaxConn() throws Exception {
        try {
            // set max connections too large
            List<Pair<String, String>> properties = new ArrayList<>();
            UserProperty userProperty = new UserProperty();
            properties.add(new Pair<>(UserProperty.PROP_MAX_USER_CONNECTIONS, "200000"));
            userProperty.update("root", properties);
            Assert.assertEquals(1, 2);
        } catch (Exception e) {
        }

        try {
            // set max connections too small
            List<Pair<String, String>> properties = new ArrayList<>();
            UserProperty userProperty = new UserProperty();
            properties.add(new Pair<>(UserProperty.PROP_MAX_USER_CONNECTIONS, "0"));
            userProperty.update("root", properties);
            Assert.assertEquals(1, 2);
        } catch (Exception e) {
        }

        try {
            // set max connections to a invalid value
            List<Pair<String, String>> properties = new ArrayList<>();
            UserProperty userProperty = new UserProperty();
            properties.add(new Pair<>(UserProperty.PROP_MAX_USER_CONNECTIONS, "xx"));
            userProperty.update("root", properties);
            Assert.assertEquals(1, 2);
        } catch (Exception e) {
        }

        try {
            // set max connections to a valid value
            List<Pair<String, String>> properties = new ArrayList<>();
            UserProperty userProperty = new UserProperty();
            properties.add(new Pair<>(UserProperty.PROP_MAX_USER_CONNECTIONS, "100"));
            userProperty.update("root", properties);
            Assert.assertEquals(100, userProperty.getMaxConn());
        } catch (Exception e) {
            Assert.assertEquals(1, 2);
        }

        try {
            // set max connections to a default value
            List<Pair<String, String>> properties = new ArrayList<>();
            UserProperty userProperty = new UserProperty();
            properties.add(new Pair<>(UserProperty.PROP_MAX_USER_CONNECTIONS, ""));
            userProperty.update("root", properties);
            Assert.assertEquals(UserProperty.MAX_CONN_DEFAULT_VALUE, userProperty.getMaxConn());
        } catch (Exception e) {
            throw e;
        }
    }

    @Test
    public void testUpdate_WithCatalog() throws Exception {
        try {
            // set the catalog property
            String catalogName = "myCatalog";
            String createExternalCatalog = "CREATE EXTERNAL CATALOG myCatalog " + "PROPERTIES( " + "   \"type\"=\"hive\", " +
                    "   \"hive.metastore.uris\"=\"thrift://xx.xx.xx.xx:9083\" " + ");";
            starRocksAssert.withCatalog(createExternalCatalog);

            // set by catalog
            List<Pair<String, String>> properties = new ArrayList<>();
            properties.add(new Pair<>(UserProperty.PROP_MAX_USER_CONNECTIONS, "2000"));
            properties.add(new Pair<>(UserProperty.PROP_CATALOG, catalogName));
            UserProperty userProperty = new UserProperty();
            userProperty.update("root", properties);
            Assert.assertEquals(2000, userProperty.getMaxConn());
            Assert.assertEquals(catalogName, userProperty.getCatalog());
            Assert.assertEquals(0, userProperty.getSessionVariables().size());

            // set by session.catalog
            properties = new ArrayList<>();
            properties.add(new Pair<>(UserProperty.PROP_MAX_USER_CONNECTIONS, "3000"));
            properties.add(new Pair<>("session.catalog", catalogName));
            userProperty = new UserProperty();
            userProperty.update("root", properties);
            Assert.assertEquals(3000, userProperty.getMaxConn());
            Assert.assertEquals(catalogName, userProperty.getCatalog());
            Assert.assertEquals(0, userProperty.getSessionVariables().size());

            // reset the catalog property
            properties = new ArrayList<>();
            properties.add(new Pair<>(UserProperty.PROP_MAX_USER_CONNECTIONS, ""));
            properties.add(new Pair<>(UserProperty.PROP_CATALOG, ""));
            userProperty = new UserProperty();
            userProperty.update("root", properties);
            Assert.assertEquals(UserProperty.MAX_CONN_DEFAULT_VALUE, userProperty.getMaxConn());
            Assert.assertEquals(
                    GlobalStateMgr.getCurrentState().getVariableMgr().getDefaultValue(SessionVariable.CATALOG),
                    userProperty.getCatalog());
            Assert.assertEquals(0, userProperty.getSessionVariables().size());
        } catch (Exception e) {
            throw e;
        }
    }

    @Test
    public void testUpdate_WithUser() throws Exception {
        try {
            String createExternalCatalog = "CREATE EXTERNAL CATALOG myCatalog " + "PROPERTIES( " + "   \"type\"=\"hive\", " +
                    "   \"hive.metastore.uris\"=\"thrift://xx.xx.xx.xx:9083\" " + ");";
            starRocksAssert.withCatalog(createExternalCatalog);

            AuthenticationMgr authenticationManager = starRocksAssert.getCtx().getGlobalStateMgr().getAuthenticationMgr();
            String createUserSql = "CREATE USER 'test' IDENTIFIED BY ''";
            CreateUserStmt createUserStmt =
                    (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());
            authenticationManager.createUser(createUserStmt);

            UserProperty userProperty = authenticationManager.getUserProperty("test");
            List<Pair<String, String>> properties = UserProperty.changeToPairList(
                    userProperty.getSessionVariables());
            properties.add(new Pair<>(UserProperty.PROP_MAX_USER_CONNECTIONS, "2000"));
            properties.add(new Pair<>(UserProperty.PROP_CATALOG, catalogName));
            authenticationManager.updateUserProperty("test", properties);
            Assert.assertEquals(catalogName, userProperty.getCatalog());
            Assert.assertEquals(2000, userProperty.getMaxConn());

            // we create a role 'r1' and grant it to user 'test'
            AuthorizationMgr authorizationMgr = starRocksAssert.getCtx().getGlobalStateMgr().getAuthorizationMgr();
            String createRoleSql = "CREATE ROLE r1";
            CreateRoleStmt createRoleStmt =
                    (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(createRoleSql, starRocksAssert.getCtx());
            authorizationMgr.createRole(createRoleStmt);

            String grantRoleSql = "GRANT r1 TO USER test";
            GrantRoleStmt grantRoleStmt =
                    (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(grantRoleSql, starRocksAssert.getCtx());
            authorizationMgr.grantRole(grantRoleStmt);

            GrantPrivilegeStmt grantPrivilegeStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                    "grant CREATE DATABASE on CATALOG myCatalog to role r1",
                    starRocksAssert.getCtx());
            authorizationMgr.grant(grantPrivilegeStmt);

            // Set Default Role
            UserIdentity testUser = authenticationManager.getUserIdentityByName("test");
            authorizationMgr.setUserDefaultRole(authorizationMgr.getAllRoleIds(testUser), testUser);

            // EXECUTE AS: the catalog property of root user is default_catalog, the catalog property of test user is myCatalog
            Assert.assertEquals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, starRocksAssert.getCtx().getCurrentCatalog());
            new StmtExecutor(starRocksAssert.getCtx(), UtFrameUtils.parseStmtWithNewParser(
                    String.format("EXECUTE AS test WITH NO REVERT;"), starRocksAssert.getCtx())).execute();
            Assert.assertEquals(catalogName, starRocksAssert.getCtx().getCurrentCatalog());
        } catch (Exception e) {
            throw e;
        }
    }

    @Test
    public void testUpdate_WithDatabase() throws Exception {
        try {
            // database does not exist
            UserProperty userProperty = new UserProperty();
            List<Pair<String, String>> properties = new ArrayList<>();
            properties.add(new Pair<>(UserProperty.PROP_DATABASE, "xxx"));
            userProperty.update("root", properties);
            Assert.assertEquals(1, 2);
        } catch (Exception e) {
        }

        try {
            // set database for root user
            starRocksAssert.withDatabase(databaseName);
            UserProperty userProperty = new UserProperty();
            List<Pair<String, String>> properties = new ArrayList<>();
            properties.add(new Pair<>(UserProperty.PROP_DATABASE, databaseName));
            userProperty.update("root", properties);
            Assert.assertEquals(databaseName, userProperty.getDatabase());

            // reset database for root user
            properties = new ArrayList<>();
            properties.add(new Pair<>(UserProperty.PROP_DATABASE, UserProperty.DATABASE_DEFAULT_VALUE));
            userProperty.update("root", properties);
            Assert.assertEquals(UserProperty.DATABASE_DEFAULT_VALUE, userProperty.getDatabase());
        } catch (Exception e) {
            throw e;
        }
    }

    @Test
    public void testUpdate_WithSessionVariables() throws Exception {
        UserProperty userProperty = new UserProperty();
        try {
            // session.aaa is not a valid session variable
            List<Pair<String, String>> properties = new ArrayList<>();
            properties.add(new Pair<>("session.aaa", "bbb"));
            userProperty.update("root", properties);
            Assert.assertEquals(1, 2);
        } catch (Exception e) {
            Assert.assertEquals(1, 1);
        }

        userProperty = new UserProperty();
        try {
            // the value type of session.wait_timeout is not correct
            List<Pair<String, String>> properties = new ArrayList<>();
            properties.add(new Pair<>("session.wait_timeout", "bbb"));
            userProperty.update("root", properties);
            Assert.assertEquals(1, 2);
        } catch (Exception e) {
            Assert.assertEquals(1, 1);
            Assert.assertEquals(0, userProperty.getSessionVariables().size());
        }

        userProperty = new UserProperty();
        try {
            // init_connect is a global session variable, can't be set.
            List<Pair<String, String>> properties = new ArrayList<>();
            properties.add(new Pair<>("session.init_connect", "bbb"));
            userProperty.update("root", properties);
            Assert.assertEquals(0, userProperty.getSessionVariables().size());
        } catch (Exception e) {
            Assert.assertEquals(1, 1);
            Assert.assertEquals(0, userProperty.getSessionVariables().size());
        }

        userProperty = new UserProperty();
        try {
            // system_time_zone is a read-only session variable, can't be set.
            List<Pair<String, String>> properties = new ArrayList<>();
            properties.add(new Pair<>("session.system_time_zone", "Asia/Shanghai"));
            userProperty.update("root", properties);
        } catch (Exception e) {
            Assert.assertEquals(1, 1);
            Assert.assertEquals(0, userProperty.getSessionVariables().size());
        }

        userProperty = new UserProperty();
        try {
            // session.wait_timeout is a valid session variable
            List<Pair<String, String>> properties = new ArrayList<>();
            properties.add(new Pair<>("session.wait_timeout", "1000"));
            userProperty.update("root", properties);
            Assert.assertEquals("1000", userProperty.getSessionVariables().get("wait_timeout"));

            // reset session.wait_timeout
            properties = new ArrayList<>();
            properties.add(new Pair<>("session.wait_timeout", UserProperty.EMPTY_VALUE));
            userProperty.update("root", properties);
            Assert.assertEquals(0, userProperty.getSessionVariables().size());
        } catch (Exception e) {
            Assert.assertEquals(1, 2);
        }
    }

    @Test
    public void testUpdateForReplayJournal() {
        try {
            // updateForReplayJournal must not throw any exception
            List<Pair<String, String>> properties = new ArrayList<>();
            properties.add(new Pair<>(UserProperty.PROP_MAX_USER_CONNECTIONS, "2000"));
            properties.add(new Pair<>(UserProperty.PROP_DATABASE, "database"));
            properties.add(new Pair<>(UserProperty.PROP_CATALOG, "catalog"));
            properties.add(new Pair<>("session.aaa", "bbb"));
            properties.add(new Pair<>("xxx", "yyy"));

            UserProperty userProperty = new UserProperty();
            userProperty.updateForReplayJournal(properties);

            Assert.assertEquals(2000, userProperty.getMaxConn());
            Assert.assertEquals("database", userProperty.getDatabase());
            Assert.assertEquals("catalog", userProperty.getCatalog());
            Map<String, String> sessionVariables = userProperty.getSessionVariables();
            Assert.assertEquals(1, sessionVariables.size());
            Assert.assertEquals("bbb", sessionVariables.get("aaa"));
        } catch (Exception e) {
            throw e;
        }

        try {
            // set the user connection to a invalid value
            List<Pair<String, String>> properties = new ArrayList<>();
            properties.add(new Pair<>(UserProperty.PROP_MAX_USER_CONNECTIONS, "200d"));
            properties.add(new Pair<>(UserProperty.PROP_DATABASE, "database"));
            properties.add(new Pair<>(UserProperty.PROP_CATALOG, "catalog"));
            properties.add(new Pair<>("session.aaa", "bbb"));
            properties.add(new Pair<>("xxx", "yyy"));

            UserProperty userProperty = new UserProperty();
            userProperty.updateForReplayJournal(properties);

            Assert.assertEquals(UserProperty.MAX_CONN_DEFAULT_VALUE, userProperty.getMaxConn());
            Assert.assertEquals("database", userProperty.getDatabase());
            Assert.assertEquals("catalog", userProperty.getCatalog());
            Map<String, String> sessionVariables = userProperty.getSessionVariables();
            Assert.assertEquals(1, sessionVariables.size());
            Assert.assertEquals("bbb", sessionVariables.get("aaa"));
        } catch (Exception e) {
            throw e;
        }
    }

    @Test
    public void testUpdateSessionContext_WithSomeAbnormalCases() throws Exception {
        ConnectContext context = new ConnectContext(null);
        UserProperty userProperty = null;
        try {
            // Update By default UserProperty
            userProperty = new UserProperty();
            context.updateByUserProperty(userProperty);
        } catch (Exception e) {
            throw e;
        }
        Assert.assertEquals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, context.getCurrentCatalog());
        Assert.assertEquals(UserProperty.DATABASE_DEFAULT_VALUE, context.getDatabase());
        Assert.assertEquals(UserProperty.MAX_CONN_DEFAULT_VALUE, userProperty.getMaxConn());

        try {
            // database does not exist
            userProperty = new UserProperty();
            userProperty.setDatabase("database");
            context.updateByUserProperty(userProperty);
        } catch (Exception e) {
            throw e;
        }
        Assert.assertEquals("", context.getDatabase());

        try {
            // session variable is not valid
            userProperty = new UserProperty();
            Map<String, String> sessionVariables = userProperty.getSessionVariables();
            sessionVariables.put("aaa", "bbb");
            userProperty.setSessionVariables(sessionVariables);
            context.updateByUserProperty(userProperty);
        } catch (Exception e) {
            throw e;
        }

        try {
            // session variable is valid
            userProperty = new UserProperty();
            Map<String, String> sessionVariables = userProperty.getSessionVariables();
            sessionVariables.put("statistic_collect_parallel", "2");
            userProperty.setSessionVariables(sessionVariables);
            context.updateByUserProperty(userProperty);
        } catch (Exception e) {
            throw e;
        }
        Assert.assertEquals(2, context.getSessionVariable().getStatisticCollectParallelism());

        try {
            // the session variable statistic_collect_parallel has been set to 2, and it is not equal to its default value 1
            // updateByUserProperty will ignore setting the session variable statistic_collect_parallel.
            userProperty = new UserProperty();
            Map<String, String> sessionVariables = userProperty.getSessionVariables();
            sessionVariables.put("statistic_collect_parallel", "100");
            userProperty.setSessionVariables(sessionVariables);
            context.updateByUserProperty(userProperty);
        } catch (Exception e) {
            throw e;
        }
        Assert.assertEquals(2, context.getSessionVariable().getStatisticCollectParallelism()); // not 100

        try {
            // catalog is valid
            String createExternalCatalog = "CREATE EXTERNAL CATALOG myCatalog " + "PROPERTIES( " + "   \"type\"=\"hive\", " +
                    "   \"hive.metastore.uris\"=\"thrift://xx.xx.xx.xx:9083\" " + ");";
            starRocksAssert.withCatalog(createExternalCatalog);

            userProperty = new UserProperty();
            Map<String, String> sessionVariables = userProperty.getSessionVariables();
            sessionVariables.put("catalog", "myCatalog");
            userProperty.setSessionVariables(sessionVariables);

            context.setCurrentUserIdentity(UserIdentity.ROOT);
            context.setCurrentRoleIds(
                    starRocksAssert.getCtx().getGlobalStateMgr().getAuthorizationMgr().getRoleIdsByUser(UserIdentity.ROOT));
            context.updateByUserProperty(userProperty);
        } catch (Exception e) {
            throw e;
        }
        Assert.assertEquals("myCatalog", context.getSessionVariable().getCatalog());

    }

    @Test
    public void testGetCatalogDbName() {
        UserProperty userProperty = new UserProperty();
        userProperty.setDatabase("db");
        userProperty.setCatalog("catalog");
        String name = userProperty.getCatalogDbName();
        Assert.assertEquals("catalog.db", name);

    }

    @Test
    public void testGetMaxConn() {
        UserProperty userProperty = new UserProperty();
        long maxConnections = userProperty.getMaxConn();
        Assert.assertEquals(1024, maxConnections);
    }

    @Test
    public void testGetDefaultSessionDatabase() {
        UserProperty userProperty = new UserProperty();
        String defaultSessionDatabase = userProperty.getDatabase();
        Assert.assertEquals("", defaultSessionDatabase);
    }

    @Test
    public void testGetDefaultSessionCatalog() {
        UserProperty userProperty = new UserProperty();
        String defaultSessionCatalog = userProperty.getCatalog();
        Assert.assertEquals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, defaultSessionCatalog);
    }

    @Test
    public void testGetSessionVariables() {
        UserProperty userProperty = new UserProperty();
        Map<String, String> sessionVariables = userProperty.getSessionVariables();
        Assert.assertEquals(0, sessionVariables.size());
    }
}
