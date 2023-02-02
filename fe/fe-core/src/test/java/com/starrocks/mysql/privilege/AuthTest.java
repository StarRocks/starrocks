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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/mysql/privilege/AuthTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.mysql.privilege;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.ResourcePattern;
import com.starrocks.analysis.TablePattern;
import com.starrocks.analysis.UserDesc;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.AccessPrivilege;
import com.starrocks.catalog.DomainResolver;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.security.LdapSecurity;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ImpersonatePrivInfo;
import com.starrocks.persist.PrivInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.RevokeRoleStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class AuthTest {

    private Auth auth;
    @Mocked
    public GlobalStateMgr globalStateMgr;
    @Mocked
    private Analyzer analyzer;
    @Mocked
    private EditLog editLog;
    @Mocked
    private ConnectContext ctx;
    @Mocked
    public LdapSecurity ldapSecurity;

    private MockDomianResolver resolver;

    // Thread is not mockable in Jmockit, so use a subclass instead.
    private static final class MockDomianResolver extends DomainResolver {
        public MockDomianResolver(Auth auth) {
            super(auth);
        }

        @Override
        public boolean resolveWithBNS(String domainName, Set<String> resolvedIPs) {
            switch (domainName) {
                case "starrocks.domain1":
                    resolvedIPs.add("10.1.1.1");
                    resolvedIPs.add("10.1.1.2");
                    resolvedIPs.add("10.1.1.3");
                    break;
                case "starrocks.domain2":
                    resolvedIPs.add("20.1.1.1");
                    resolvedIPs.add("20.1.1.2");
                    resolvedIPs.add("20.1.1.3");
                    break;
            }
            return true;
        }
    }

    @Before
    public void setUp() throws NoSuchMethodException, SecurityException {
        auth = new Auth();

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getAuth();
                minTimes = 0;
                result = auth;

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;

                editLog.logCreateUser((PrivInfo) any);
                minTimes = 0;

                ConnectContext.get();
                minTimes = 0;
                result = ctx;

                ctx.getQualifiedUser();
                minTimes = 0;
                result = "root";

                ctx.getRemoteIP();
                minTimes = 0;
                result = "192.168.1.1";

                ctx.getState();
                minTimes = 0;
                result = new QueryState();

                ctx.getCurrentUserIdentity();
                minTimes = 0;
                result = UserIdentity.createAnalyzedUserIdentWithIp("root", "%");
            }
        };

        resolver = new MockDomianResolver(auth);
    }

    @After
    public void tearDown() {
        Config.enable_validate_password = false;  // skip password validation
    }

    @Test
    public void test() throws IllegalArgumentException {
        // 1. create cmy@%
        String createUserSql = "CREATE USER 'cmy' IDENTIFIED BY '12345'";
        CreateUserStmt createUserStmt = null;
        try {
            createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
            auth.createUser(createUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        UserIdentity userIdentity = createUserStmt.getUserIdent();

        // 2. check if cmy from specified ip can access
        List<UserIdentity> currentUser = Lists.newArrayList();
        Assert.assertTrue(auth.checkPlainPassword("cmy", "192.168.0.1", "12345",
                currentUser));
        Assert.assertFalse(auth.checkPlainPassword("cmy", "192.168.0.1",
                "123456", null));
        Assert.assertTrue(currentUser.get(0).equals(userIdentity));

        // 3. create another user: zhangsan@"192.%"
        createUserSql = "CREATE USER 'zhangsan'@'192.%' IDENTIFIED BY '12345'";
        try {
            createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
            auth.createUser(createUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        userIdentity = createUserStmt.getUserIdent();

        // 4. check if zhangsan from specified ip can access
        Assert.assertTrue(auth.checkPlainPassword("zhangsan", "192.168.0.1",
                "12345", null));
        Assert.assertFalse(auth.checkPlainPassword("zhangsan", "172.168.0.1",
                "12345", null));

        // 4.1 check if we can create same user
        Config.enable_password_reuse = true;
        try {
            createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        boolean hasException = false;
        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 4.2 check if we can create same user name with different host
        createUserSql = "CREATE USER 'zhangsan'@'172.18.1.1' IDENTIFIED BY '12345'";
        try {
            createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
            auth.createUser(createUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        Assert.assertTrue(auth.checkPlainPassword("zhangsan", "172.18.1.1",
                "12345", null));

        // 5. create a user with domain [starrocks.domain]
        createUserSql = "CREATE USER 'zhangsan'@['starrocks.domain1'] IDENTIFIED BY '12345'";
        try {
            createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
            auth.createUser(createUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 5.1 resolve domain [starrocks.domain1]
        resolver.runAfterCatalogReady();

        // 6. check if user from resolved ip can access
        Assert.assertTrue(auth.checkPlainPassword("zhangsan", "10.1.1.1",
                "12345", null));
        Assert.assertFalse(auth.checkPlainPassword("zhangsan", "10.1.1.1",
                "123456", null));

        // 7. add duplicated user@['starrocks.domain1']
        try {
            createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        hasException = false;
        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 8. add another user@['starrocks.domain2']
        createUserSql = "CREATE USER 'lisi'@['starrocks.domain2'] IDENTIFIED BY '123456'";
        try {
            createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
            auth.createUser(createUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 8.1 resolve domain [starrocks.domain2]
        resolver.runAfterCatalogReady();

        Assert.assertTrue(auth.checkPlainPassword("lisi", "20.1.1.1",
                "123456", null));
        Assert.assertFalse(auth.checkPlainPassword("lisi", "10.1.1.1",
                "123456", null));
        Assert.assertFalse(auth.checkPlainPassword("lisi", "20.1.1.2",
                "123455", null));

        /*
         * Now we have 4 users:
         * cmy@'%'
         * zhangsan@"192.%"
         * zhangsan@['starrocks.domain1']
         * lisi@['starrocks.domain2']
         */

        // 9. grant for cmy@'%'
        GrantPrivilegeStmt grantStmt = null;
        try {
            String sql = "GRANT CREATE_PRIV,DROP_PRIV on '*'.'*' to cmy";
            grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        List<UserIdentity> currentUser2 = Lists.newArrayList();
        auth.checkPlainPassword("cmy", "172.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        // check auth before grant
        Assert.assertFalse(auth.checkDbPriv(currentUser2.get(0), "db1",
                PrivPredicate.CREATE));

        try {
            auth.grant(grantStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 9.1 check auth
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), "db1",
                PrivPredicate.CREATE));
        UserIdentity zhangsan1 =
                UserIdentity.createAnalyzedUserIdentWithIp("zhangsan", "172.1.1.1");
        Assert.assertFalse(auth.checkDbPriv(zhangsan1, "db1",
                PrivPredicate.CREATE));

        // 10. grant auth for non exist user
        hasException = false;
        try {
            String sql = "GRANT CREATE_PRIV,DROP_PRIV on '*'.'*' to nouser";
            grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 11. grant auth for user with non exist host
        hasException = false;
        try {
            String sql = "GRANT SELECT,DROP_PRIV on *.* to zhangsan";
            grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 12. grant db auth to exist user
        try {
            String sql = "GRANT SELECT_PRIV,DROP_PRIV on db1.* to zhangsan@'192.%'";
            grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.grant(grantStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword("zhangsan", "192.168.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());

        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), "db1",
                PrivPredicate.SELECT));
        Assert.assertFalse(auth.checkGlobalPriv(currentUser2.get(0), PrivPredicate.SELECT));
        Assert.assertTrue(auth.checkTblPriv(currentUser2.get(0), "db1",
                "tbl1", PrivPredicate.SELECT));

        // 13. grant tbl auth to exist user
        try {
            String sql = "GRANT ALTER_PRIV,DROP_PRIV on db2.tbl2 to zhangsan@'192.%'";
            grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.grant(grantStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword("zhangsan", "192.168.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertFalse(auth.checkDbPriv(currentUser2.get(0), "db2",
                PrivPredicate.SELECT));
        Assert.assertFalse(auth.checkGlobalPriv(currentUser2.get(0), PrivPredicate.SELECT));
        Assert.assertTrue(auth.checkTblPriv(currentUser2.get(0), "db2", "tbl2",
                PrivPredicate.DROP));

        // 14. grant db auth to zhangsan@['starrocks.domain1']
        try {
            String sql = "GRANT alter,drop on db3.* to 'zhangsan'@['starrocks.domain1']";
            grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            auth.grant(grantStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword("zhangsan", "10.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), "db3",
                PrivPredicate.ALTER));
        // 15. grant new auth to exist priv entry (exist ALTER/DROP, add SELECT)
        try {
            String sql = "GRANT SELECT_PRIV on db3.* to zhangsan@['starrocks.domain1']";
            grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            auth.grant(grantStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword("zhangsan", "10.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), "db3",
                PrivPredicate.SELECT));

        currentUser2.clear();
        auth.checkPlainPassword("zhangsan", "10.1.1.2", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), "db3",
                PrivPredicate.ALTER));
        currentUser2.clear();
        auth.checkPlainPassword("zhangsan", "10.1.1.3", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), "db3",
                PrivPredicate.DROP));

        /*
         * for now, we have following auth:
         * cmy@'%'
         *      *.* -> CREATE/DROP
         * zhangsan@"192.%"
         *      db1.* -> SELECT/DROP
         *      db2.tbl2 -> ALTER/DROP
         * zhangsan@['starrocks.domain1']
         *      db3.* -> ALTER/DROP/SELECT
         * lisi@['starrocks.domain2']
         *      N/A
         */

        // 16. revoke privs from non exist user
        String sql = "REVOKE SELECT_PRIV ON *.* FROM nouser";
        RevokePrivilegeStmt revokeStmt = null;
        hasException = false;
        try {
            revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 17. revoke privs from non exist host
        sql = "REVOKE SELECT_PRIV ON *.* FROM cmy@'172.%'";
        hasException = false;
        try {
            revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 18. revoke privs from non exist db
        sql = "REVOKE SELECT_PRIV ON nodb.* FROM cmy";
        hasException = false;
        try {
            revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            auth.revoke(revokeStmt);
        } catch (Exception e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 19. revoke privs from user @ ip
        sql = "REVOKE CREATE_PRIV ON *.* FROM cmy";
        try {
            revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword("cmy", "172.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), "db",
                PrivPredicate.CREATE));
        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(auth.checkDbPriv(currentUser2.get(0), "db",
                PrivPredicate.CREATE));
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), "db",
                PrivPredicate.DROP));

        // 19. revoke tbl privs from user @ ip
        sql = "REVOKE ALTER_PRIV ON db2.tbl2 FROM zhangsan@'192.%'";
        try {
            revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword("zhangsan", "192.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkTblPriv(currentUser2.get(0), "db2",
                "tbl2", PrivPredicate.ALTER));
        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword("zhangsan", "192.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertFalse(auth.checkTblPriv(currentUser2.get(0), "db2",
                "tbl2", PrivPredicate.ALTER));
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), "db1",
                PrivPredicate.SELECT));

        // 20. revoke privs from non exist user @ domain
        sql = "REVOKE ALTER_PRIV ON db2.tbl2 FROM zhangsan@nodomain";
        hasException = false;
        try {
            revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 21. revoke privs from non exist db from user @ domain
        sql = "REVOKE ALTER_PRIV ON nodb.* FROM zhangsan@['starrocks.domain1']";
        hasException = false;
        try {
            revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            auth.revoke(revokeStmt);
        } catch (Exception e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 22. revoke privs from exist user @ domain
        sql = "REVOKE DROP_PRIV ON db3.* FROM zhangsan@['starrocks.domain1']";
        try {
            revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword("zhangsan", "10.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), "db3",
                PrivPredicate.DROP));

        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword("zhangsan", "10.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertFalse(auth.checkDbPriv(currentUser2.get(0), "db3",
                PrivPredicate.DROP));

        /*
         * for now, we have following auth:
         * cmy@'%'
         *      *.* -> DROP
         * zhangsan@"192.%"
         *      db1.* -> SELECT/DROP
         *      db2.tbl2 -> DROP
         * zhangsan@['starrocks.domain1']
         *      db3.* -> ALTER/SELECT
         * lisi@['starrocks.domain2']
         *      N/A
         */

        // 23. create admin role, which is not allowed
        String createRoleSql = "CREATE ROLE admin";
        CreateRoleStmt roleStmt = null;
        hasException = false;
        try {
            UtFrameUtils.parseStmtWithNewParser(createRoleSql, ctx);
        } catch (Exception e1) {
            e1.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 23. create operator role, which is not allowed
        createRoleSql = "CREATE ROLE operator";
        hasException = false;
        try {
            UtFrameUtils.parseStmtWithNewParser(createRoleSql, ctx);
        } catch (Exception e1) {
            e1.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 24. create role
        createRoleSql = "CREATE ROLE role1";
        try {
            roleStmt = (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(createRoleSql, ctx);
        } catch (Exception e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        try {
            auth.createRole(roleStmt);
        } catch (DdlException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // 25. grant auth to non exist role, will report error
        sql = "GRANT DROP_PRIV, SELECT_PRIV TO *.* ON ROLE role2";
        hasException = false;
        try {
            grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e1) {
            e1.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 26. grant auth to role
        sql = "GRANT DROP_PRIV, SELECT_PRIV ON *.* TO ROLE role1";
        try {
            grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        try {
            auth.grant(grantStmt);
        } catch (DdlException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // 27. create user and set it as role1
        createUserSql = "CREATE USER 'wangwu' IDENTIFIED BY '12345' DEFAULT ROLE 'role1'";
        try {
            createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
            auth.createUser(createUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword("wangwu", "10.17.2.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), "db4",
                PrivPredicate.DROP));

        // 28. create user@domain and set it as role1
        createUserSql = "CREATE USER 'chenliu'@['starrocks.domain2'] IDENTIFIED BY '12345' DEFAULT ROLE 'role1'";
        try {
            createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
            auth.createUser(createUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword("chenliu", "20.1.1.1", "12345", currentUser2);
        Assert.assertEquals(0, currentUser2.size());
        resolver.runAfterCatalogReady();
        currentUser2.clear();
        auth.checkPlainPassword("chenliu", "20.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), "db4",
                PrivPredicate.DROP));

        // 29. revoke auth on non exist db from role1
        sql = "REVOKE DROP_PRIV ON nodb.* FROM ROLE role1";
        try {
            revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        hasException = false;
        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e1) {
            e1.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 30. revoke auth from role1
        sql = "REVOKE DROP_PRIV on *.* FROM ROLE role1";
        try {
            revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword("chenliu", "20.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertFalse(auth.checkDbPriv(currentUser2.get(0), "db4",
                PrivPredicate.DROP));

        // 31. drop role, privs remain unchanged
        /*
        String dropRoleSql = "DROP ROLE role1";
        DropRoleStmt dropRoleStmt;
        try {
            dropRoleStmt = (DropRoleStmt) UtFrameUtils.parseStmtWithNewParser(dropRoleSql, ctx);
            auth.dropRole(dropRoleStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
         */

        currentUser2.clear();
        auth.checkPlainPassword("chenliu", "20.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertFalse(auth.checkDbPriv(currentUser2.get(0), "db4",
                PrivPredicate.DROP));

        // 32. drop user cmy@"%"
        String dropSql = "DROP USER 'cmy'";
        DropUserStmt dropUserStmt = null;
        try {
            dropUserStmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(dropSql, ctx);
            auth.dropUser(dropUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        Assert.assertFalse(
                auth.checkPlainPassword("cmy", "192.168.0.1", "12345", null));
        Assert.assertTrue(auth.checkPlainPassword("zhangsan", "192.168.0.1",
                "12345", null));

        // 33. drop user zhangsan@"192.%"
        dropSql = "DROP USER 'zhangsan'@'192.%'";
        try {
            dropUserStmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(dropSql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(
                auth.checkPlainPassword("zhangsan", "192.168.0.1", "12345", null));

        try {
            auth.dropUser(dropUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
        Assert.assertFalse(
                auth.checkPlainPassword("zhangsan", "192.168.0.1", "12345", null));
        Assert.assertTrue(
                auth.checkPlainPassword("zhangsan", "10.1.1.1", "12345", null));

        // 34. create user zhangsan@'10.1.1.1' to overwrite one of zhangsan@['starrocks.domain1']
        createUserSql = "CREATE USER 'zhangsan'@'10.1.1.1' IDENTIFIED BY 'abcde'";
        try {
            createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        Assert.assertTrue(
                auth.checkPlainPassword("zhangsan", "10.1.1.1", "12345", null));

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(
                auth.checkPlainPassword("zhangsan", "10.1.1.1", "12345", null));
        Assert.assertTrue(
                auth.checkPlainPassword("zhangsan", "10.1.1.1", "abcde", null));

        // 35. drop user zhangsan@['starrocks.domain1']
        dropSql = "DROP USER 'zhangsan'@['starrocks.domain1']";
        try {
            dropUserStmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(dropSql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(
                auth.checkPlainPassword("zhangsan", "10.1.1.2", "12345", null));

        try {
            auth.dropUser(dropUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
        Assert.assertTrue(
                auth.checkPlainPassword("zhangsan", "10.1.1.2", "12345", null));

        resolver.runAfterCatalogReady();
        Assert.assertFalse(
                auth.checkPlainPassword("zhangsan", "10.1.1.2", "12345", null));
        Assert.assertTrue(
                auth.checkPlainPassword("zhangsan", "10.1.1.1", "abcde", null));

        // 36. drop user lisi@['starrocks.domain2']
        dropSql = "DROP USER 'lisi'@['starrocks.domain2']";
        try {
            dropUserStmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(dropSql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(
                auth.checkPlainPassword("lisi", "20.1.1.1", "123456", null));
        Assert.assertFalse(
                auth.checkPlainPassword("lisi", "10.1.1.1", "123456", null));

        try {
            auth.dropUser(dropUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
        Assert.assertTrue(
                auth.checkPlainPassword("lisi", "20.1.1.1", "123456", null));
        Assert.assertFalse(
                auth.checkPlainPassword("lisi", "10.1.1.1", "123456", null));

        resolver.runAfterCatalogReady();
        Assert.assertFalse(
                auth.checkPlainPassword("lisi", "20.1.1.1", "123456", null));
        Assert.assertFalse(
                auth.checkPlainPassword("lisi", "10.1.1.1", "123456", null));

        // 37. drop zhangsan@'172.18.1.1' and zhangsan@'172.18.1.1'
        dropSql = "DROP USER 'zhangsan'@'172.18.1.1'";
        try {
            dropUserStmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(dropSql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.dropUser(dropUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }

        dropSql = "DROP USER 'zhangsan'@'10.1.1.1'";
        try {
            dropUserStmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(dropSql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.dropUser(dropUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
        Assert.assertFalse(
                auth.checkPlainPassword("zhangsan", "10.1.1.1", "abcde", null));

        // 38.1 grant node_priv to user

        createUserSql = "CREATE USER 'zhaoliu' IDENTIFIED BY '12345'";
        try {
            createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
            auth.createUser(createUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        userIdentity = createUserStmt.getUserIdent();

        sql = "GRANT NODE_PRIV ON *.* TO zhaoliu";
        try {
            grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        try {
            auth.grant(grantStmt);
        } catch (DdlException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // 38.2 revoke node_priv from user
        currentUser2.clear();
        auth.checkPlainPassword("zhaoliu", "", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkGlobalPriv(currentUser2.get(0), PrivPredicate.OPERATOR));

        sql = "REVOKE NODE_PRIV ON *.* FROM zhaoliu";
        try {
            revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(auth.checkGlobalPriv(currentUser2.get(0), PrivPredicate.OPERATOR));

        // 38.3 grant node_priv to role
        sql = "GRANT NODE_PRIV ON *.* TO ROLE role1";
        try {
            grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        try {
            auth.grant(grantStmt);
        } catch (DdlException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // 38.4 revoke node_priv from role
        createUserSql = "CREATE USER 'sunqi' IDENTIFIED BY '12345' DEFAULT ROLE 'role1'";
        try {
            createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
            auth.createUser(createUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword("sunqi", "", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkGlobalPriv(currentUser2.get(0), PrivPredicate.OPERATOR));

        sql = "REVOKE NODE_PRIV ON *.* FROM ROLE role1";
        try {
            revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(auth.checkGlobalPriv(currentUser2.get(0), PrivPredicate.OPERATOR));
    }

    @Test
    public void testGrantRevokeRole() throws Exception {
        // 1. create user with no role specified
        String createUserSql = "CREATE USER 'test_user' IDENTIFIED BY '12345'";
        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
        auth.createUser(createUserStmt);
        UserIdentity userIdentity = createUserStmt.getUserIdent();

        // check if select & load & spark resource usage privilege all not granted
        String dbName = "db1";
        String resouceName = "test_spark";
        Assert.assertEquals(false, auth.checkDbPriv(userIdentity, dbName, PrivPredicate.SELECT));
        Assert.assertEquals(false, auth.checkDbPriv(userIdentity, dbName, PrivPredicate.LOAD));
        Assert.assertEquals(false, auth.checkResourcePriv(userIdentity, resouceName, PrivPredicate.USAGE));
        Assert.assertEquals(0, auth.getRoleNamesByUser(userIdentity).size());

        // 2. add a role with select privilege
        String selectRoleName = "select_role";
        String createRoleSql = String.format("CREATE ROLE %s", selectRoleName);
        CreateRoleStmt createRoleStmt = (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(createRoleSql, ctx);
        Assert.assertEquals(false, auth.doesRoleExist(createRoleStmt.getQualifiedRole()));
        auth.createRole(createRoleStmt);
        Assert.assertEquals(true, auth.doesRoleExist(createRoleStmt.getQualifiedRole()));

        // 3. grant select privilege to role
        GrantPrivilegeStmt grantStmt = null;
        String sql = "GRANT SELECT_PRIV ON db1.'*' TO ROLE select_role";
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        auth.grant(grantStmt);

        // 4. grant role to user
        GrantRoleStmt grantRoleStmt = new GrantRoleStmt(Collections.singletonList(selectRoleName), userIdentity);
        com.starrocks.sql.analyzer.Analyzer.analyze(grantRoleStmt, ctx);
        auth.grantRole(grantRoleStmt);

        // check if select privilege granted, load privilege not granted
        Assert.assertEquals(true, auth.checkDbPriv(userIdentity, dbName, PrivPredicate.SELECT));
        Assert.assertEquals(false, auth.checkDbPriv(userIdentity, dbName, PrivPredicate.LOAD));
        Assert.assertEquals(false, auth.checkResourcePriv(userIdentity, resouceName, PrivPredicate.USAGE));
        Assert.assertEquals(1, auth.getRoleNamesByUser(userIdentity).size());

        // 5. add a new role with load privilege & spark resource usage
        String loadRoleName = "load_role";
        createRoleSql = String.format("CREATE ROLE %s", loadRoleName);
        createRoleStmt = (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(createRoleSql, ctx);
        auth.createRole(createRoleStmt);

        // 6. grant load privilege to role
        sql = "GRANT LOAD_PRIV ON db1.* TO ROLE load_role";
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        auth.grant(grantStmt);

        // 8. grant resource to role
        sql = "GRANT USAGE_PRIV ON RESOURCE test_spark TO ROLE load_role";
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        auth.grant(grantStmt);

        // 7. grant role to user
        grantRoleStmt = new GrantRoleStmt(Collections.singletonList(loadRoleName), userIdentity);
        com.starrocks.sql.analyzer.Analyzer.analyze(grantRoleStmt, ctx);
        auth.grantRole(grantRoleStmt);

        // check if select & load privilege & spark resource usage all granted
        Assert.assertEquals(true, auth.checkDbPriv(userIdentity, dbName, PrivPredicate.SELECT));
        Assert.assertEquals(true, auth.checkDbPriv(userIdentity, dbName, PrivPredicate.LOAD));
        Assert.assertEquals(true, auth.checkResourcePriv(userIdentity, resouceName, PrivPredicate.USAGE));
        Assert.assertEquals(2, auth.getRoleNamesByUser(userIdentity).size());

        // 8. revoke load & spark resource usage from user
        RevokeRoleStmt revokeRoleStmt = new RevokeRoleStmt(Collections.singletonList(loadRoleName), userIdentity);
        com.starrocks.sql.analyzer.Analyzer.analyze(revokeRoleStmt, ctx);
        auth.revokeRole(revokeRoleStmt);

        // check if select privilege granted, load privilege not granted
        Assert.assertEquals(true, auth.checkDbPriv(userIdentity, dbName, PrivPredicate.SELECT));
        Assert.assertEquals(false, auth.checkDbPriv(userIdentity, dbName, PrivPredicate.LOAD));
        Assert.assertEquals(false, auth.checkResourcePriv(userIdentity, resouceName, PrivPredicate.USAGE));
        Assert.assertEquals(1, auth.getRoleNamesByUser(userIdentity).size());

        // 9. grant usage on db
        boolean hasException = false;
        try {
            sql = "GRANT USAGE ON db1.* TO role select_role";
            grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            auth.grant(grantStmt);
        } catch (DdlException e) {
            // expect exception;
            hasException = true;
        }
        Assert.assertTrue(hasException);
    }

    @Test
    public void testResource() {
        String role = "role0";
        String resourceName = "spark0";
        String anyResource = "*";

        // ------ grant|revoke resource to|from user ------
        // 1. create user with no role
        String createUserSql = "CREATE USER 'testUser' IDENTIFIED BY '12345'";
        CreateUserStmt createUserStmt = null;
        try {
            createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
            auth.createUser(createUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        UserIdentity userIdentity = createUserStmt.getUserIdent();


        // 2. grant usage_priv on resource 'spark0' to 'testUser'@'%'
        GrantPrivilegeStmt grantStmt = null;
        try {
            String sql = "GRANT USAGE_PRIV on resource 'spark0' to testUser";
            grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            auth.grant(grantStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertEquals(1, auth.getAuthInfo(userIdentity).size());
        Assert.assertTrue(auth.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertFalse(auth.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 3. revoke usage_priv on resource 'spark0' from 'testUser'@'%'
        RevokePrivilegeStmt revokeStmt = null;
        try {
            String sql = "REVOKE USAGE_PRIV on resource 'spark0' FROM 'testUser'";
            revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            auth.revoke(revokeStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(auth.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertFalse(auth.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 4. drop user
        String dropSql = "DROP USER 'testUser'";
        DropUserStmt dropUserStmt;
        try {
            dropUserStmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(dropSql, ctx);
            auth.dropUser(dropUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ grant|revoke resource to|from role ------
        // 1. create role
        String createRoleSql = String.format("CREATE ROLE %s", role);
        CreateRoleStmt roleStmt;
        try {
            roleStmt = (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(createRoleSql, ctx);
            auth.createRole(roleStmt);
        } catch (Exception e1) {
            e1.printStackTrace();
            Assert.fail();
        }
        // grant usage_priv on resource 'spark0' to role 'role0'
        try {
            String sql = "GRANT usage_priv on resource 'spark0' to role 'role0'";
            grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            auth.grant(grantStmt);
        } catch (Exception e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // 2. create user with role
        createUserSql = "CREATE USER 'testUser' IDENTIFIED BY '12345' DEFAULT ROLE 'role0'";
        try {
            createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
            auth.createUser(createUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(auth.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertFalse(auth.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 3. revoke usage_priv on resource 'spark0' from role 'role0'
        try {
            String sql = "REVOKE usage_priv on resource 'spark0' from role 'role0'";
            revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            auth.revoke(revokeStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        // also revoke from user with this role
        Assert.assertFalse(auth.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertFalse(auth.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 4. drop user and role
        dropSql = "DROP USER 'testUser'";
        try {
            dropUserStmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(dropSql, ctx);
            auth.dropUser(dropUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        String dropRoleSql = "DROP ROLE role0";
        DropRoleStmt dropRoleStmt;
        try {
            dropRoleStmt = (DropRoleStmt) UtFrameUtils.parseStmtWithNewParser(dropRoleSql, ctx);
            auth.dropRole(dropRoleStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ grant|revoke any resource to|from user ------
        // 1. create user with no role
        createUserSql = "CREATE USER 'testUser' IDENTIFIED BY '12345'";
        try {
            createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
            auth.createUser(createUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 2. grant usage_priv on resource '*' to 'testUser'@'%'
        try {
            String sql = "GRANT USAGE_PRIV on RESOURCE '*' TO 'testUser'@'%'";
            grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            auth.grant(grantStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        Assert.assertTrue(auth.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertTrue(auth.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 3. revoke usage_priv on resource '*' from 'testUser'@'%'
        try {
            String sql = "REVOKE USAGE_PRIV on RESOURCE '*' from 'testUser'@'%'";
            revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            auth.revoke(revokeStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(auth.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertFalse(auth.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 4. drop user
        dropSql = "DROP USER 'testUser'";
        try {
            dropUserStmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(dropSql, ctx);
            auth.dropUser(dropUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ grant|revoke any resource to|from role ------
        // 1. create role
        createRoleSql = String.format("CREATE ROLE %s", role);
        try {
            roleStmt = (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(createRoleSql, ctx);
            com.starrocks.sql.analyzer.Analyzer.analyze(createUserStmt, new ConnectContext());
            auth.createRole(roleStmt);
        } catch (Exception e1) {
            e1.printStackTrace();
            Assert.fail();
        }
        // grant usage_priv on resource '*' to role 'role0'
        try {
            String sql = "GRANT USAGE_PRIV on RESOURCE '*' TO ROLE 'role0'";
            grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            auth.grant(grantStmt);
        } catch (Exception e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // 2. create user with role
        createUserSql = "CREATE USER 'testUser' IDENTIFIED BY '12345' DEFAULT ROLE 'role0'";
        try {
            createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
            auth.createUser(createUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(auth.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertTrue(auth.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 3. revoke usage_priv on resource '*' from role 'role0'
        try {
            String sql = "REVOKE USAGE_PRIV on RESOURCE '*' from ROLE 'role0'";
            revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            auth.revoke(revokeStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        // also revoke from user with this role
        Assert.assertFalse(auth.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertFalse(auth.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 4. drop user and role
        dropSql = "DROP USER 'testUser'";
        try {
            dropUserStmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(dropSql, ctx);
            auth.dropUser(dropUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ error case ------
        boolean hasException = false;
        createUserSql = "CREATE USER 'testUser' IDENTIFIED BY '12345'";
        try {
            createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
            auth.createUser(createUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 1. grant db table priv to resource
        hasException = false;
        try {
            String sql = "GRANT SELECT_PRIV on RESOURCE 'spark0' TO 'testUser'";
            grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            auth.grant(grantStmt);
        } catch (Exception e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 2. grant resource priv to db table
        hasException = false;
        try {
            String sql = "GRANT USAGE_PRIV on 'db1.*' TO 'testUser'";
            grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            auth.grant(grantStmt);
        } catch (Exception e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        try {
            dropSql = "DROP USER 'testUser'";
            dropUserStmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(dropSql, ctx);
            auth.dropUser(dropUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ grant|revoke node_priv to|from role ------
        // 1. grant node_priv on resource '*' to role 'role0'
        try {
            String sql = "GRANT NODE_PRIV on RESOURCE '*' TO ROLE 'role0'";
            grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            auth.grant(grantStmt);
        } catch (Exception e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        createUserSql = "CREATE USER 'testUser' IDENTIFIED BY '12345' DEFAULT ROLE 'role0'";
        try {
            createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
            auth.createUser(createUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(auth.checkResourcePriv(userIdentity, anyResource, PrivPredicate.OPERATOR));
        Assert.assertTrue(auth.checkGlobalPriv(userIdentity, PrivPredicate.OPERATOR));

        // 2. revoke node_priv on resource '*' from role 'role0'
        try {
            String sql = "REVOKE NODE_PRIV on RESOURCE '*' FROM ROLE 'role0'";
            revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            auth.revoke(revokeStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(auth.checkResourcePriv(userIdentity, anyResource, PrivPredicate.OPERATOR));
        Assert.assertFalse(auth.checkGlobalPriv(userIdentity, PrivPredicate.OPERATOR));

        // ------ error case ------
        hasException = false;
        try {
            String sql = "GRANT NODE_PRIV on RESOURCE 'spark0' TO ROLE 'role0'";
            grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e1) {
            e1.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);
    }

    @Test
    public void testAuthPlugin() {
        new Expectations() {
            {
                LdapSecurity.checkPassword("uid=zhangsan,ou=company,dc=example,dc=com", "123");
                result = true;

                LdapSecurity.checkPassword("uid=zhangsan,ou=company,dc=example,dc=com", "456");
                result = false;

                LdapSecurity.checkPasswordByRoot("zhangsan", "123");
                result = true;

                LdapSecurity.checkPasswordByRoot("zhangsan", "456");
                result = false;
            }
        };

        /*
            AUTHENTICATION_LDAP_SIMPLE
         */
        // create user zhangsan identified with authentication_ldap_simple as 'uid=zhangsan,ou=company,dc=example,dc=com'
        String createUserSql =
                "create user zhangsan identified with authentication_ldap_simple as 'uid=zhangsan,ou=company,dc=example,dc=com'";
        CreateUserStmt createUserStmt = null;
        try {
            createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
            auth.createUser(createUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        List<UserIdentity> currentUser = Lists.newArrayList();
        Assert.assertTrue(auth.checkPlainPassword("zhangsan", "192.168.8.8", "123",
                currentUser));
        Assert.assertTrue(auth.checkPassword("zhangsan", "192.168.8.8",
                "123".getBytes(StandardCharsets.UTF_8), null, currentUser));
        Assert.assertFalse(
                auth.checkPlainPassword("zhangsan", "192.168.8.8", "456",
                        currentUser));
        Assert.assertFalse(auth.checkPassword("zhangsan", "192.168.8.8",
                "456".getBytes(StandardCharsets.UTF_8), null, currentUser));
        List<List<String>> authInfos = auth.getAuthenticationInfo(currentUser.get(0));
        Assert.assertEquals(1, authInfos.size());
        Assert.assertEquals("No", authInfos.get(0).get(1));
        Assert.assertEquals("AUTHENTICATION_LDAP_SIMPLE", authInfos.get(0).get(2));
        Assert.assertEquals("uid=zhangsan,ou=company,dc=example,dc=com", authInfos.get(0).get(3));

        // alter user zhangsan identified with authentication_ldap_simple
        String alterUserSql = "alter user zhangsan identified with authentication_ldap_simple";
        AlterUserStmt alterUserStmt = null;
        try {
            alterUserStmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(alterUserSql, ctx);
            auth.alterUser(alterUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser = Lists.newArrayList();
        Assert.assertTrue(auth.checkPlainPassword("zhangsan", "192.168.8.8", "123",
                currentUser));
        Assert.assertTrue(auth.checkPassword("zhangsan", "192.168.8.8",
                "123".getBytes(StandardCharsets.UTF_8), null, currentUser));
        Assert.assertFalse(
                auth.checkPlainPassword("zhangsan", "192.168.8.8", "456",
                        currentUser));
        Assert.assertFalse(auth.checkPassword("zhangsan", "192.168.8.8",
                "456".getBytes(StandardCharsets.UTF_8), null, currentUser));
        authInfos = auth.getAuthenticationInfo(currentUser.get(0));
        Assert.assertEquals(1, authInfos.size());
        Assert.assertEquals("No", authInfos.get(0).get(1));
        Assert.assertEquals("AUTHENTICATION_LDAP_SIMPLE", authInfos.get(0).get(2));
        Assert.assertEquals(FeConstants.null_string, authInfos.get(0).get(3));

        /*
            mysql_native_password
         */
        // create user lisi identified with mysql_native_password by '123456'
        createUserSql = "create user lisi identified with mysql_native_password by '123456'";
        try {
            createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
            auth.createUser(createUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        currentUser = Lists.newArrayList();
        byte[] seed = "dJSH\\]mcwKJlLH[bYunm".getBytes(StandardCharsets.UTF_8);
        byte[] scramble = MysqlPassword.scramble(seed, "123456");
        Assert.assertTrue(auth.checkPlainPassword("lisi", "192.168.8.8", "123456",
                currentUser));
        Assert.assertFalse(auth.checkPlainPassword("lisi", "192.168.8.8", "654321",
                currentUser));
        Assert.assertTrue(auth.checkPassword("lisi", "192.168.8.8", scramble, seed,
                currentUser));
        authInfos = auth.getAuthenticationInfo(currentUser.get(0));
        Assert.assertEquals(1, authInfos.size());
        Assert.assertEquals("Yes", authInfos.get(0).get(1));
        Assert.assertEquals("MYSQL_NATIVE_PASSWORD", authInfos.get(0).get(2));
        Assert.assertEquals(FeConstants.null_string, authInfos.get(0).get(3));

        // alter user lisi identified with mysql_native_password by '654321'
        String sql = "alter user lisi identified with mysql_native_password by '654321'";
        try {
            alterUserStmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
            auth.alterUser(alterUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.alterUser(alterUserStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }
        currentUser = Lists.newArrayList();
        scramble = MysqlPassword.scramble(seed, "654321");
        Assert.assertTrue(auth.checkPlainPassword("lisi", "192.168.8.8", "654321",
                currentUser));
        Assert.assertFalse(auth.checkPlainPassword("lisi", "192.168.8.8", "123456",
                currentUser));
        Assert.assertTrue(auth.checkPassword("lisi", "192.168.8.8", scramble, seed,
                currentUser));

        // alter user lisi identified with mysql_native_password as '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'
        sql = "alter user lisi identified with mysql_native_password as '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'";
        try {
            alterUserStmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
            auth.alterUser(alterUserStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.alterUser(alterUserStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }
        currentUser = Lists.newArrayList();
        scramble = MysqlPassword.scramble(seed, "123456");
        Assert.assertTrue(auth.checkPlainPassword("lisi", "192.168.8.8", "123456",
                currentUser));
        Assert.assertFalse(auth.checkPlainPassword("lisi", "192.168.8.8", "654321",
                currentUser));
        Assert.assertTrue(auth.checkPassword("lisi", "192.168.8.8", scramble, seed,
                currentUser));

        // alter user lisi identified with mysql_native_password
        sql = "alter user lisi identified with mysql_native_password";
        try {
            alterUserStmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.alterUser(alterUserStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }
        currentUser = Lists.newArrayList();
        Assert.assertTrue(
                auth.checkPlainPassword("lisi", "192.168.8.8", null, currentUser));
        Assert.assertFalse(auth.checkPlainPassword("lisi", "192.168.8.8", "123456",
                currentUser));
        Assert.assertTrue(
                auth.checkPassword("lisi", "192.168.8.8", new byte[0], seed,
                        currentUser));
    }

    @Test
    public void testPasswordReuseNormal() throws Exception {
        String password = "123456AAbb";
        // 1. create user with no role
        String createUserSql = String.format("CREATE USER 'testUser' IDENTIFIED BY '%s'", password);

        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
        // createUserStmt.analyze(analyzer);
        auth.createUser(createUserStmt);
        UserIdentity user = createUserStmt.getUserIdent();

        // enable_password_reuse is false allow same password
        Config.enable_password_reuse = true;
        auth.checkPasswordReuse(user, password);

        // enable_password_reuse is false, check different password
        Config.enable_password_reuse = false;
        auth.checkPasswordReuse(user, password + "ss");
    }

    @Test
    public void testPasswordValidationNormal() throws Exception {
        String badPassword = "123456";
        String goodPassword = "1234Star";

        Config.enable_validate_password = false;
        // enable_auth_check is false, allow bad password
        auth.validatePassword(badPassword);

        // enable_password_reuse is true for a good password
        Config.enable_validate_password = true;
        auth.validatePassword(goodPassword);
    }

    @Test(expected = DdlException.class)
    public void testPasswordValidationShortPasssword() throws Exception {
        // length 5 < 8
        String badPassword = "Aa123";
        Config.enable_validate_password = true;
        auth.validatePassword(badPassword);
    }

    @Test(expected = DdlException.class)
    public void testPasswordValidationAllNumberPasssword() throws Exception {
        // no lowercase letter or uppercase letter
        String badPassword = "123456789";
        Config.enable_validate_password = true;
        auth.validatePassword(badPassword);
    }

    @Test(expected = DdlException.class)
    public void testPasswordValidationPasswordReuse() throws Exception {
        String password = "123456AAbb";
        String createUserSql = String.format("CREATE USER 'test_user' IDENTIFIED BY '%s'", password);
        // 1. create user with no role
        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
        auth.createUser(createUserStmt);

        // 2. check reuse
        Config.enable_password_reuse = false;
        auth.checkPasswordReuse(createUserStmt.getUserIdent(), password);
    }

    private static final Logger LOG = LogManager.getLogger(AuthTest.class);

    @Test
    public void testManyUsersAndTables() throws Exception {
        int bigNumber = 500;
        int bigNumber2 = bigNumber / 2;
        int logInterval = bigNumber / 50;
        String db = "db1";
        LOG.info("before add privilege: table {} entries, user {} entries",
                auth.getTablePrivTable().size(), auth.getUserPrivTable().size());
        Assert.assertEquals(1, auth.getAuthInfo(null).size());

        // 1. create N user with select privilege to N/2 table
        // 1.1 create user
        for (int i = 0; i != bigNumber; i++) {
            String createUserSql = String.format("CREATE USER 'user_%d_of_%d' IDENTIFIED BY '12345'", i, bigNumber);
            CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
            auth.createUser(createUserStmt);
        }
        Assert.assertEquals(1 + bigNumber, auth.getAuthInfo(null).size());

        // check the last user
        String lastUserName = String.format("user_%d_of_%d", bigNumber - 1, bigNumber);
        UserIdentity lastUserIdentity = new UserIdentity(lastUserName, "%");
        lastUserIdentity.analyze();
        // information_schema
        Assert.assertEquals(1, auth.getDBPrivEntries(lastUserIdentity).size());
        int infomationSchemaTableCnt = auth.getTablePrivEntries(lastUserIdentity).size();
        Assert.assertEquals(1, auth.getAuthInfo(lastUserIdentity).size());

        // 1.2 grant N/2 table privilege
        long start = System.currentTimeMillis();
        for (int i = 0; i != bigNumber; i++) {
            if (i % logInterval == 0) {
                LOG.info("added {} user..", i);
            }
            String userName = String.format("user_%d_of_%d", i, bigNumber);
            UserIdentity userIdentity = new UserIdentity(userName, "%");
            userIdentity.analyze();
            for (int j = 0; j != bigNumber2; j++) {
                String tableName = String.format("table_%d_of_%d", j, bigNumber2);
                TablePattern tablePattern = new TablePattern("db1", tableName);
                tablePattern.analyze();
                PrivBitSet privileges = AccessPrivilege.SELECT_PRIV.toPrivilege();
                auth.grantPrivs(userIdentity, tablePattern, privileges, false);
            }
        }
        long end = System.currentTimeMillis();
        LOG.info("add privilege: {} entries, total {} ms", auth.getTablePrivTable().size(), end - start);

        start = System.currentTimeMillis();
        for (int i = 0; i != bigNumber; i++) {
            // 1.1 create user
            String userName = String.format("user_%d_of_%d", i, bigNumber);
            UserIdentity userIdentity = new UserIdentity(userName, "%");
            userIdentity.analyze();
            for (int j = 0; j != bigNumber2; j++) {
                String tableName = String.format("table_%d_of_%d", j, bigNumber2);
                Assert.assertTrue(auth.checkTblPriv(
                        userIdentity, db, tableName, PrivPredicate.SELECT));
            }
        }
        end = System.currentTimeMillis();
        LOG.info("check privilege: total {} ms", end - start);

        // check the last user
        // infomation_schema
        Assert.assertEquals(1, auth.getDBPrivEntries(lastUserIdentity).size());
        Assert.assertEquals(bigNumber / 2 + infomationSchemaTableCnt, auth.getTablePrivEntries(lastUserIdentity).size());
        Assert.assertEquals(1, auth.getAuthInfo(lastUserIdentity).size());
    }

    @Test
    public void checkDefaultRootPrivilege() throws Exception {
        Assert.assertTrue(auth.checkHasPriv(ctx, PrivPredicate.ADMIN, Auth.PrivLevel.GLOBAL));
        Assert.assertTrue(auth.checkHasPriv(ctx, PrivPredicate.GRANT, Auth.PrivLevel.GLOBAL));
        Assert.assertFalse(auth.checkHasPriv(ctx, PrivPredicate.ADMIN, Auth.PrivLevel.DATABASE));
        Assert.assertFalse(auth.checkHasPriv(ctx, PrivPredicate.ADMIN, Auth.PrivLevel.TABLE));
    }

    @Test
    public void testGetPasswordByApproximate() throws Exception {
        UserIdentity userIdentity = new UserIdentity("test_user", "%");
        userIdentity.analyze();
        UserDesc userDesc = new UserDesc(userIdentity, "12345", true);
        CreateUserStmt createUserStmt = new CreateUserStmt(false, userDesc, null);
        com.starrocks.sql.analyzer.Analyzer.analyze(createUserStmt, new ConnectContext());

        auth.createUser(createUserStmt);

        Assert.assertNull(auth.getUserPrivTable().getPasswordByApproximate(
                "unknown_user", "10.1.1.1"));
        Assert.assertNotNull(auth.getUserPrivTable().getPasswordByApproximate(
                "test_user", "10.1.1.1"));
        Assert.assertNotNull(auth.getUserPrivTable().getPasswordByApproximate(
                "test_user", "localhost"));
    }

    /**
     * TODO I think this case should in UserPrivTableTest instead of AuthTest
     *    Unfortunately the two classes are highly coupled.
     */
    @Test
    public void testMultiUserMatch() throws Exception {
        Assert.assertEquals(1, auth.getUserPrivTable().size());
        String passwordStr = "12345";

        // create four entries
        String[][] userHostPatterns = {
                {"user_1", "10.1.1.1"},
                {"user_1", "%"},
                {"user_zzz", "%"},
                {"user_zzz", "10.1.1.1"},
        };
        for (String[] userHost : userHostPatterns) {
            String createUserSql = String.format("CREATE USER '%s'@'%s' IDENTIFIED BY '%s'",
                    userHost[0], userHost[1], passwordStr);
            CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
            auth.createUser(createUserStmt);
        }
        Assert.assertEquals(5, auth.getUserPrivTable().size());

        // check if login match
        // remote_user, remote_ip, expect_user_identity
        String[][] userHostAndMatchedUserHosts = {
                // login as user_1 from 10.1.1.1, expected identified as user_1@10.1.1.1
                {"user_1", "10.1.1.1", "user_1", "10.1.1.1"},
                // login as user_1 from 10.1.1.2, expected identified as user_1@%, fuzzy matching
                {"user_1", "10.1.1.2", "user_1", "%"},
                {"user_zzz", "10.1.1.1", "user_zzz", "10.1.1.1"},
                {"user_zzz", "10.1.1.2", "user_zzz", "%"},

        };
        for (String[] userHostAndMatchedUserHost : userHostAndMatchedUserHosts) {
            List<UserIdentity> identities = new ArrayList<>();
            String remoteUser = "" + userHostAndMatchedUserHost[0];
            String remoteIp = userHostAndMatchedUserHost[1];
            String expectQualifiedUser = "" + userHostAndMatchedUserHost[2];
            String expectHost = userHostAndMatchedUserHost[3];

            auth.checkPlainPassword(remoteUser, remoteIp, passwordStr, identities);
            Assert.assertEquals(1, identities.size());
            Assert.assertEquals(expectQualifiedUser, identities.get(0).getQualifiedUser());
            Assert.assertEquals(expectHost, identities.get(0).getHost());

            identities.clear();
            byte[] seed = "dJSH\\]mcwKJlLH[bYunm".getBytes(StandardCharsets.UTF_8);
            byte[] scramble = MysqlPassword.scramble(seed, passwordStr);
            auth.checkPassword(remoteUser, remoteIp, scramble, seed, identities);
            Assert.assertEquals(1, identities.size());
            Assert.assertEquals(expectQualifiedUser, identities.get(0).getQualifiedUser());
            Assert.assertEquals(expectHost, identities.get(0).getHost());
        }

        // test iterator
        // full iterator
        Iterator<PrivEntry> iter = auth.getUserPrivTable().getFullReadOnlyIterator();
        List<String> userHostResult = new ArrayList<>();
        while (iter.hasNext()) {
            PrivEntry entry = iter.next();
            if (entry.getOrigUser() != "root") {
                userHostResult.add(String.format("%s@%s", entry.getOrigUser(), entry.getOrigHost()));
            }
        }
        Assert.assertEquals(4, userHostResult.size());
        List<String> expect = new ArrayList<>();
        for (String[] userHost : userHostPatterns) {
            expect.add(String.format("%s@%s", userHost[0], userHost[1]));
        }
        Collections.sort(expect);
        Collections.sort(userHostResult);
        Assert.assertEquals(expect, userHostResult);

        UserIdentity user = new UserIdentity("user_1", "10.1.1.1");
        user.analyze();
        iter = auth.getUserPrivTable().getReadOnlyIteratorByUser(user);
        userHostResult.clear();
        while (iter.hasNext()) {
            PrivEntry entry = iter.next();
            userHostResult.add(String.format("%s@%s", entry.getOrigUser(), entry.getOrigHost()));
        }
        // expect match 2: user_1@10.1.1.1 & user_1@%
        Assert.assertEquals(2, userHostResult.size());
        Assert.assertTrue(userHostResult.contains("user_1@10.1.1.1"));
        Assert.assertTrue(userHostResult.contains("user_1@%"));


        // test grant
        // GRANT select_priv on db1.table1 to user_1@%
        // GRANT select_priv on db1.table2 to user_1@10.1.1.2
        // see if user_1@10.1.1.1 can see two table
        // and user_1@10.1.1.2 can see one table

        // GRANT select_priv on db1.table1 to user_1@%
        TablePattern tablePattern = new TablePattern("db1", "table1");
        tablePattern.analyze();
        PrivBitSet privileges = AccessPrivilege.SELECT_PRIV.toPrivilege();
        user = new UserIdentity("user_1", "%");
        user.analyze();
        auth.grantPrivs(user, tablePattern, privileges, false);

        // GRANT select_priv on db1.table2 to user_1@10.1.1.1
        tablePattern = new TablePattern("db1", "table2");
        tablePattern.analyze();
        privileges = AccessPrivilege.SELECT_PRIV.toPrivilege();
        user = new UserIdentity("user_1", "10.1.1.1");
        user.analyze();
        auth.grantPrivs(user, tablePattern, privileges, false);

        // check if user_1@10.1.1.1 can see two table
        List<UserIdentity> identities = new ArrayList<>();
        auth.checkPlainPassword(
                "user_1", "10.1.1.1", passwordStr, identities);
        Assert.assertEquals(1, identities.size());
        user = identities.get(0);
        Assert.assertEquals("10.1.1.1", user.getHost());
        String db = "db1";
        // TODO: this is a legacy bug, I will fix it in another PR
        // Assert.assertTrue(auth.checkTblPriv(user, db, "table1", PrivPredicate.SELECT));
        Assert.assertTrue(auth.checkTblPriv(user, db, "table2", PrivPredicate.SELECT));

        // check if user_1@10.1.1.2 can see one table
        identities.clear();
        auth.checkPlainPassword(
                "user_1", "10.1.1.2", passwordStr, identities);
        Assert.assertEquals(1, identities.size());
        user = identities.get(0);
        Assert.assertEquals("%", user.getHost());
        Assert.assertTrue(auth.checkTblPriv(user, db, "table1", PrivPredicate.SELECT));
        Assert.assertFalse(auth.checkTblPriv(user, db, "table2", PrivPredicate.SELECT));
    }

    @Test
    public void testGrantRevokeImpersonate() throws Exception {
        // 1. prepare
        // 1.1create harry, gregory, albert, neville
        UserIdentity harry = new UserIdentity("Harry", "%");
        harry.analyze();
        UserIdentity gregory = new UserIdentity("Gregory", "%");
        gregory.analyze();
        UserIdentity albert = new UserIdentity("Albert", "%");
        albert.analyze();
        UserIdentity neville = new UserIdentity("Neville", "%");
        neville.analyze();
        String createUserSql = "CREATE USER '%s' IDENTIFIED BY '12345'";
        String[] userNames = {"Harry", "Gregory", "Albert", "Neville"};
        for (String userName : userNames) {
            CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils
                    .parseStmtWithNewParser(String.format(createUserSql, userName), ctx);
            auth.createUser(createUserStmt);
        }


        // 1.2 before test
        Assert.assertFalse(auth.canImpersonate(harry, gregory));
        Assert.assertFalse(auth.canImpersonate(harry, albert));

        // 2. grant impersonate on gregory to harry
        // 2.1 grant
        String sql = "grant impersonate on Gregory to Harry";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        auth.grant(grantStmt);
        // 2.2 assert
        Assert.assertTrue(auth.canImpersonate(harry, gregory));
        Assert.assertFalse(auth.canImpersonate(harry, albert));

        // 3. grant impersonate on albert to harry
        // 3.1 grant
        sql = "grant impersonate on Albert to Harry";
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        auth.grant(grantStmt);
        // 3.2 assert
        Assert.assertTrue(auth.canImpersonate(harry, gregory));
        Assert.assertTrue(auth.canImpersonate(harry, albert));

        // 4. revoke impersonate on gregory from harry
        // 4.1 revoke
        sql = "revoke impersonate on Gregory from Harry";
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        auth.revoke(revokeStmt);
        // 4.2 assert
        Assert.assertFalse(auth.canImpersonate(harry, gregory));
        Assert.assertTrue(auth.canImpersonate(harry, albert));

        // Auror usually has the ability to impersonate to others..
        // 5.1 create role
        String auror = "auror";
        String createRoleSql = String.format("CREATE ROLE %s", auror);
        CreateRoleStmt roleStmt = (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(createRoleSql, ctx);
        auth.createRole(roleStmt);
        // 5.2 grant impersonate on gregory to role auror
        sql = "grant impersonate on Gregory to role auror";
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        auth.grant(grantStmt);
        // 5.3 grant auror to neiville
        GrantRoleStmt grantRoleStmt = new GrantRoleStmt(Collections.singletonList(auror), neville);
        com.starrocks.sql.analyzer.Analyzer.analyze(grantRoleStmt, ctx);
        auth.grantRole(grantRoleStmt);
        // 5.4 assert
        Assert.assertTrue(auth.canImpersonate(neville, gregory));

        // 6. grant impersonate on albert to role auror
        // 6.1 grant
        sql = "grant impersonate on Albert to role auror";
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        auth.grant(grantStmt);
        // 6.2 assert
        Assert.assertTrue(auth.canImpersonate(neville, albert));

        // 7. revert impersonate to gregory from role auror
        // 7.1 revoke
        sql = "revoke impersonate on Gregory from role auror";
        revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        auth.revoke(revokeStmt);
        // 7.2 assert
        Assert.assertFalse(auth.canImpersonate(neville, gregory));

        // 8. revoke role from neville
        // 8.2 revoke
        RevokeRoleStmt revokeRoleStmt = new RevokeRoleStmt(Collections.singletonList(auror), neville);
        com.starrocks.sql.analyzer.Analyzer.analyze(revokeRoleStmt, ctx);
        auth.revokeRole(revokeRoleStmt);
        // 8.2 assert
        Assert.assertFalse(auth.canImpersonate(neville, albert));
    }

    @Test
    public void testShowGrants() throws Exception {
        // 1. create 3 users
        List<String> names = Arrays.asList("user1", "user2", "user3");
        List<UserIdentity> userToBeCreated = new ArrayList<>();
        for (String name : names) {
            UserIdentity userIdentity = new UserIdentity(name, "%");
            userIdentity.analyze();
            UserDesc userDesc = new UserDesc(userIdentity, "12345", true);
            CreateUserStmt createUserStmt = new CreateUserStmt(false, userDesc, null);
            com.starrocks.sql.analyzer.Analyzer.analyze(createUserStmt, new ConnectContext());
            auth.createUser(createUserStmt);
            userToBeCreated.add(userIdentity);
        }
        UserIdentity emptyPrivilegeUser = userToBeCreated.get(0);
        UserIdentity onePrivilegeUser = userToBeCreated.get(1);
        UserIdentity manyPrivilegeUser = userToBeCreated.get(2);

        // 1. emptyPrivilegeUser has one privilege
        List<List<String>> infos = auth.getGrantsSQLs(emptyPrivilegeUser);
        Assert.assertEquals(1, infos.size());
        Assert.assertEquals(2, infos.get(0).size());
        Assert.assertEquals(emptyPrivilegeUser.toString(), infos.get(0).get(0));
        Assert.assertEquals("GRANT SELECT ON information_schema.* TO 'user1'@'%'", infos.get(0).get(1));

        // 2. grant table privilege to onePrivilegeUser
        TablePattern table = new TablePattern("testdb", "table1");
        table.analyze();
        auth.grantPrivs(onePrivilegeUser, table, PrivBitSet.of(Privilege.SELECT_PRIV), false);
        infos = auth.getGrantsSQLs(onePrivilegeUser);
        Assert.assertEquals(1, infos.size());
        Assert.assertEquals(2, infos.get(0).size());
        Assert.assertEquals(onePrivilegeUser.toString(), infos.get(0).get(0));
        String expectSQL = "GRANT SELECT ON testdb.table1 TO 'user2'@'%'";
        Assert.assertTrue(infos.get(0).get(1).contains(expectSQL));

        // 3. grant resource & table & global & impersonate to manyPrivilegeUser
        List<String> expectSQLs = new ArrayList<>();
        TablePattern db = new TablePattern("testdb", "*");
        db.analyze();
        auth.grantPrivs(manyPrivilegeUser, db, PrivBitSet.of(Privilege.LOAD_PRIV, Privilege.SELECT_PRIV), false);
        expectSQLs.add("GRANT SELECT, LOAD ON testdb.* TO 'user3'@'%'");
        TablePattern global = new TablePattern("*", "*");
        global.analyze();
        auth.grantPrivs(manyPrivilegeUser, global, PrivBitSet.of(Privilege.GRANT_PRIV), false);
        expectSQLs.add("GRANT GRANT ON *.* TO 'user3'@'%'");
        ResourcePattern resourcePattern = new ResourcePattern("test_resource");
        resourcePattern.analyze();
        auth.grantPrivs(manyPrivilegeUser, resourcePattern, PrivBitSet.of(Privilege.USAGE_PRIV), false);
        expectSQLs.add("GRANT USAGE ON RESOURCE test_resource TO 'user3'@'%'");
        String sql = "GRANT IMPERSONATE ON 'user1'@'%' TO 'user3'@'%'";
        auth.grant((GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx));
        expectSQLs.add(sql);
        infos = auth.getGrantsSQLs(manyPrivilegeUser);
        Assert.assertEquals(1, infos.size());
        Assert.assertEquals(2, infos.get(0).size());
        Assert.assertEquals(manyPrivilegeUser.toString(), infos.get(0).get(0));
        for (String expect : expectSQLs) {
            Assert.assertTrue(infos.get(0).get(1).contains(expect));
        }

        // 4. check all grants
        infos = auth.getGrantsSQLs(null);
        Assert.assertEquals(4, infos.size()); // the other is root
        Set<String> nameSet = new HashSet<>();
        for (List<String> line : infos) {
            nameSet.add(line.get(0));
        }
        Assert.assertTrue(nameSet.contains(emptyPrivilegeUser.toString()));
        Assert.assertTrue(nameSet.contains(onePrivilegeUser.toString()));
        Assert.assertTrue(nameSet.contains(manyPrivilegeUser.toString()));
    }

    @Test
    public void testImpersonateReplay(@Mocked EditLog editLog) throws Exception {
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;

                globalStateMgr.isUsingNewPrivilege();
                minTimes = 0;
                result = false;
            }
        };

        List<ImpersonatePrivInfo> infos = new ArrayList<>();
        new Expectations(editLog) {
            {
                editLog.logGrantImpersonate((ImpersonatePrivInfo) any);
                minTimes = 0;
                result = new Delegate() {
                    void recordInfo(ImpersonatePrivInfo info) {
                        infos.add(info);
                    }
                };
            }

            {
                editLog.logRevokeImpersonate((ImpersonatePrivInfo) any);
                minTimes = 0;
                result = new Delegate() {
                    void recordInfo(ImpersonatePrivInfo info) {
                        infos.add(info);
                    }
                };
            }

            {
                editLog.logCreateUser((PrivInfo) any);
                minTimes = 0;
            }
        };

        // 1. prepare
        // 1.1 create harry, gregory
        UserIdentity harry = new UserIdentity("Harry", "%");
        harry.analyze();
        UserIdentity gregory = new UserIdentity("Gregory", "%");
        gregory.analyze();
        List<UserIdentity> userToBeCreated = new ArrayList<>();
        userToBeCreated.add(harry);
        userToBeCreated.add(gregory);
        for (UserIdentity userIdentity : userToBeCreated) {
            UserDesc userDesc = new UserDesc(userIdentity, "12345", true);
            CreateUserStmt createUserStmt = new CreateUserStmt(false, userDesc, null);
            com.starrocks.sql.analyzer.Analyzer.analyze(createUserStmt, new ConnectContext());
            auth.createUser(createUserStmt);
        }


        // 2. grant impersonate on gregory to harry
        // 2.1 grant
        Assert.assertFalse(auth.canImpersonate(harry, gregory));
        String sql = "grant impersonate on Gregory to Harry";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        auth.grant(grantStmt);
        // 2.2 check
        Assert.assertTrue(auth.canImpersonate(harry, gregory));

        // 3. check log grant
        Assert.assertEquals(1, infos.size());

        // 4. replay grant
        Auth newAuth = new Auth();
        Assert.assertFalse(newAuth.canImpersonate(harry, gregory));
        newAuth.replayGrantImpersonate(infos.get(0));
        Assert.assertTrue(newAuth.canImpersonate(harry, gregory));
        infos.clear();
        Assert.assertEquals(0, infos.size());

        // 5. revoke impersonate on greogory from harry
        sql = "revoke impersonate on Gregory from Harry";
        auth.revoke((RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx));
        Assert.assertFalse(auth.canImpersonate(harry, gregory));

        // 6. check log revoke
        Assert.assertEquals(1, infos.size());

        // 7. replay revoke
        newAuth.replayRevokeImpersonate(infos.get(0));
        Assert.assertFalse(newAuth.canImpersonate(harry, gregory));
    }

    @Test
    public void testUserNamePureDigit() throws Exception {
        String sql = "CREATE USER '12345' IDENTIFIED BY '12345'";
        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        auth.createUser(createUserStmt);
        Assert.assertNotNull(auth.getUserProperties("'12345'@'%'"));
    }
}
