// This file is made available under Elastic License 2.0.
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
import com.starrocks.analysis.AlterUserStmt;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.CreateRoleStmt;
import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.DropRoleStmt;
import com.starrocks.analysis.DropUserStmt;
import com.starrocks.analysis.GrantStmt;
import com.starrocks.analysis.ResourcePattern;
import com.starrocks.analysis.RevokeStmt;
import com.starrocks.analysis.TablePattern;
import com.starrocks.analysis.UserDesc;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.AccessPrivilege;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.DomainResolver;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.security.LdapSecurity;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.PrivInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.system.SystemInfoService;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Set;

public class AuthTest {

    private Auth auth;
    @Mocked
    public Catalog catalog;
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
                analyzer.getClusterName();
                minTimes = 0;
                result = SystemInfoService.DEFAULT_CLUSTER;

                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getAuth();
                minTimes = 0;
                result = auth;

                catalog.getEditLog();
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

    @Test
    public void test() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        // 1. create cmy@%
        UserIdentity userIdentity = new UserIdentity("cmy", "%");
        UserDesc userDesc = new UserDesc(userIdentity, "12345", true);
        CreateUserStmt createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }

        // 2. check if cmy from specified ip can access
        List<UserIdentity> currentUser = Lists.newArrayList();
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":cmy", "192.168.0.1", "12345",
                currentUser));
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":cmy", "192.168.0.1",
                "123456", null));
        Assert.assertFalse(auth.checkPlainPassword("other:cmy", "192.168.0.1", "12345", null));
        Assert.assertTrue(currentUser.get(0).equals(userIdentity));

        // 3. create another user: zhangsan@"192.%"
        userIdentity = new UserIdentity("zhangsan", "192.%");
        userDesc = new UserDesc(userIdentity, "12345", true);
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }

        // 4. check if zhangsan from specified ip can access
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.168.0.1",
                "12345", null));
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "172.168.0.1",
                "12345", null));
        Assert.assertFalse(auth.checkPlainPassword("zhangsan", "192.168.0.1", "12345", null));

        // 4.1 check if we can create same user
        userIdentity = new UserIdentity("zhangsan", "192.%");
        userDesc = new UserDesc(userIdentity, "12345", true);
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
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
        userIdentity = new UserIdentity("zhangsan", "172.18.1.1");
        userDesc = new UserDesc(userIdentity, "12345", true);
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "172.18.1.1",
                "12345", null));

        // 5. create a user with domain [starrocks.domain]
        userIdentity = new UserIdentity("zhangsan", "starrocks.domain1", true);
        userDesc = new UserDesc(userIdentity, "12345", true);
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }

        // 5.1 resolve domain [starrocks.domain1]
        resolver.runAfterCatalogReady();

        // 6. check if user from resolved ip can access
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1",
                "12345", null));
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1",
                "123456", null));
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "11.1.1.1",
                "12345", null));

        // 7. add duplicated user@['starrocks.domain1']
        userIdentity = new UserIdentity("zhangsan", "starrocks.domain1", true);
        userDesc = new UserDesc(userIdentity, "12345", true);
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
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
        userIdentity = new UserIdentity("lisi", "starrocks.domain2", true);
        userDesc = new UserDesc(userIdentity, "123456", true);
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 8.1 resolve domain [starrocks.domain2]
        resolver.runAfterCatalogReady();

        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "20.1.1.1",
                "123456", null));
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "10.1.1.1",
                "123456", null));
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "20.1.1.2",
                "123455", null));

        /*
         * Now we have 4 users:
         * cmy@'%'
         * zhangsan@"192.%"
         * zhangsan@['starrocks.domain1']
         * lisi@['starrocks.domain2']
         */

        // 9. grant for cmy@'%'
        TablePattern tablePattern = new TablePattern("*", "*");
        List<AccessPrivilege> privileges = Lists.newArrayList(AccessPrivilege.CREATE_PRIV, AccessPrivilege.DROP_PRIV);
        GrantStmt grantStmt = new GrantStmt(new UserIdentity("cmy", "%"), null, tablePattern, privileges);

        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        List<UserIdentity> currentUser2 = Lists.newArrayList();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":cmy", "172.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        // check auth before grant
        Assert.assertFalse(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db1",
                PrivPredicate.CREATE));

        try {
            auth.grant(grantStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 9.1 check auth
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db1",
                PrivPredicate.CREATE));
        UserIdentity zhangsan1 =
                UserIdentity.createAnalyzedUserIdentWithIp(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan",
                        "172.1.1.1");
        Assert.assertFalse(auth.checkDbPriv(zhangsan1, SystemInfoService.DEFAULT_CLUSTER + ":db1",
                PrivPredicate.CREATE));

        // 10. grant auth for non exist user
        tablePattern = new TablePattern("*", "*");
        privileges = Lists.newArrayList(AccessPrivilege.CREATE_PRIV, AccessPrivilege.DROP_PRIV);
        grantStmt = new GrantStmt(new UserIdentity("nouser", "%"), null, tablePattern, privileges);

        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        hasException = false;
        try {
            auth.grant(grantStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 11. grant auth for user with non exist host
        tablePattern = new TablePattern("*", "*");
        privileges = Lists.newArrayList(AccessPrivilege.SELECT_PRIV, AccessPrivilege.DROP_PRIV);
        grantStmt = new GrantStmt(new UserIdentity("zhangsan", "%"), null, tablePattern, privileges);

        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        hasException = false;
        try {
            auth.grant(grantStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 12. grant db auth to exist user
        tablePattern = new TablePattern("db1", "*");
        privileges = Lists.newArrayList(AccessPrivilege.SELECT_PRIV, AccessPrivilege.DROP_PRIV);
        grantStmt = new GrantStmt(new UserIdentity("zhangsan", "192.%"), null, tablePattern, privileges);

        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e) {
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
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.168.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());

        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db1",
                PrivPredicate.SELECT));
        Assert.assertFalse(auth.checkGlobalPriv(currentUser2.get(0), PrivPredicate.SELECT));
        Assert.assertTrue(auth.checkTblPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db1",
                "tbl1", PrivPredicate.SELECT));

        // 13. grant tbl auth to exist user
        tablePattern = new TablePattern("db2", "tbl2");
        privileges = Lists.newArrayList(AccessPrivilege.ALTER_PRIV, AccessPrivilege.DROP_PRIV);
        grantStmt = new GrantStmt(new UserIdentity("zhangsan", "192.%"), null, tablePattern, privileges);

        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e) {
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
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.168.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertFalse(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db2",
                PrivPredicate.SELECT));
        Assert.assertFalse(auth.checkGlobalPriv(currentUser2.get(0), PrivPredicate.SELECT));
        Assert.assertTrue(auth.checkTblPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db2", "tbl2",
                PrivPredicate.DROP));

        // 14. grant db auth to zhangsan@['starrocks.domain1']
        tablePattern = new TablePattern("db3", "*");
        privileges = Lists.newArrayList(AccessPrivilege.ALTER_PRIV, AccessPrivilege.DROP_PRIV);
        grantStmt =
                new GrantStmt(new UserIdentity("zhangsan", "starrocks.domain1", true), null, tablePattern, privileges);

        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e) {
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
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db3",
                PrivPredicate.ALTER));
        // 15. grant new auth to exist priv entry (exist ALTER/DROP, add SELECT)
        tablePattern = new TablePattern("db3", "*");
        privileges = Lists.newArrayList(AccessPrivilege.SELECT_PRIV);
        grantStmt =
                new GrantStmt(new UserIdentity("zhangsan", "starrocks.domain1", true), null, tablePattern, privileges);

        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e) {
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
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db3",
                PrivPredicate.SELECT));

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.2", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db3",
                PrivPredicate.ALTER));
        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.3", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db3",
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
        tablePattern = new TablePattern("*", "*");
        privileges = Lists.newArrayList(AccessPrivilege.SELECT_PRIV);
        RevokeStmt revokeStmt = new RevokeStmt(new UserIdentity("nouser", "%"), null, tablePattern, privileges);

        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        hasException = false;
        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 17. revoke privs from non exist host
        tablePattern = new TablePattern("*", "*");
        privileges = Lists.newArrayList(AccessPrivilege.SELECT_PRIV);
        revokeStmt = new RevokeStmt(new UserIdentity("cmy", "172.%"), null, tablePattern, privileges);

        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        hasException = false;
        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 18. revoke privs from non exist db
        tablePattern = new TablePattern("nodb", "*");
        privileges = Lists.newArrayList(AccessPrivilege.SELECT_PRIV);
        revokeStmt = new RevokeStmt(new UserIdentity("cmy", "%"), null, tablePattern, privileges);

        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        hasException = false;
        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 19. revoke privs from user @ ip
        tablePattern = new TablePattern("*", "*");
        privileges = Lists.newArrayList(AccessPrivilege.CREATE_PRIV);
        revokeStmt = new RevokeStmt(new UserIdentity("cmy", "%"), null, tablePattern, privileges);

        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":cmy", "172.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db",
                PrivPredicate.CREATE));
        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db",
                PrivPredicate.CREATE));
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db",
                PrivPredicate.DROP));

        // 19. revoke tbl privs from user @ ip
        tablePattern = new TablePattern("db2", "tbl2");
        privileges = Lists.newArrayList(AccessPrivilege.ALTER_PRIV);
        revokeStmt = new RevokeStmt(new UserIdentity("zhangsan", "192.%"), null, tablePattern, privileges);

        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkTblPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db2",
                "tbl2", PrivPredicate.ALTER));
        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertFalse(auth.checkTblPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db2",
                "tbl2", PrivPredicate.ALTER));
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db1",
                PrivPredicate.SELECT));

        // 20. revoke privs from non exist user @ domain
        tablePattern = new TablePattern("db2", "tbl2");
        privileges = Lists.newArrayList(AccessPrivilege.ALTER_PRIV);
        revokeStmt = new RevokeStmt(new UserIdentity("zhangsan", "nodomain", true), null, tablePattern, privileges);

        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        hasException = false;
        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 21. revoke privs from non exist db from user @ domain
        tablePattern = new TablePattern("nodb", "*");
        privileges = Lists.newArrayList(AccessPrivilege.ALTER_PRIV);
        revokeStmt =
                new RevokeStmt(new UserIdentity("zhangsan", "starrocks.domain1", true), null, tablePattern, privileges);

        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        hasException = false;
        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 22. revoke privs from exist user @ domain
        tablePattern = new TablePattern("db3", "*");
        privileges = Lists.newArrayList(AccessPrivilege.DROP_PRIV);
        revokeStmt =
                new RevokeStmt(new UserIdentity("zhangsan", "starrocks.domain1", true), null, tablePattern, privileges);

        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db3",
                PrivPredicate.DROP));

        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertFalse(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db3",
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
        CreateRoleStmt roleStmt = new CreateRoleStmt(Role.ADMIN_ROLE);
        hasException = false;
        try {
            roleStmt.analyze(analyzer);
        } catch (UserException e1) {
            e1.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 23. create operator role, which is not allowed
        roleStmt = new CreateRoleStmt(Role.OPERATOR_ROLE);
        hasException = false;
        try {
            roleStmt.analyze(analyzer);
        } catch (UserException e1) {
            e1.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 24. create role
        roleStmt = new CreateRoleStmt("role1");
        try {
            roleStmt.analyze(analyzer);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        try {
            auth.createRole(roleStmt);
        } catch (DdlException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // 25. grant auth to non exist role, will create this new role
        privileges = Lists.newArrayList(AccessPrivilege.DROP_PRIV, AccessPrivilege.SELECT_PRIV);
        grantStmt = new GrantStmt(null, "role2", new TablePattern("*", "*"), privileges);
        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        try {
            auth.grant(grantStmt);
        } catch (DdlException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // 26. grant auth to role
        privileges = Lists.newArrayList(AccessPrivilege.DROP_PRIV, AccessPrivilege.SELECT_PRIV);
        grantStmt = new GrantStmt(null, "role1", new TablePattern("*", "*"), privileges);
        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e1) {
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
        userIdentity = new UserIdentity("wangwu", "%");
        userDesc = new UserDesc(userIdentity, "12345", true);
        createUserStmt = new CreateUserStmt(false, userDesc, "role1");
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":wangwu", "10.17.2.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db4",
                PrivPredicate.DROP));

        // 28. create user@domain and set it as role1
        userIdentity = new UserIdentity("chenliu", "starrocks.domain2", true);
        userDesc = new UserDesc(userIdentity, "12345", true);
        createUserStmt = new CreateUserStmt(false, userDesc, "role1");
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":chenliu", "20.1.1.1", "12345", currentUser2);
        Assert.assertEquals(0, currentUser2.size());
        resolver.runAfterCatalogReady();
        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":chenliu", "20.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db4",
                PrivPredicate.DROP));

        // 29. revoke auth on non exist db from role1
        privileges = Lists.newArrayList(AccessPrivilege.DROP_PRIV);
        revokeStmt = new RevokeStmt(null, "role1", new TablePattern("nodb", "*"), privileges);
        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
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
        privileges = Lists.newArrayList(AccessPrivilege.DROP_PRIV);
        revokeStmt = new RevokeStmt(null, "role1", new TablePattern("*", "*"), privileges);
        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
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
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":chenliu", "20.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertFalse(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db4",
                PrivPredicate.DROP));

        // 31. drop role, privs remain unchanged
        DropRoleStmt dropRoleStmt = new DropRoleStmt("role1");
        try {
            dropRoleStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.dropRole(dropRoleStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }
        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":chenliu", "20.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertFalse(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db4",
                PrivPredicate.DROP));

        // 32. drop user cmy@"%"
        DropUserStmt dropUserStmt = new DropUserStmt(new UserIdentity("cmy", "%"));
        try {
            dropUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.dropUser(dropUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }

        Assert.assertFalse(
                auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":cmy", "192.168.0.1", "12345", null));
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.168.0.1",
                "12345", null));

        // 33. drop user zhangsan@"192.%"
        dropUserStmt = new DropUserStmt(new UserIdentity("zhangsan", "192.%"));
        try {
            dropUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(
                auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.168.0.1", "12345", null));

        try {
            auth.dropUser(dropUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
        Assert.assertFalse(
                auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.168.0.1", "12345", null));
        Assert.assertTrue(
                auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1", "12345", null));

        // 34. create user zhangsan@'10.1.1.1' to overwrite one of zhangsan@['starrocks.domain1']
        userIdentity = new UserIdentity("zhangsan", "10.1.1.1");
        userDesc = new UserDesc(userIdentity, "abcde", true);
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        Assert.assertTrue(
                auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1", "12345", null));

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(
                auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1", "12345", null));
        Assert.assertTrue(
                auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1", "abcde", null));

        // 35. drop user zhangsan@['starrocks.domain1']
        dropUserStmt = new DropUserStmt(new UserIdentity("zhangsan", "starrocks.domain1", true));
        try {
            dropUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(
                auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.2", "12345", null));

        try {
            auth.dropUser(dropUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
        Assert.assertTrue(
                auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.2", "12345", null));

        resolver.runAfterCatalogReady();
        Assert.assertFalse(
                auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.2", "12345", null));
        Assert.assertTrue(
                auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1", "abcde", null));

        // 36. drop user lisi@['starrocks.domain1']
        dropUserStmt = new DropUserStmt(new UserIdentity("lisi", "starrocks.domain2", true));
        try {
            dropUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(
                auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "20.1.1.1", "123456", null));
        Assert.assertFalse(
                auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "10.1.1.1", "123456", null));

        try {
            auth.dropUser(dropUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
        Assert.assertTrue(
                auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "20.1.1.1", "123456", null));
        Assert.assertFalse(
                auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "10.1.1.1", "123456", null));

        resolver.runAfterCatalogReady();
        Assert.assertFalse(
                auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "20.1.1.1", "123456", null));
        Assert.assertFalse(
                auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "10.1.1.1", "123456", null));

        // 37. drop zhangsan@'172.18.1.1' and zhangsan@'10.1.1.1'
        dropUserStmt = new DropUserStmt(new UserIdentity("zhangsan", "172.18.1.1", false));
        try {
            dropUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.dropUser(dropUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }

        dropUserStmt = new DropUserStmt(new UserIdentity("zhangsan", "10.1.1.1", false));
        try {
            dropUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.dropUser(dropUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
        Assert.assertFalse(
                auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1", "abcde", null));

        // 38.1 grant node_priv to user
        privileges = Lists.newArrayList(AccessPrivilege.NODE_PRIV);
        userIdentity = new UserIdentity("zhaoliu", "%");
        userDesc = new UserDesc(userIdentity, "12345", true);
        tablePattern = new TablePattern("*", "*");

        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }

        grantStmt = new GrantStmt(userIdentity, null, tablePattern, privileges);
        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e1) {
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
        revokeStmt = new RevokeStmt(userIdentity, null, tablePattern, privileges);
        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhaoliu", "", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkGlobalPriv(currentUser2.get(0), PrivPredicate.OPERATOR));

        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(auth.checkGlobalPriv(currentUser2.get(0), PrivPredicate.OPERATOR));


        // 38.3 grant node_priv to role
        grantStmt = new GrantStmt(null, "role3", tablePattern, privileges);
        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e1) {
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
        userDesc = new UserDesc(new UserIdentity("sunqi", "%"), "12345", true);
        createUserStmt = new CreateUserStmt(false, userDesc, "role3");
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":sunqi", "", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkGlobalPriv(currentUser2.get(0), PrivPredicate.OPERATOR));

        revokeStmt = new RevokeStmt(null, "role3", tablePattern, privileges);
        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
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
    public void testResource() {
        UserIdentity userIdentity = new UserIdentity("testUser", "%");
        String role = "role0";
        String resourceName = "spark0";
        ResourcePattern resourcePattern = new ResourcePattern(resourceName);
        String anyResource = "*";
        ResourcePattern anyResourcePattern = new ResourcePattern(anyResource);
        List<AccessPrivilege> usagePrivileges = Lists.newArrayList(AccessPrivilege.USAGE_PRIV);
        UserDesc userDesc = new UserDesc(userIdentity, "12345", true);

        // ------ grant|revoke resource to|from user ------
        // 1. create user with no role
        CreateUserStmt createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 2. grant usage_priv on resource 'spark0' to 'testUser'@'%'
        GrantStmt grantStmt = new GrantStmt(userIdentity, null, resourcePattern, usagePrivileges);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(auth.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertFalse(auth.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 3. revoke usage_priv on resource 'spark0' from 'testUser'@'%'
        RevokeStmt revokeStmt = new RevokeStmt(userIdentity, null, resourcePattern, usagePrivileges);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(auth.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertFalse(auth.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 4. drop user
        DropUserStmt dropUserStmt = new DropUserStmt(userIdentity);
        try {
            dropUserStmt.analyze(analyzer);
            auth.dropUser(dropUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ grant|revoke resource to|from role ------
        // 1. create role
        CreateRoleStmt roleStmt = new CreateRoleStmt(role);
        try {
            roleStmt.analyze(analyzer);
            auth.createRole(roleStmt);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }
        // grant usage_priv on resource 'spark0' to role 'role0'
        grantStmt = new GrantStmt(null, role, resourcePattern, usagePrivileges);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // 2. create user with role
        createUserStmt = new CreateUserStmt(false, userDesc, role);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(auth.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertFalse(auth.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 3. revoke usage_priv on resource 'spark0' from role 'role0'
        revokeStmt = new RevokeStmt(null, role, resourcePattern, usagePrivileges);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        // also revoke from user with this role
        Assert.assertFalse(auth.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertFalse(auth.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 4. drop user and role
        dropUserStmt = new DropUserStmt(userIdentity);
        try {
            dropUserStmt.analyze(analyzer);
            auth.dropUser(dropUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        DropRoleStmt dropRoleStmt = new DropRoleStmt(role);
        try {
            dropRoleStmt.analyze(analyzer);
            auth.dropRole(dropRoleStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ grant|revoke any resource to|from user ------
        // 1. create user with no role
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 2. grant usage_priv on resource '*' to 'testUser'@'%'
        grantStmt = new GrantStmt(userIdentity, null, anyResourcePattern, usagePrivileges);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(auth.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertTrue(auth.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 3. revoke usage_priv on resource '*' from 'testUser'@'%'
        revokeStmt = new RevokeStmt(userIdentity, null, anyResourcePattern, usagePrivileges);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(auth.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertFalse(auth.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 4. drop user
        dropUserStmt = new DropUserStmt(userIdentity);
        try {
            dropUserStmt.analyze(analyzer);
            auth.dropUser(dropUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ grant|revoke any resource to|from role ------
        // 1. create role
        roleStmt = new CreateRoleStmt(role);
        try {
            roleStmt.analyze(analyzer);
            auth.createRole(roleStmt);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }
        // grant usage_priv on resource '*' to role 'role0'
        grantStmt = new GrantStmt(null, role, anyResourcePattern, usagePrivileges);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // 2. create user with role
        createUserStmt = new CreateUserStmt(false, userDesc, role);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(auth.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertTrue(auth.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 3. revoke usage_priv on resource '*' from role 'role0'
        revokeStmt = new RevokeStmt(null, role, anyResourcePattern, usagePrivileges);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        // also revoke from user with this role
        Assert.assertFalse(auth.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertFalse(auth.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 4. drop user and role
        dropUserStmt = new DropUserStmt(userIdentity);
        try {
            dropUserStmt.analyze(analyzer);
            auth.dropUser(dropUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        dropRoleStmt = new DropRoleStmt(role);
        try {
            dropRoleStmt.analyze(analyzer);
            auth.dropRole(dropRoleStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ error case ------
        boolean hasException = false;
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 1. grant db table priv to resource
        List<AccessPrivilege> privileges = Lists.newArrayList(AccessPrivilege.SELECT_PRIV);
        grantStmt = new GrantStmt(userIdentity, null, resourcePattern, privileges);
        hasException = false;
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 2. grant resource priv to db table
        TablePattern tablePattern = new TablePattern("db1", "*");
        grantStmt = new GrantStmt(userIdentity, null, tablePattern, usagePrivileges);
        hasException = false;
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        dropUserStmt = new DropUserStmt(userIdentity);
        try {
            dropUserStmt.analyze(analyzer);
            auth.dropUser(dropUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ grant|revoke node_priv to|from role ------
        // 1. grant node_priv on resource '*' to role 'role0'
        List<AccessPrivilege> nodePrivileges = Lists.newArrayList(AccessPrivilege.NODE_PRIV);
        grantStmt = new GrantStmt(null, role, anyResourcePattern, nodePrivileges);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        createUserStmt = new CreateUserStmt(false, userDesc, role);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(auth.checkResourcePriv(userIdentity, anyResource, PrivPredicate.OPERATOR));
        Assert.assertTrue(auth.checkGlobalPriv(userIdentity, PrivPredicate.OPERATOR));

        // 2. revoke node_priv on resource '*' from role 'role0'
        revokeStmt = new RevokeStmt(null, role, anyResourcePattern, nodePrivileges);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(auth.checkResourcePriv(userIdentity, anyResource, PrivPredicate.OPERATOR));
        Assert.assertFalse(auth.checkGlobalPriv(userIdentity, PrivPredicate.OPERATOR));

        // ------ error case ------
        hasException = false;
        grantStmt = new GrantStmt(null, role, resourcePattern, nodePrivileges);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e1) {
            e1.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);
    }

    @Test
    public void testAuthPlugin() throws UnsupportedEncodingException {
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
        UserIdentity userIdentity = new UserIdentity("zhangsan", "%");
        UserDesc userDesc = new UserDesc(userIdentity, AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name(),
                "uid=zhangsan,ou=company,dc=example,dc=com", true);
        CreateUserStmt createUserStmt = new CreateUserStmt(userDesc);
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }

        List<UserIdentity> currentUser = Lists.newArrayList();
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.168.8.8", "123",
                currentUser));
        Assert.assertTrue(auth.checkPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.168.8.8",
                "123".getBytes("utf-8"), null, currentUser));
        Assert.assertFalse(
                auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.168.8.8", "456",
                        currentUser));
        Assert.assertFalse(auth.checkPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.168.8.8",
                "456".getBytes("utf-8"), null, currentUser));

        // alter user zhangsan identified with authentication_ldap_simple
        userDesc = new UserDesc(userIdentity, AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name(), null, true);
        AlterUserStmt alterUserStmt = new AlterUserStmt(userDesc);
        try {
            alterUserStmt.analyze(analyzer);
        } catch (UserException e) {
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
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.168.8.8", "123",
                currentUser));
        Assert.assertTrue(auth.checkPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.168.8.8",
                "123".getBytes("utf-8"), null, currentUser));
        Assert.assertFalse(
                auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.168.8.8", "456",
                        currentUser));
        Assert.assertFalse(auth.checkPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.168.8.8",
                "456".getBytes("utf-8"), null, currentUser));

        /*
            mysql_native_password
         */
        // create user lisi identified with mysql_native_password by '123456'
        userIdentity = new UserIdentity("lisi", "%");
        userDesc = new UserDesc(userIdentity, AuthPlugin.MYSQL_NATIVE_PASSWORD.name(), "123456", true);
        createUserStmt = new CreateUserStmt(userDesc);
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }
        currentUser = Lists.newArrayList();
        byte[] seed = "dJSH\\]mcwKJlLH[bYunm".getBytes("utf-8");
        byte[] scramble = MysqlPassword.scramble(seed, "123456");
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "192.168.8.8", "123456",
                currentUser));
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "192.168.8.8", "654321",
                currentUser));
        Assert.assertTrue(auth.checkPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "192.168.8.8", scramble, seed,
                currentUser));

        // alter user lisi identified with mysql_native_password by '654321'
        userDesc = new UserDesc(userIdentity, AuthPlugin.MYSQL_NATIVE_PASSWORD.name(), "654321", true);
        alterUserStmt = new AlterUserStmt(userDesc);
        try {
            alterUserStmt.analyze(analyzer);
        } catch (UserException e) {
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
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "192.168.8.8", "654321",
                currentUser));
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "192.168.8.8", "123456",
                currentUser));
        Assert.assertTrue(auth.checkPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "192.168.8.8", scramble, seed,
                currentUser));

        // alter user lisi identified with mysql_native_password as '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'
        userDesc = new UserDesc(userIdentity, AuthPlugin.MYSQL_NATIVE_PASSWORD.name(),
                "*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9", false);
        alterUserStmt = new AlterUserStmt(userDesc);
        try {
            alterUserStmt.analyze(analyzer);
        } catch (UserException e) {
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
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "192.168.8.8", "123456",
                currentUser));
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "192.168.8.8", "654321",
                currentUser));
        Assert.assertTrue(auth.checkPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "192.168.8.8", scramble, seed,
                currentUser));

        // alter user lisi identified with mysql_native_password
        userDesc = new UserDesc(userIdentity, AuthPlugin.MYSQL_NATIVE_PASSWORD.name(), null, false);
        alterUserStmt = new AlterUserStmt(userDesc);
        try {
            alterUserStmt.analyze(analyzer);
        } catch (UserException e) {
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
                auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "192.168.8.8", null, currentUser));
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "192.168.8.8", "123456",
                currentUser));
        Assert.assertTrue(
                auth.checkPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "192.168.8.8", new byte[0], seed,
                        currentUser));
    }
}
