// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/CreateUserStmtTest.java

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

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CreateUserStmtTest {

    @Before
    public void setUp() {
        ConnectContext ctx = new ConnectContext(null);
        ctx.setQualifiedUser("root");
        ctx.setRemoteIP("192.168.1.1");
        UserIdentity currentUserIdentity = new UserIdentity("root", "192.168.1.1");
        currentUserIdentity.setIsAnalyzed();
        ctx.setCurrentUserIdentity(currentUserIdentity);
        ctx.setThreadLocalInfo();
    }

    @Test
    public void testToString() throws Exception {
        String sql = "CREATE USER 'user' IDENTIFIED BY 'passwd'";
        CreateUserStmt stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("CREATE USER 'user'@'%' IDENTIFIED BY '*XXX'", stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assert.assertNull(stmt.getAuthPlugin());

        sql = "CREATE USER 'user' IDENTIFIED BY PASSWORD '*59c70da2f3e3a5bdf46b68f5c8b8f25762bccef0'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("user", stmt.getUserIdent().getQualifiedUser());
        Assert.assertEquals(
                "CREATE USER 'user'@'%' IDENTIFIED BY PASSWORD '*59c70da2f3e3a5bdf46b68f5c8b8f25762bccef0'",
                stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assert.assertNull(stmt.getAuthPlugin());

        sql = "CREATE USER 'user'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("CREATE USER 'user'@'%'", stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "");
        Assert.assertNull(stmt.getAuthPlugin());

        sql = "CREATE USER 'user' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD BY 'passwd'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("CREATE USER 'user'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD BY 'passwd'",
                stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assert.assertEquals(AuthPlugin.MYSQL_NATIVE_PASSWORD.name(), stmt.getAuthPlugin());

        sql = "CREATE USER 'user' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS '*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals(
                "CREATE USER 'user'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS '*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0'",
                stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assert.assertEquals(AuthPlugin.MYSQL_NATIVE_PASSWORD.name(), stmt.getAuthPlugin());

        sql = "CREATE USER 'user' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("CREATE USER 'user'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD",
                stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "");
        Assert.assertEquals(AuthPlugin.MYSQL_NATIVE_PASSWORD.name(), stmt.getAuthPlugin());

        sql = "CREATE USER 'user' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE AS 'uid=gengjun,ou=people,dc=example,dc=io'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals(
                "CREATE USER 'user'@'%' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE AS 'uid=gengjun,ou=people,dc=example,dc=io'",
                stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "");
        Assert.assertEquals(AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name(), stmt.getAuthPlugin());
        Assert.assertEquals("uid=gengjun,ou=people,dc=example,dc=io", stmt.getUserForAuthPlugin());

        sql = "CREATE USER 'user' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE BY 'uid=gengjun,ou=people,dc=example,dc=io'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals(
                "CREATE USER 'user'@'%' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE BY 'uid=gengjun,ou=people,dc=example,dc=io'",
                stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "");
        Assert.assertEquals(AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name(), stmt.getAuthPlugin());
        Assert.assertEquals("uid=gengjun,ou=people,dc=example,dc=io", stmt.getUserForAuthPlugin());

        sql = "CREATE USER 'user' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("CREATE USER 'user'@'%' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE",
                stmt.toString());
        Assert.assertEquals(AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name(), stmt.getAuthPlugin());
        Assert.assertNull(stmt.getUserForAuthPlugin());
    }

    @Test(expected = AnalysisException.class)
    public void testEmptyUser() throws Exception {
        String sql = "CREATE USER '' IDENTIFIED BY 'passwd'";
        UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testBadPass() throws Exception {
        String sql = "CREATE USER 'user' IDENTIFIED BY PASSWORD 'passwd'";
        UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testInvalidAuthPlugin() throws Exception {
        String sql = "CREATE USER 'user' IDENTIFIED WITH authentication_ldap_sasl";
        UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.fail("No exception throws.");
    }
}
