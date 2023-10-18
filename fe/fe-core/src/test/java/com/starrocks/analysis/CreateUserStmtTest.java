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
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class CreateUserStmtTest {

    @Before
    public void setUp() {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setQualifiedUser("root");
        ctx.setRemoteIP("192.168.1.1");
        UserIdentity currentUserIdentity = new UserIdentity("root", "192.168.1.1");
        ctx.setCurrentUserIdentity(currentUserIdentity);
        ctx.setThreadLocalInfo();
    }

    @Test
    public void testToString() throws Exception {
        String sql = "CREATE USER 'user' IDENTIFIED BY 'passwd'";
        CreateUserStmt stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("CREATE USER 'user'@'%' IDENTIFIED BY '*XXX'", AstToSQLBuilder.toSQL(stmt));
        Assert.assertEquals(new String(stmt.getAuthenticationInfo().getPassword(), StandardCharsets.UTF_8),
                "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assert.assertNull(stmt.getAuthPluginName());

        sql = "CREATE USER 'user' IDENTIFIED BY PASSWORD '*59c70da2f3e3a5bdf46b68f5c8b8f25762bccef0'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("user", stmt.getUserIdentity().getUser());
        Assert.assertEquals(
                "CREATE USER 'user'@'%' IDENTIFIED BY PASSWORD '*59c70da2f3e3a5bdf46b68f5c8b8f25762bccef0'",
                AstToSQLBuilder.toSQL(stmt));
        Assert.assertEquals(new String(stmt.getAuthenticationInfo().getPassword(), StandardCharsets.UTF_8), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assert.assertNull(stmt.getAuthPluginName());

        sql = "CREATE USER 'user'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("CREATE USER 'user'@'%'", AstToSQLBuilder.toSQL(stmt));
        Assert.assertEquals(new String(stmt.getAuthenticationInfo().getPassword(), StandardCharsets.UTF_8), "");
        Assert.assertNull(stmt.getAuthPluginName());

        sql = "CREATE USER 'user' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD BY 'passwd'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("CREATE USER 'user'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD BY 'passwd'",
                AstToSQLBuilder.toSQL(stmt));
        Assert.assertEquals(new String(stmt.getAuthenticationInfo().getPassword(), StandardCharsets.UTF_8), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assert.assertEquals(AuthPlugin.MYSQL_NATIVE_PASSWORD.name(), stmt.getAuthPluginName());

        sql = "CREATE USER 'user' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS '*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals(
                "CREATE USER 'user'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS '*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0'",
                AstToSQLBuilder.toSQL(stmt));
        Assert.assertEquals(new String(stmt.getAuthenticationInfo().getPassword(), StandardCharsets.UTF_8), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assert.assertEquals(AuthPlugin.MYSQL_NATIVE_PASSWORD.name(), stmt.getAuthPluginName());

        sql = "CREATE USER 'user' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("CREATE USER 'user'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD",
                AstToSQLBuilder.toSQL(stmt));
        Assert.assertEquals(new String(stmt.getAuthenticationInfo().getPassword(), StandardCharsets.UTF_8), "");
        Assert.assertEquals(AuthPlugin.MYSQL_NATIVE_PASSWORD.name(), stmt.getAuthPluginName());

        sql = "CREATE USER 'user' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE AS 'uid=gengjun,ou=people,dc=example,dc=io'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals(
                "CREATE USER 'user'@'%' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE AS 'uid=gengjun,ou=people,dc=example,dc=io'",
                AstToSQLBuilder.toSQL(stmt));
        Assert.assertEquals(new String(stmt.getAuthenticationInfo().getPassword(), StandardCharsets.UTF_8), "");
        Assert.assertEquals(AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name(), stmt.getAuthPluginName());
        Assert.assertEquals("uid=gengjun,ou=people,dc=example,dc=io", stmt.getAuthenticationInfo().getTextForAuthPlugin());

        sql = "CREATE USER 'user' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE BY 'uid=gengjun,ou=people,dc=example,dc=io'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals(
                "CREATE USER 'user'@'%' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE BY 'uid=gengjun,ou=people,dc=example,dc=io'",
                AstToSQLBuilder.toSQL(stmt));
        Assert.assertEquals(new String(stmt.getAuthenticationInfo().getPassword(), StandardCharsets.UTF_8), "");
        Assert.assertEquals(AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name(), stmt.getAuthPluginName());
        Assert.assertEquals("uid=gengjun,ou=people,dc=example,dc=io",  stmt.getAuthenticationInfo().getTextForAuthPlugin());

        sql = "CREATE USER 'user' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("CREATE USER 'user'@'%' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE",
                AstToSQLBuilder.toSQL(stmt));
        Assert.assertEquals(AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name(), stmt.getAuthPluginName());
        Assert.assertNull( stmt.getAuthenticationInfo().getTextForAuthPlugin());
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
