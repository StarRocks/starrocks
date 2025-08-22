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

import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorReportException;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class CreateUserStmtTest {

    @BeforeEach
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
        Assertions.assertEquals("CREATE USER 'user'@'%' IDENTIFIED BY '*XXX'", AstToSQLBuilder.toSQL(stmt));
        Assertions.assertEquals(new String(new UserAuthenticationInfo(stmt.getUser(), stmt.getAuthOption()).getPassword(),
                        StandardCharsets.UTF_8),
                "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assertions.assertNull(stmt.getAuthOption().getAuthPlugin());

        sql = "CREATE USER 'user' IDENTIFIED BY PASSWORD '*59c70da2f3e3a5bdf46b68f5c8b8f25762bccef0'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assertions.assertEquals("user", stmt.getUser().getUser());
        Assertions.assertEquals(
                "CREATE USER 'user'@'%' IDENTIFIED BY PASSWORD '*59c70da2f3e3a5bdf46b68f5c8b8f25762bccef0'",
                AstToSQLBuilder.toSQL(stmt));
        Assertions.assertEquals(new String(new UserAuthenticationInfo(stmt.getUser(), stmt.getAuthOption()).getPassword(),
                StandardCharsets.UTF_8), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assertions.assertNull(stmt.getAuthOption().getAuthPlugin());

        sql = "CREATE USER 'user'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assertions.assertEquals("CREATE USER 'user'@'%'", AstToSQLBuilder.toSQL(stmt));
        Assertions.assertEquals(new String(new UserAuthenticationInfo(stmt.getUser(), stmt.getAuthOption()).getPassword(),
                StandardCharsets.UTF_8), "");
        Assertions.assertNull(stmt.getAuthOption());

        sql = "CREATE USER 'user' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD BY 'passwd'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assertions.assertEquals("CREATE USER 'user'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD BY 'passwd'",
                AstToSQLBuilder.toSQL(stmt));
        Assertions.assertEquals(new String(new UserAuthenticationInfo(stmt.getUser(), stmt.getAuthOption()).getPassword(),
                StandardCharsets.UTF_8), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assertions.assertEquals(AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.name(), stmt.getAuthOption().getAuthPlugin());

        sql = "CREATE USER 'user' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS '*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assertions.assertEquals(
                "CREATE USER 'user'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS '*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0'",
                AstToSQLBuilder.toSQL(stmt));
        Assertions.assertEquals(new String(new UserAuthenticationInfo(stmt.getUser(), stmt.getAuthOption()).getPassword(),
                StandardCharsets.UTF_8), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assertions.assertEquals(AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.name(), stmt.getAuthOption().getAuthPlugin());

        sql = "CREATE USER 'user' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assertions.assertEquals("CREATE USER 'user'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD",
                AstToSQLBuilder.toSQL(stmt));
        Assertions.assertEquals(new String(new UserAuthenticationInfo(stmt.getUser(), stmt.getAuthOption()).getPassword(),
                StandardCharsets.UTF_8), "");
        Assertions.assertEquals(AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.name(), stmt.getAuthOption().getAuthPlugin());

        sql = "CREATE USER 'user' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE AS 'uid=gengjun,ou=people,dc=example,dc=io'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assertions.assertEquals(
                "CREATE USER 'user'@'%' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE AS 'uid=gengjun,ou=people,dc=example,dc=io'",
                AstToSQLBuilder.toSQL(stmt));
        Assertions.assertEquals(new String(new UserAuthenticationInfo(stmt.getUser(), stmt.getAuthOption()).getPassword(),
                StandardCharsets.UTF_8), "");
        Assertions.assertEquals(AuthPlugin.Server.AUTHENTICATION_LDAP_SIMPLE.name(), stmt.getAuthOption().getAuthPlugin());
        Assertions.assertEquals("uid=gengjun,ou=people,dc=example,dc=io", new UserAuthenticationInfo(stmt.getUser(),
                stmt.getAuthOption()).getAuthString());

        sql = "CREATE USER 'user' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE BY 'uid=gengjun,ou=people,dc=example,dc=io'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assertions.assertEquals(
                "CREATE USER 'user'@'%' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE BY 'uid=gengjun,ou=people,dc=example,dc=io'",
                AstToSQLBuilder.toSQL(stmt));
        Assertions.assertEquals(new String(new UserAuthenticationInfo(stmt.getUser(), stmt.getAuthOption()).getPassword(),
                StandardCharsets.UTF_8), "");
        Assertions.assertEquals(AuthPlugin.Server.AUTHENTICATION_LDAP_SIMPLE.name(), stmt.getAuthOption().getAuthPlugin());
        Assertions.assertEquals("uid=gengjun,ou=people,dc=example,dc=io", new UserAuthenticationInfo(stmt.getUser(),
                stmt.getAuthOption()).getAuthString());

        sql = "CREATE USER 'user' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assertions.assertEquals("CREATE USER 'user'@'%' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE",
                AstToSQLBuilder.toSQL(stmt));
        Assertions.assertEquals(AuthPlugin.Server.AUTHENTICATION_LDAP_SIMPLE.name(), stmt.getAuthOption().getAuthPlugin());
        Assertions.assertNull(new UserAuthenticationInfo(stmt.getUser(), stmt.getAuthOption()).getAuthString());
    }

    @Test
    public void testEmptyUser() {
        assertThrows(AnalysisException.class, () -> {
            String sql = "CREATE USER '' IDENTIFIED BY 'passwd'";
            UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testBadPass() {
        assertThrows(ErrorReportException.class, () -> {
            String sql = "CREATE USER 'user' IDENTIFIED BY PASSWORD 'passwd'";
            UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testInvalidAuthPlugin() {
        assertThrows(AnalysisException.class, () -> {
            String sql = "CREATE USER 'user' IDENTIFIED WITH authentication_ldap_sasl";
            UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
            Assertions.fail("No exception throws.");
        });
    }
}
