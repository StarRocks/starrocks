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

package com.starrocks.analysis;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorReportException;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class AlterUserStmtTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        starRocksAssert = new StarRocksAssert(UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT));
        String createUserSql = "CREATE USER 'user' IDENTIFIED BY ''";
        CreateUserStmt createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());

        AuthenticationMgr authenticationManager =
                starRocksAssert.getCtx().getGlobalStateMgr().getAuthenticationMgr();
        authenticationManager.createUser(createUserStmt);
    }

    @Test
    public void testToString() throws Exception {
        String sql = "ALTER USER 'user' IDENTIFIED BY 'passwd'";
        AlterUserStmt stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assertions.assertEquals("ALTER USER 'user'@'%' IDENTIFIED BY '*XXX'",
                AstToSQLBuilder.toSQL(stmt));

        Assertions.assertEquals(new String(new UserAuthenticationInfo(stmt.getUser(), stmt.getAuthOption()).getPassword(),
                        StandardCharsets.UTF_8),
                "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assertions.assertNull(stmt.getAuthOption().getAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED BY PASSWORD '*59c70da2f3e3a5bdf46b68f5c8b8f25762bccef0'";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assertions.assertEquals("user", stmt.getUser().getUser());
        Assertions.assertEquals("ALTER USER 'user'@'%' IDENTIFIED BY PASSWORD '*59c70da2f3e3a5bdf46b68f5c8b8f25762bccef0'",
                AstToSQLBuilder.toSQL(stmt));
        Assertions.assertEquals(new String(new UserAuthenticationInfo(stmt.getUser(), stmt.getAuthOption()).getPassword(),
                        StandardCharsets.UTF_8),
                "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assertions.assertNull(stmt.getAuthOption().getAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED BY ''";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assertions.assertEquals("ALTER USER 'user'@'%'", AstToSQLBuilder.toSQL(stmt));
        Assertions.assertEquals(new String(new UserAuthenticationInfo(stmt.getUser(), stmt.getAuthOption()).getPassword(),
                StandardCharsets.UTF_8), "");
        Assertions.assertNull(stmt.getAuthOption().getAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD BY 'passwd'";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assertions.assertEquals("ALTER USER 'user'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD BY 'passwd'",
                AstToSQLBuilder.toSQL(stmt));
        Assertions.assertEquals(new String(new UserAuthenticationInfo(stmt.getUser(), stmt.getAuthOption()).getPassword(),
                        StandardCharsets.UTF_8),
                "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assertions.assertEquals(AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.name(), stmt.getAuthOption().getAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS '*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0'";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assertions.assertEquals(
                "ALTER USER 'user'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS '*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0'",
                AstToSQLBuilder.toSQL(stmt));
        Assertions.assertEquals(new String(new UserAuthenticationInfo(stmt.getUser(), stmt.getAuthOption()).getPassword(),
                        StandardCharsets.UTF_8),
                "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assertions.assertEquals(AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.name(), stmt.getAuthOption().getAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS ''";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assertions.assertEquals("ALTER USER 'user'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD", AstToSQLBuilder.toSQL(stmt));
        Assertions.assertEquals(new String(new UserAuthenticationInfo(stmt.getUser(), stmt.getAuthOption()).getPassword(),
                        StandardCharsets.UTF_8),
                "");
        Assertions.assertEquals(AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.name(), stmt.getAuthOption().getAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE AS 'uid=gengjun,ou=people,dc=example,dc=io'";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assertions.assertEquals(
                "ALTER USER 'user'@'%' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE AS 'uid=gengjun,ou=people,dc=example,dc=io'",
                AstToSQLBuilder.toSQL(stmt));
        Assertions.assertEquals(new String(new UserAuthenticationInfo(stmt.getUser(), stmt.getAuthOption()).getPassword(),
                StandardCharsets.UTF_8), "");
        Assertions.assertEquals(AuthPlugin.Server.AUTHENTICATION_LDAP_SIMPLE.name(), stmt.getAuthOption().getAuthPlugin());
        Assertions.assertEquals("uid=gengjun,ou=people,dc=example,dc=io", new UserAuthenticationInfo(stmt.getUser(),
                stmt.getAuthOption()).getAuthString());

        sql = "ALTER USER 'user' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE BY 'uid=gengjun,ou=people,dc=example,dc=io'";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assertions.assertEquals(
                "ALTER USER 'user'@'%' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE BY 'uid=gengjun,ou=people,dc=example,dc=io'",
                AstToSQLBuilder.toSQL(stmt));
        Assertions.assertEquals(new String(new UserAuthenticationInfo(stmt.getUser(), stmt.getAuthOption()).getPassword(),
                StandardCharsets.UTF_8), "");
        Assertions.assertEquals(AuthPlugin.Server.AUTHENTICATION_LDAP_SIMPLE.name(), stmt.getAuthOption().getAuthPlugin());
        Assertions.assertEquals("uid=gengjun,ou=people,dc=example,dc=io", new UserAuthenticationInfo(stmt.getUser(),
                stmt.getAuthOption()).getAuthString());

        sql = "ALTER USER 'user' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assertions.assertEquals("ALTER USER 'user'@'%' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE",
                AstToSQLBuilder.toSQL(stmt));
        Assertions.assertEquals(AuthPlugin.Server.AUTHENTICATION_LDAP_SIMPLE.name(), stmt.getAuthOption().getAuthPlugin());
        Assertions.assertNull(new UserAuthenticationInfo(stmt.getUser(), stmt.getAuthOption()).getAuthString());
    }

    @Test
    public void testEmptyUser() {
        assertThrows(AnalysisException.class, () -> {
            String sql = "ALTER USER '' IDENTIFIED BY 'passwd'";
            UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testBadPass() {
        assertThrows(ErrorReportException.class, () -> {
            String sql = "ALTER USER 'user' IDENTIFIED BY PASSWORD 'passwd12345'";
            UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testInvalidAuthPlugin() {
        assertThrows(AnalysisException.class, () -> {
            String sql = "ALTER USER 'user' IDENTIFIED WITH authentication_ldap_sasl";
            UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testRootPassword() {
        assertThrows(SemanticException.class, () -> {
            String createUserSql = "CREATE USER 'admin' IDENTIFIED BY ''";
            CreateUserStmt createUserStmt =
                    (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());
            AuthenticationMgr authenticationManager =
                    starRocksAssert.getCtx().getGlobalStateMgr().getAuthenticationMgr();
            authenticationManager.createUser(createUserStmt);

            starRocksAssert.getCtx().setCurrentUserIdentity(new UserIdentity("admin", "%"));
            starRocksAssert.getCtx().setCurrentRoleIds(new UserIdentity("admin", "%"));

            AlterUserStmt alterUserStmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(
                    "alter user root identified by \"12345\"; ", starRocksAssert.getCtx());
            Authorizer.check(alterUserStmt, starRocksAssert.getCtx());
        });
    }
}
