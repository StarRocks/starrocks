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

import com.starrocks.authentication.AuthenticationManager;
import com.starrocks.common.AnalysisException;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AlterUserStmtTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        starRocksAssert = new StarRocksAssert(UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT));
        String createUserSql = "CREATE USER 'user' IDENTIFIED BY ''";
        CreateUserStmt createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());

        AuthenticationManager authenticationManager =
                starRocksAssert.getCtx().getGlobalStateMgr().getAuthenticationManager();
        authenticationManager.createUser(createUserStmt);
    }

    @Test
    public void testToString() throws Exception {
        String sql = "ALTER USER 'user' IDENTIFIED BY 'passwd'";
        AlterUserStmt stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("ALTER USER 'user'@'%' IDENTIFIED BY '*XXX'",
                AstToSQLBuilder.toSQL(stmt));
        Assert.assertEquals(new String(stmt.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assert.assertNull(stmt.getAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED BY PASSWORD '*59c70da2f3e3a5bdf46b68f5c8b8f25762bccef0'";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("user", stmt.getUserIdent().getQualifiedUser());
        Assert.assertEquals(
                "ALTER USER 'user'@'%' IDENTIFIED BY PASSWORD '*59c70da2f3e3a5bdf46b68f5c8b8f25762bccef0'",
                AstToSQLBuilder.toSQL(stmt));
        Assert.assertEquals(new String(stmt.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assert.assertNull(stmt.getAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED BY ''";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("ALTER USER 'user'@'%'", AstToSQLBuilder.toSQL(stmt));
        Assert.assertEquals(new String(stmt.getPassword()), "");
        Assert.assertNull(stmt.getAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD BY 'passwd'";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("ALTER USER 'user'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD BY 'passwd'",
                AstToSQLBuilder.toSQL(stmt));
        Assert.assertEquals(new String(stmt.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assert.assertEquals(AuthPlugin.MYSQL_NATIVE_PASSWORD.name(), stmt.getAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS '*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0'";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals(
                "ALTER USER 'user'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS '*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0'",
                AstToSQLBuilder.toSQL(stmt));
        Assert.assertEquals(new String(stmt.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assert.assertEquals(AuthPlugin.MYSQL_NATIVE_PASSWORD.name(), stmt.getAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS ''";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("ALTER USER 'user'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD", AstToSQLBuilder.toSQL(stmt));
        Assert.assertEquals(new String(stmt.getPassword()), "");
        Assert.assertEquals(AuthPlugin.MYSQL_NATIVE_PASSWORD.name(), stmt.getAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE AS 'uid=gengjun,ou=people,dc=example,dc=io'";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals(
                "ALTER USER 'user'@'%' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE AS 'uid=gengjun,ou=people,dc=example,dc=io'",
                AstToSQLBuilder.toSQL(stmt));
        Assert.assertEquals(new String(stmt.getPassword()), "");
        Assert.assertEquals(AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name(), stmt.getAuthPlugin());
        Assert.assertEquals("uid=gengjun,ou=people,dc=example,dc=io", stmt.getUserForAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE BY 'uid=gengjun,ou=people,dc=example,dc=io'";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals(
                "ALTER USER 'user'@'%' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE BY 'uid=gengjun,ou=people,dc=example,dc=io'",
                AstToSQLBuilder.toSQL(stmt));
        Assert.assertEquals(new String(stmt.getPassword()), "");
        Assert.assertEquals(AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name(), stmt.getAuthPlugin());
        Assert.assertEquals("uid=gengjun,ou=people,dc=example,dc=io", stmt.getUserForAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("ALTER USER 'user'@'%' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE",
                AstToSQLBuilder.toSQL(stmt));
        Assert.assertEquals(AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name(), stmt.getAuthPlugin());
        Assert.assertNull(stmt.getUserForAuthPlugin());
    }

    @Test(expected = AnalysisException.class)
    public void testEmptyUser() throws Exception {
        String sql = "ALTER USER '' IDENTIFIED BY 'passwd'";
        UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testBadPass() throws Exception {
        String sql = "ALTER USER 'user' IDENTIFIED BY PASSWORD 'passwd12345'";
        UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testInvalidAuthPlugin() throws Exception {
        String sql = "ALTER USER 'user' IDENTIFIED WITH authentication_ldap_sasl";
        UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.fail("No exception throws.");
    }
}
