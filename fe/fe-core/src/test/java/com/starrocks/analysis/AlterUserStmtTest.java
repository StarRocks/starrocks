// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AlterUserStmtTest {

    @Before
    public void setUp() {
        ConnectContext ctx = new ConnectContext(null);
        ctx.setQualifiedUser("root");
        ctx.setRemoteIP("192.168.1.1");
        UserIdentity currentUserIdentity = new UserIdentity("root", "192.168.1.1");
        currentUserIdentity.setIsAnalyzed();
        ctx.setCurrentUserIdentity(currentUserIdentity);
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx.setThreadLocalInfo();
    }

    @Test
    public void testToString(@Mocked Auth auth) throws Exception {

        new Expectations() {
            {
                auth.doesUserExist((UserIdentity) any);
                result = true;
            }
        };

        String sql = "ALTER USER 'user' IDENTIFIED BY 'passwd'";
        AlterUserStmt stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("ALTER USER 'user'@'%' IDENTIFIED BY '*XXX'", stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assert.assertNull(stmt.getAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED BY PASSWORD '*59c70da2f3e3a5bdf46b68f5c8b8f25762bccef0'";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("user", stmt.getUserIdent().getQualifiedUser());
        Assert.assertEquals(
                "ALTER USER 'user'@'%' IDENTIFIED BY PASSWORD '*59c70da2f3e3a5bdf46b68f5c8b8f25762bccef0'",
                stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assert.assertNull(stmt.getAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED BY ''";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("ALTER USER 'user'@'%'", stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "");
        Assert.assertNull(stmt.getAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD BY 'passwd'";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("ALTER USER 'user'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD BY 'passwd'",
                stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assert.assertEquals(AuthPlugin.MYSQL_NATIVE_PASSWORD.name(), stmt.getAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS '*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0'";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals(
                "ALTER USER 'user'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS '*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0'",
                stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assert.assertEquals(AuthPlugin.MYSQL_NATIVE_PASSWORD.name(), stmt.getAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS ''";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("ALTER USER 'user'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD", stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "");
        Assert.assertEquals(AuthPlugin.MYSQL_NATIVE_PASSWORD.name(), stmt.getAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE AS 'uid=gengjun,ou=people,dc=example,dc=io'";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals(
                "ALTER USER 'user'@'%' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE AS 'uid=gengjun,ou=people,dc=example,dc=io'",
                stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "");
        Assert.assertEquals(AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name(), stmt.getAuthPlugin());
        Assert.assertEquals("uid=gengjun,ou=people,dc=example,dc=io", stmt.getUserForAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE BY 'uid=gengjun,ou=people,dc=example,dc=io'";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals(
                "ALTER USER 'user'@'%' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE BY 'uid=gengjun,ou=people,dc=example,dc=io'",
                stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "");
        Assert.assertEquals(AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name(), stmt.getAuthPlugin());
        Assert.assertEquals("uid=gengjun,ou=people,dc=example,dc=io", stmt.getUserForAuthPlugin());

        sql = "ALTER USER 'user' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE";
        stmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.assertEquals("ALTER USER 'user'@'%' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE",
                stmt.toString());
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
    public void testBadPass(@Mocked Auth auth) throws Exception {
        new Expectations() {
            {
                auth.doesUserExist((UserIdentity) any);
                result = true;
            }
        };
        String sql = "ALTER USER 'user' IDENTIFIED BY PASSWORD 'passwd'";
        UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testInvalidAuthPlugin(@Mocked Auth auth) throws Exception {
        new Expectations() {
            {
                auth.doesUserExist((UserIdentity) any);
                result = true;
            }
        };
        String sql = "ALTER USER 'user' IDENTIFIED WITH authentication_ldap_sasl";
        UtFrameUtils.parseStmtWithNewParser(sql, ConnectContext.get());
        Assert.fail("No exception throws.");
    }
}
