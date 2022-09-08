// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import mockit.Expectations;
import mockit.Injectable;
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
        ctx.setThreadLocalInfo();
    }

    @Test
    public void testToString(@Injectable Analyzer analyzer,
                             @Mocked Auth auth) throws UserException, AnalysisException {

        new Expectations() {
            {
<<<<<<< HEAD
                analyzer.getClusterName();
                result = "testCluster";
                auth.checkHasPriv((ConnectContext) any, PrivPredicate.GRANT, Auth.PrivLevel.GLOBAL, Auth
                        .PrivLevel.DATABASE);
=======
                auth.doesUserExist((UserIdentity) any);
>>>>>>> 82db084e8 ([BugFix] Fix checking the existance of domained users fails (#10999))
                result = true;
            }
        };

        AlterUserStmt stmt = new AlterUserStmt(new UserDesc(new UserIdentity("user", "%"), "passwd", true));
        stmt.analyze(analyzer);
        Assert.assertEquals("ALTER USER 'testCluster:user'@'%' IDENTIFIED BY '*XXX'", stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assert.assertNull(stmt.getAuthPlugin());

        stmt = new AlterUserStmt(
                new UserDesc(new UserIdentity("user", "%"), "*59c70da2f3e3a5bdf46b68f5c8b8f25762bccef0", false));
        stmt.analyze(analyzer);
        Assert.assertEquals("testCluster:user", stmt.getUserIdent().getQualifiedUser());
        Assert.assertEquals(
                "ALTER USER 'testCluster:user'@'%' IDENTIFIED BY PASSWORD '*59c70da2f3e3a5bdf46b68f5c8b8f25762bccef0'",
                stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assert.assertNull(stmt.getAuthPlugin());

        stmt = new AlterUserStmt(new UserDesc(new UserIdentity("user", "%"), "", false));
        stmt.analyze(analyzer);
        Assert.assertEquals("ALTER USER 'testCluster:user'@'%'", stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "");
        Assert.assertNull(stmt.getAuthPlugin());

        stmt = new AlterUserStmt(
                new UserDesc(new UserIdentity("user", "%"), AuthPlugin.MYSQL_NATIVE_PASSWORD.name(), "passwd", true));
        stmt.analyze(analyzer);
        Assert.assertEquals("ALTER USER 'testCluster:user'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD BY 'passwd'",
                stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assert.assertEquals(AuthPlugin.MYSQL_NATIVE_PASSWORD.name(), stmt.getAuthPlugin());

        stmt = new AlterUserStmt(new UserDesc(new UserIdentity("user", "%"), AuthPlugin.MYSQL_NATIVE_PASSWORD.name(),
                "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0", false));
        stmt.analyze(analyzer);
        Assert.assertEquals(
                "ALTER USER 'testCluster:user'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS '*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0'",
                stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");
        Assert.assertEquals(AuthPlugin.MYSQL_NATIVE_PASSWORD.name(), stmt.getAuthPlugin());

        stmt = new AlterUserStmt(new UserDesc(new UserIdentity("user", "%"), AuthPlugin.MYSQL_NATIVE_PASSWORD.name()));
        stmt.analyze(analyzer);
        Assert.assertEquals("ALTER USER 'testCluster:user'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD", stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "");
        Assert.assertEquals(AuthPlugin.MYSQL_NATIVE_PASSWORD.name(), stmt.getAuthPlugin());

        stmt = new AlterUserStmt(
                new UserDesc(new UserIdentity("user", "%"), AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name(),
                        "uid=gengjun,ou=people,dc=example,dc=io", false));
        stmt.analyze(analyzer);
        Assert.assertEquals(
                "ALTER USER 'testCluster:user'@'%' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE AS 'uid=gengjun,ou=people,dc=example,dc=io'",
                stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "");
        Assert.assertEquals(AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name(), stmt.getAuthPlugin());
        Assert.assertEquals("uid=gengjun,ou=people,dc=example,dc=io", stmt.getUserForAuthPlugin());

        stmt = new AlterUserStmt(
                new UserDesc(new UserIdentity("user", "%"), AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name(),
                        "uid=gengjun,ou=people,dc=example,dc=io", true));
        stmt.analyze(analyzer);
        Assert.assertEquals(
                "ALTER USER 'testCluster:user'@'%' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE BY 'uid=gengjun,ou=people,dc=example,dc=io'",
                stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "");
        Assert.assertEquals(AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name(), stmt.getAuthPlugin());
        Assert.assertEquals("uid=gengjun,ou=people,dc=example,dc=io", stmt.getUserForAuthPlugin());

        stmt = new AlterUserStmt(
                new UserDesc(new UserIdentity("user", "%"), AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name()));
        stmt.analyze(analyzer);
        Assert.assertEquals("ALTER USER 'testCluster:user'@'%' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE",
                stmt.toString());
        Assert.assertEquals(AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name(), stmt.getAuthPlugin());
        Assert.assertNull(stmt.getUserForAuthPlugin());
    }

    @Test(expected = AnalysisException.class)
    public void testEmptyUser(@Injectable Analyzer analyzer) throws UserException, AnalysisException {
        new Expectations() {
            {
                analyzer.getClusterName();
                result = "testCluster";
            }
        };
        AlterUserStmt stmt = new AlterUserStmt(new UserDesc(new UserIdentity("", "%"), "passwd", true));
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testBadPass(@Injectable Analyzer analyzer) throws UserException, AnalysisException {
        new Expectations() {
            {
<<<<<<< HEAD
                analyzer.getClusterName();
                result = "testCluster";
=======
                auth.doesUserExist((UserIdentity) any);
                result = true;
>>>>>>> 82db084e8 ([BugFix] Fix checking the existance of domained users fails (#10999))
            }
        };
        AlterUserStmt stmt = new AlterUserStmt(new UserDesc(new UserIdentity("", "%"), "passwd", false));
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testInvalidAuthPlugin(@Injectable Analyzer analyzer) throws UserException, AnalysisException {
        new Expectations() {
            {
<<<<<<< HEAD
                analyzer.getClusterName();
                result = "testCluster";
=======
                auth.doesUserExist((UserIdentity) any);
                result = true;
>>>>>>> 82db084e8 ([BugFix] Fix checking the existance of domained users fails (#10999))
            }
        };
        AlterUserStmt stmt = new AlterUserStmt(new UserDesc(new UserIdentity("user", "%"), "authentication_ldap_sasl"));
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }
}
