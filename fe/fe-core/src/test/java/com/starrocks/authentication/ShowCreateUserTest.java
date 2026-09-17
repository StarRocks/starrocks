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

import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.DefaultAuthorizationProvider;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.ErrorReportException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.ShowCreateUserStmt;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SHOW CREATE USER: statement-level authorization (self is free, another user needs GRANT on SYSTEM)
 * and secret masking driven by the SHOW SECRET privilege.
 */
public class ShowCreateUserTest {
    // MySQL native digest of "123456"
    private static final String DIGEST_123456 = "*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9";

    private AuthenticationMgr authenticationMgr;
    private AuthorizationMgr authorizationMgr;

    private ConnectContext rootCtx;
    // user_admin: holds GRANT on SYSTEM, but not SHOW SECRET
    private ConnectContext userAdminCtx;
    // user_admin plus a role carrying SHOW SECRET on SYSTEM
    private ConnectContext secretReaderCtx;
    // no roles at all
    private ConnectContext plainUserCtx;

    @BeforeAll
    public static void setUpPersistJournal() throws Exception {
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterAll
    public static void tearDownPersistJournal() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @BeforeEach
    public void setUp() throws Exception {
        authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);
        authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);

        rootCtx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);

        createUser("jack", "123456");
        createUser("user_admin_user", null);
        createUser("secret_reader", null);
        createUser("plain_user", null);

        // GRANT on SYSTEM cannot be granted directly, so reuse the built-in user_admin role that holds it
        createRoleWithSystemPrivilege("secret_role", "SHOW SECRET");
        grantRoles("user_admin_user", PrivilegeBuiltinConstants.USER_ADMIN_ROLE_NAME);
        grantRoles("secret_reader", PrivilegeBuiltinConstants.USER_ADMIN_ROLE_NAME, "secret_role");

        userAdminCtx = ctxFor("user_admin_user");
        secretReaderCtx = ctxFor("secret_reader");
        plainUserCtx = ctxFor("plain_user");
    }

    private void createUser(String name, String password) throws Exception {
        UserAuthOption authOption = password == null
                ? null : new UserAuthOption(null, password, true, NodePosition.ZERO);
        authenticationMgr.createUser(new CreateUserStmt(
                new UserRef(name, "%"), true, authOption, List.of(), Map.of(), NodePosition.ZERO));
    }

    private void createRoleWithSystemPrivilege(String roleName, String privilege) throws Exception {
        authorizationMgr.createRole(new CreateRoleStmt(List.of(roleName), true, ""));
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT " + privilege + " ON SYSTEM TO ROLE " + roleName, new ConnectContext());
        authorizationMgr.grant(grantStmt);
    }

    private void grantRoles(String user, String... roleNames) throws Exception {
        List<String> roles = List.of(roleNames);
        authorizationMgr.grantRole(new GrantRoleStmt(roles, new UserRef(user, "%"), NodePosition.ZERO));
        Set<Long> roleIds = new HashSet<>();
        for (String roleName : roles) {
            roleIds.add(authorizationMgr.getRoleIdByNameAllowNull(roleName));
        }
        authorizationMgr.setUserDefaultRole(roleIds, new UserIdentity(user, "%"));
    }

    /**
     * initCtxForNewPrivilege pins the role set to the root role, which would make every privilege
     * check pass. Reset it to the user's own default roles.
     */
    private ConnectContext ctxFor(String user) throws Exception {
        UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp(user, "%");
        ConnectContext context = UtFrameUtils.initCtxForNewPrivilege(userIdentity);
        context.setCurrentRoleIds(authorizationMgr.getDefaultRoleIdsByUser(new UserIdentity(user, "%")));
        return context;
    }

    /** Creates a user by executing a real CREATE USER statement, so every clause takes the normal path. */
    private void execCreateUser(String sql) throws Exception {
        CreateUserStmt stmt = (CreateUserStmt) SqlParser.parseSingleStatement(
                sql, rootCtx.getSessionVariable().getSqlMode());
        Analyzer.analyze(stmt, rootCtx);
        authenticationMgr.createUser(stmt);
    }

    private static ShowCreateUserStmt parse(String sql, ConnectContext context) {
        return (ShowCreateUserStmt) SqlParser.parseSingleStatement(
                sql, context.getSessionVariable().getSqlMode());
    }

    private static String run(String sql, ConnectContext context) {
        ShowCreateUserStmt stmt = parse(sql, context);
        Analyzer.analyze(stmt, context);
        Authorizer.check(stmt, context);
        ShowResultSet resultSet = ShowExecutor.execute(stmt, context);
        Assertions.assertEquals(1, resultSet.getResultRows().size());
        return resultSet.getResultRows().get(0).get(1);
    }

    @Test
    public void testParseUserAndCurrentUser() {
        Assertions.assertEquals("jack", parse("SHOW CREATE USER 'jack'@'%'", rootCtx).getUser().getUser());
        Assertions.assertNull(parse("SHOW CREATE USER CURRENT_USER()", rootCtx).getUser());
        Assertions.assertNull(parse("SHOW CREATE USER CURRENT_USER", rootCtx).getUser());
    }

    @Test
    public void testCurrentUserIsFilledByAnalyzer() {
        ShowCreateUserStmt stmt = parse("SHOW CREATE USER CURRENT_USER()", plainUserCtx);
        Analyzer.analyze(stmt, plainUserCtx);
        Assertions.assertEquals("plain_user", stmt.getUser().getUser());
    }

    @Test
    public void testUnknownUserIsRejected() {
        ShowCreateUserStmt stmt = parse("SHOW CREATE USER 'nobody'@'%'", rootCtx);
        Assertions.assertThrows(SemanticException.class, () -> Analyzer.analyze(stmt, rootCtx));
    }

    @Test
    public void testFirstColumnIsTheUserIdentity() {
        ShowCreateUserStmt stmt = parse("SHOW CREATE USER 'jack'@'%'", rootCtx);
        Analyzer.analyze(stmt, rootCtx);
        ShowResultSet resultSet = ShowExecutor.execute(stmt, rootCtx);
        Assertions.assertEquals("'jack'@'%'", resultSet.getResultRows().get(0).get(0));
    }

    @Test
    public void testSelfNeedsNoPrivilege() {
        Assertions.assertEquals("CREATE USER 'plain_user'@'%'",
                run("SHOW CREATE USER CURRENT_USER()", plainUserCtx));
        Assertions.assertEquals("CREATE USER 'plain_user'@'%'",
                run("SHOW CREATE USER 'plain_user'@'%'", plainUserCtx));
    }

    @Test
    public void testOtherUserNeedsSystemGrant() {
        ShowCreateUserStmt stmt = parse("SHOW CREATE USER 'jack'@'%'", plainUserCtx);
        Analyzer.analyze(stmt, plainUserCtx);
        Assertions.assertThrows(ErrorReportException.class, () -> Authorizer.check(stmt, plainUserCtx));

        Assertions.assertDoesNotThrow(() -> run("SHOW CREATE USER 'jack'@'%'", userAdminCtx));
    }

    @Test
    public void testSecretMaskedWithoutShowSecret() {
        String ddl = run("SHOW CREATE USER 'jack'@'%'", userAdminCtx);
        Assertions.assertTrue(ddl.contains("IDENTIFIED WITH mysql_native_password AS '<secret>'"), ddl);
        Assertions.assertFalse(ddl.contains(DIGEST_123456), ddl);
    }

    @Test
    public void testSecretVisibleWithShowSecret() {
        String ddl = run("SHOW CREATE USER 'jack'@'%'", secretReaderCtx);
        Assertions.assertTrue(ddl.contains("IDENTIFIED WITH mysql_native_password AS '" + DIGEST_123456 + "'"), ddl);
    }

    @Test
    public void testRootSeesTheSecret() {
        String ddl = run("SHOW CREATE USER 'jack'@'%'", rootCtx);
        Assertions.assertTrue(ddl.contains(DIGEST_123456), ddl);
    }

    @Test
    public void testOwnSecretIsAlsoMaskedWithoutShowSecret() throws Exception {
        createUser("selfie", "123456");
        ConnectContext selfCtx = ctxFor("selfie");

        String ddl = run("SHOW CREATE USER CURRENT_USER()", selfCtx);
        Assertions.assertTrue(ddl.contains("IDENTIFIED WITH mysql_native_password AS '<secret>'"), ddl);
        Assertions.assertFalse(ddl.contains(DIGEST_123456), ddl);
    }

    @Test
    public void testDefaultRoleIsRendered() {
        String ddl = run("SHOW CREATE USER 'secret_reader'@'%'", rootCtx);
        Assertions.assertTrue(ddl.contains("DEFAULT ROLE 'secret_role', 'user_admin'"), ddl);
    }

    @Test
    public void testUnknownUserStillFails() {
        ShowCreateUserStmt stmt = parse("SHOW CREATE USER 'not_a_user'@'%'", rootCtx);
        Assertions.assertThrows(SemanticException.class, () -> Analyzer.analyze(stmt, rootCtx));
    }

    /**
     * The emitted statement is consumed by the cluster migration tool, so it has to recreate the user
     * exactly. Render a user carrying every clause at once, replay that exact string, and require the
     * second rendering to be byte-identical and the resulting state to match.
     */
    @Test
    public void testFullCompositionRoundTrip() throws Exception {
        execCreateUser("CREATE USER 'full_shape'@'%' IDENTIFIED BY '123456' DEFAULT ROLE 'secret_role' "
                + "EXPIRE_PASSWORD = true LOCK PROPERTIES (\"max_user_connections\" = \"100\")");

        String ddl = run("SHOW CREATE USER 'full_shape'@'%'", rootCtx);
        // every clause must be present, otherwise the round-trip proves nothing
        Assertions.assertTrue(ddl.contains("IDENTIFIED WITH mysql_native_password AS '" + DIGEST_123456 + "'"), ddl);
        Assertions.assertTrue(ddl.contains("DEFAULT ROLE 'secret_role'"), ddl);
        Assertions.assertTrue(ddl.contains("EXPIRE_PASSWORD = true"), ddl);
        Assertions.assertTrue(ddl.contains("LOCK"), ddl);
        Assertions.assertTrue(ddl.contains("\"max_user_connections\" = \"100\""), ddl);

        UserIdentity userIdentity = new UserIdentity("full_shape", "%");
        UserAuthenticationInfo before = authenticationMgr.getUserAuthenticationInfoByUserIdentity(userIdentity);
        Set<Long> rolesBefore = authorizationMgr.getDefaultRoleIdsByUser(userIdentity);

        authenticationMgr.dropUser(new DropUserStmt(new UserRef("full_shape", "%"), false, NodePosition.ZERO));
        execCreateUser(ddl);

        Assertions.assertEquals(ddl, run("SHOW CREATE USER 'full_shape'@'%'", rootCtx));

        UserAuthenticationInfo after = authenticationMgr.getUserAuthenticationInfoByUserIdentity(userIdentity);
        Assertions.assertArrayEquals(before.getPassword(), after.getPassword());
        Assertions.assertEquals(before.getAuthPlugin(), after.getAuthPlugin());
        Assertions.assertEquals(before.isPasswordExpired(), after.isPasswordExpired());
        Assertions.assertEquals(before.isLock(), after.isLock());
        Assertions.assertTrue(after.isPasswordExpired());
        Assertions.assertTrue(after.isLock());
        Assertions.assertEquals(
                authorizationMgr.getRoleNamesByRoleIds(rolesBefore),
                authorizationMgr.getRoleNamesByRoleIds(authorizationMgr.getDefaultRoleIdsByUser(userIdentity)));
        Assertions.assertEquals(100, authenticationMgr.getUserProperty("full_shape").getMaxConn());
    }

    /**
     * The grammar relaxation applies to ALTER USER as well as CREATE USER, and the emitted DDL only
     * ever exercises the CREATE form. Cover the ALTER form so the change to existing syntax is not
     * left untested.
     */
    @Test
    public void testAlterUserAcceptsExpireAndLockInOneStatement() throws Exception {
        execCreateUser("CREATE USER 'alter_both'@'%' IDENTIFIED BY '123456'");

        AlterUserStmt alterStmt = (AlterUserStmt) SqlParser.parseSingleStatement(
                "ALTER USER 'alter_both'@'%' EXPIRE_PASSWORD = true LOCK",
                rootCtx.getSessionVariable().getSqlMode());
        Analyzer.analyze(alterStmt, rootCtx);
        Assertions.assertNotNull(alterStmt.getPasswordOption());
        Assertions.assertNotNull(alterStmt.getLockOption());

        // mirrors DDLStmtExecutor.visitAlterUserStatement
        UserIdentity userIdentity = new UserIdentity("alter_both", "%");
        authenticationMgr.alterUser(userIdentity, null,
                alterStmt.getPasswordOption(), alterStmt.getLockOption(), alterStmt.getProperties());

        UserAuthenticationInfo info = authenticationMgr.getUserAuthenticationInfoByUserIdentity(userIdentity);
        Assertions.assertTrue(info.isPasswordExpired());
        Assertions.assertTrue(info.isLock());

        String ddl = run("SHOW CREATE USER 'alter_both'@'%'", rootCtx);
        Assertions.assertTrue(ddl.contains("EXPIRE_PASSWORD = true"), ddl);
        Assertions.assertTrue(ddl.contains("LOCK"), ddl);
    }

    /**
     * SECRET is a new lexer token. It is listed in nonReserved, so it must still work as an ordinary
     * identifier; if that line were ever dropped, previously valid SQL would stop parsing.
     */
    @Test
    public void testSecretIsStillUsableAsAnIdentifier() {
        long sqlMode = rootCtx.getSessionVariable().getSqlMode();
        for (String sql : List.of(
                "SELECT secret FROM secret",
                "SELECT secret.secret FROM db.secret secret",
                "CREATE TABLE secret (secret INT) DUPLICATE KEY(secret) DISTRIBUTED BY HASH(secret) BUCKETS 1",
                "SELECT a AS secret FROM t")) {
            Assertions.assertDoesNotThrow(() -> SqlParser.parseSingleStatement(sql, sqlMode), sql);
        }
        Assertions.assertEquals("secret",
                parse("SHOW CREATE USER secret", rootCtx).getUser().getUser());
    }

    @Test
    public void testOutputIsReparsable() {
        String ddl = run("SHOW CREATE USER 'jack'@'%'", rootCtx);
        Assertions.assertInstanceOf(CreateUserStmt.class,
                SqlParser.parseSingleStatement(ddl, rootCtx.getSessionVariable().getSqlMode()));
    }
}
