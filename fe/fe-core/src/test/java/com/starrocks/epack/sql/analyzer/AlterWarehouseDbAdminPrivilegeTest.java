// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.google.common.collect.Sets;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.ErrorReportException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;

/**
 * `db_admin` is exempted from the ALTER-on-warehouse check for `ALTER WAREHOUSE ... SET (...)`,
 * so that a database administrator can tune warehouse properties without a cluster administrator.
 *
 * The exemption follows role inheritance, only counts roles the session actually activated, and
 * must not leak into any other warehouse statement.
 */
public class AlterWarehouseDbAdminPrivilegeTest {
    private static final String WAREHOUSE = WarehouseManager.DEFAULT_WAREHOUSE_NAME;

    private static final String DIRECT_USER = "u_wh_db_admin";
    private static final String NESTED_USER = "u_wh_nested";
    private static final String PLAIN_USER = "u_wh_plain";
    private static final String REVOKED_USER = "u_wh_revoked";

    private static final String INNER_ROLE = "r_wh_inner";
    private static final String OUTER_ROLE = "r_wh_outer";

    private static ConnectContext ctx;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        ctx.setThreadLocalInfo();

        // db_admin granted straight to the user
        ddl("create user " + DIRECT_USER);
        ddl("grant db_admin to user " + DIRECT_USER);

        // db_admin reached through a chain of roles: outer -> inner -> db_admin
        ddl("create user " + NESTED_USER);
        ddl("create role " + INNER_ROLE);
        ddl("create role " + OUTER_ROLE);
        ddl("grant db_admin to role " + INNER_ROLE);
        ddl("grant " + INNER_ROLE + " to role " + OUTER_ROLE);
        ddl("grant " + OUTER_ROLE + " to user " + NESTED_USER);

        // no warehouse-related role at all
        ddl("create user " + PLAIN_USER);

        // db_admin granted for now, revoked inside the test
        ddl("create user " + REVOKED_USER);
        ddl("grant db_admin to user " + REVOKED_USER);
    }

    @Test
    public void testDbAdminMayAlterWarehouseProperties() throws Exception {
        useUserWithAllRoles(DIRECT_USER);
        checkPasses("alter warehouse " + WAREHOUSE + " set (\"compute_replica\" = \"2\")");
        checkPasses("alter warehouse " + WAREHOUSE + " set (\"enable_query_queue\" = \"true\", " +
                "\"query_queue_concurrency_limit\" = \"8\")");
    }

    @Test
    public void testDbAdminMayAlterWarehouseSessionVariables() throws Exception {
        useUserWithAllRoles(DIRECT_USER);
        checkPasses("alter warehouse " + WAREHOUSE + " set (\"session.query_timeout\" = \"600\")");
        checkPasses("alter warehouse " + WAREHOUSE + " set (\"session.query_timeout\" = \"600\", " +
                "\"compute_replica\" = \"2\")");
    }

    @Test
    public void testDbAdminInheritedThroughRoleChainIsAllowed() throws Exception {
        useUserWithAllRoles(NESTED_USER);
        checkPasses("alter warehouse " + WAREHOUSE + " set (\"compute_replica\" = \"2\")");
    }

    @Test
    public void testDbAdminNotActivatedIsDenied() throws Exception {
        // the user owns db_admin but activated no role at all, as after `SET ROLE NONE`
        useUser(DIRECT_USER, Sets.newHashSet());
        checkDenied("alter warehouse " + WAREHOUSE + " set (\"compute_replica\" = \"2\")");
    }

    @Test
    public void testUserWithoutDbAdminIsDenied() throws Exception {
        useUserWithAllRoles(PLAIN_USER);
        checkDenied("alter warehouse " + WAREHOUSE + " set (\"compute_replica\" = \"2\")");
        checkDenied("alter warehouse " + WAREHOUSE + " set (\"session.query_timeout\" = \"600\")");
    }

    @Test
    public void testDbAdminRevokedMidSessionIsDenied() throws Exception {
        useUserWithAllRoles(REVOKED_USER);
        checkPasses("alter warehouse " + WAREHOUSE + " set (\"compute_replica\" = \"2\")");

        // the session keeps listing db_admin as activated after the revoke, so the exemption has to
        // re-check the roles the user still owns rather than trust the session's snapshot
        Set<Long> staleRoleIds = ctx.getCurrentRoleIds();
        Assertions.assertTrue(staleRoleIds.contains(PrivilegeBuiltinConstants.DB_ADMIN_ROLE_ID));

        ctxAsRoot();
        ddl("revoke db_admin from user " + REVOKED_USER);

        useUser(REVOKED_USER, staleRoleIds);
        checkDenied("alter warehouse " + WAREHOUSE + " set (\"compute_replica\" = \"2\")");
    }

    @Test
    public void testExemptionDoesNotCoverOtherWarehouseStatements() throws Exception {
        useUserWithAllRoles(DIRECT_USER);
        // both need the very same ALTER-on-warehouse privilege the SET statement is exempted from
        checkDenied("suspend warehouse " + WAREHOUSE);
        checkDenied("resume warehouse " + WAREHOUSE);
    }

    // ------------------------------------------------------------------------------------------

    private static void ddl(String sql) throws Exception {
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);
    }

    private static void ctxAsRoot() {
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setQualifiedUser(UserIdentity.ROOT.getUser());
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
    }

    /**
     * Activate every role the user owns, which is what a login does when
     * `activate_all_roles_on_login` is on.
     */
    private static void useUserWithAllRoles(String user) throws Exception {
        UserIdentity identity = UserIdentity.createAnalyzedUserIdentWithIp(user, "%");
        AuthorizationMgr authorizationMgr = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        useUser(user, authorizationMgr.getRoleIdsByUser(identity));
    }

    private static void useUser(String user, Set<Long> activatedRoleIds) {
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(user, "%"));
        ctx.setQualifiedUser(user);
        ctx.setCurrentRoleIds(activatedRoleIds);
    }

    private static void checkPasses(String sql) throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Authorizer.check(stmt, ctx);
    }

    private static void checkDenied(String sql) throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        ErrorReportException e =
                Assertions.assertThrows(ErrorReportException.class, () -> Authorizer.check(stmt, ctx), sql);
        Assertions.assertTrue(e.getMessage().contains("Access denied"), e.getMessage());
    }
}
