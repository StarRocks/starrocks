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

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.InformationSchemaDataSource;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TGetTablesInfoRequest;
import com.starrocks.thrift.TTableInfo;
import com.starrocks.thrift.TUserIdentity;
import com.starrocks.thrift.TUserRoles;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Rebuilding the caller of an FE RPC from the user identity a BE sends, on a thread that, like the RPC threads,
 * has no session.
 */
public class UserIdentityUtilsTest {
    private static final String DB = "user_ident_utils_db";

    private static long openRoleId;
    private static long secretRoleId;
    private static long groupRoleId;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        ctx.getGlobalStateMgr().getAuthorizationMgr().initBuiltinRolesAndUsers();
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase(DB).useDatabase(DB)
                .withTable("CREATE TABLE t_open (k INT) DUPLICATE KEY(k) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')")
                .withTable("CREATE TABLE t_secret (k INT) DUPLICATE KEY(k) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')")
                .withTable("CREATE TABLE t_group (k INT) DUPLICATE KEY(k) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        starRocksAssert.ddl("CREATE ROLE uiu_open_role");
        starRocksAssert.ddl("CREATE ROLE uiu_secret_role");
        starRocksAssert.ddl("GRANT SELECT ON TABLE " + DB + ".t_open TO ROLE uiu_open_role");
        starRocksAssert.ddl("GRANT SELECT ON TABLE " + DB + ".t_secret TO ROLE uiu_secret_role");
        starRocksAssert.ddl("CREATE ROLE uiu_group_role");
        starRocksAssert.ddl("GRANT SELECT ON TABLE " + DB + ".t_group TO ROLE uiu_group_role");
        starRocksAssert.ddl("GRANT uiu_group_role TO EXTERNAL GROUP uiu_group");
        starRocksAssert.ddl("CREATE USER uiu_native");
        starRocksAssert.ddl("GRANT uiu_open_role TO USER uiu_native");
        openRoleId = GlobalStateMgr.getCurrentState().getAuthorizationMgr().getRoleIdByNameAllowNull("uiu_open_role");
        secretRoleId =
                GlobalStateMgr.getCurrentState().getAuthorizationMgr().getRoleIdByNameAllowNull("uiu_secret_role");
        groupRoleId =
                GlobalStateMgr.getCurrentState().getAuthorizationMgr().getRoleIdByNameAllowNull("uiu_group_role");
    }

    private static TUserIdentity userIdent(UserIdentity user, List<Long> roleIds) {
        TUserIdentity userIdent = UserIdentityUtils.toThrift(user);
        TUserRoles roles = new TUserRoles();
        roles.setRole_id_list(roleIds);
        userIdent.setCurrent_role_ids(roles);
        return userIdent;
    }

    private static TUserIdentity userIdent(UserIdentity user, List<Long> roleIds, List<String> groups) {
        TUserIdentity userIdent = userIdent(user, roleIds);
        userIdent.setGroups(groups);
        return userIdent;
    }

    private static <T> T withoutSession(Callable<T> task) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            return executor.submit(() -> {
                assertNull(ConnectContext.get());
                return task.call();
            }).get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    private static ConnectContext callerOf(TUserIdentity userIdent) {
        ConnectContext context = new ConnectContext();
        UserIdentityUtils.setAuthInfoFromThrift(context, userIdent);
        return context;
    }

    @Test
    public void testEphemeralUserGetsActivatedRoles() throws Exception {
        // An ephemeral user owns no roles; its privileges are the roles its session activated.
        TUserIdentity userIdent = userIdent(new UserIdentity("uiu_ephemeral", "%", false, true), List.of(openRoleId));
        ConnectContext caller = withoutSession(() -> callerOf(userIdent));

        assertEquals(Set.of(openRoleId), caller.getCurrentRoleIds());
        assertDoesNotThrow(() -> Authorizer.checkTableAction(caller, DB, "t_open", PrivilegeType.SELECT));
        assertThrows(AccessDeniedException.class,
                () -> Authorizer.checkTableAction(caller, DB, "t_secret", PrivilegeType.SELECT));
    }

    @Test
    public void testNativeUserKeepsOnlyOwnedRoles() throws Exception {
        // A native user cannot claim a role it was not granted.
        TUserIdentity userIdent = userIdent(new UserIdentity("uiu_native", "%"), List.of(openRoleId, secretRoleId));
        ConnectContext caller = withoutSession(() -> callerOf(userIdent));

        assertEquals(Set.of(openRoleId), caller.getCurrentRoleIds());
        assertThrows(AccessDeniedException.class,
                () -> Authorizer.checkTableAction(caller, DB, "t_secret", PrivilegeType.SELECT));
    }

    @Test
    public void testNativeUserGetsTheRolesOfItsGroups() throws Exception {
        // A role the session got from its group, e.g. an LDAP memberOf, is kept, and grants its privileges.
        TUserIdentity userIdent = userIdent(new UserIdentity("uiu_native", "%"), List.of(openRoleId, groupRoleId),
                List.of("uiu_group"));
        ConnectContext caller = withoutSession(() -> callerOf(userIdent));

        assertEquals(Set.of("uiu_group"), caller.getGroups());
        assertEquals(Set.of(openRoleId, groupRoleId), caller.getCurrentRoleIds());
        assertDoesNotThrow(() -> Authorizer.checkTableAction(caller, DB, "t_group", PrivilegeType.SELECT));
        assertThrows(AccessDeniedException.class,
                () -> Authorizer.checkTableAction(caller, DB, "t_secret", PrivilegeType.SELECT));
    }

    @Test
    public void testGroupRoleNeedsTheGroup() throws Exception {
        // The role of a group is not kept for a caller that is not in the group.
        for (List<String> groups : List.of(List.<String>of(), List.of("uiu_other_group"))) {
            TUserIdentity userIdent = userIdent(new UserIdentity("uiu_native", "%"), List.of(openRoleId, groupRoleId),
                    groups);
            ConnectContext caller = withoutSession(() -> callerOf(userIdent));

            assertEquals(Set.of(openRoleId), caller.getCurrentRoleIds());
            assertThrows(AccessDeniedException.class,
                    () -> Authorizer.checkTableAction(caller, DB, "t_group", PrivilegeType.SELECT));
        }
    }

    @Test
    public void testNativeUserWithoutActivatedRolesGetsTheRolesOfItsGroups() throws Exception {
        // A caller that sends no role ids gets its default roles and the roles of its groups, as at login.
        TUserIdentity userIdent = UserIdentityUtils.toThrift(new UserIdentity("uiu_native", "%"));
        userIdent.setGroups(List.of("uiu_group"));
        ConnectContext caller = withoutSession(() -> callerOf(userIdent));

        assertDoesNotThrow(() -> Authorizer.checkTableAction(caller, DB, "t_group", PrivilegeType.SELECT));
    }

    @Test
    public void testSessionGroupsReachTheRpc() throws Exception {
        // information_schema.tables: a session whose only active role comes through its group sees what that role
        // may read (uiu_open_role is granted to the user but is not a default role, so it is not active).
        ConnectContext session = new ConnectContext();
        UserIdentity user = new UserIdentity("uiu_native", "%");
        session.setCurrentUserIdentity(user);
        session.setGroups(Set.of("uiu_group"));
        session.setCurrentRoleIds(user, Set.of("uiu_group"));
        TUserIdentity userIdent = UserIdentityUtils.callerToThrift(session);
        assertEquals(List.of("uiu_group"), userIdent.getGroups());

        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setCurrent_user_ident(userIdent);
        authInfo.setPattern(DB);
        TGetTablesInfoRequest request = new TGetTablesInfoRequest();
        request.setAuth_info(authInfo);

        List<String> tables = withoutSession(() -> InformationSchemaDataSource.generateTablesInfoResponse(request)
                .getTables_infos().stream().map(TTableInfo::getTable_name).sorted().collect(Collectors.toList()));
        assertEquals(List.of("t_group"), tables);
    }

    @Test
    public void testEphemeralUserListsTables() throws Exception {
        // information_schema.tables: an ephemeral user sees the tables its activated roles may read.
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setCurrent_user_ident(
                userIdent(new UserIdentity("uiu_ephemeral", "%", false, true), List.of(openRoleId)));
        authInfo.setPattern(DB);
        TGetTablesInfoRequest request = new TGetTablesInfoRequest();
        request.setAuth_info(authInfo);

        List<String> tables = withoutSession(() -> InformationSchemaDataSource.generateTablesInfoResponse(request)
                .getTables_infos().stream().map(TTableInfo::getTable_name).collect(Collectors.toList()));
        assertEquals(List.of("t_open"), tables);
    }
}
