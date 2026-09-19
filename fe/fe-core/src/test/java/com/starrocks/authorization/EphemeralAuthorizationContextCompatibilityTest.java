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

package com.starrocks.authorization;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.UserIdentityUtils;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.datacache.DataCacheSelectExecutor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.QueryDetail;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TUserIdentity;
import com.starrocks.thrift.TUserRoles;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Ordinary privilege compatibility checks for derived contexts, with no AI expressions or external services. */
public class EphemeralAuthorizationContextCompatibilityTest {
    private AuthenticationMgr previousAuthenticationMgr;
    private AuthorizationMgr previousAuthorizationMgr;
    private boolean previousCacheEnabled;
    private ConnectContext caller;

    @BeforeEach
    public void setUp() throws Exception {
        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        previousAuthenticationMgr = state.getAuthenticationMgr();
        previousAuthorizationMgr = state.getAuthorizationMgr();
        previousCacheEnabled = Config.authorization_enable_priv_collection_cache;
        Config.authorization_enable_priv_collection_cache = true;
        UtFrameUtils.setUpForPersistTest();
        state.setAuthenticationMgr(new AuthenticationMgr());
        state.setAuthorizationMgr(new AuthorizationMgr(new DefaultAuthorizationProvider()));

        ConnectContext root = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        execute(root, "CREATE ROLE context_compatibility_role");
        execute(root, "GRANT OPERATE ON SYSTEM TO ROLE context_compatibility_role");
        execute(root, "GRANT context_compatibility_role TO EXTERNAL GROUP context_compatibility_group");

        UserIdentity identity = UserIdentity.createEphemeralUserIdent("external_context_user", "%");
        caller = UtFrameUtils.initCtxForNewPrivilege(identity);
        caller.setQuerySource(QueryDetail.QuerySource.EXTERNAL);
        caller.setGroups(Set.of("context_compatibility_group"));
        // Use the same role derivation as AuthenticationHandler, replacing the helper's root role.
        caller.setCurrentRoleIds(identity, caller.getGroups());
        assertTrue(caller.getCurrentUserIdentity().isEphemeral());
        assertFalse(caller.getCurrentRoleIds().isEmpty());
        assertDoesNotThrow(() -> Authorizer.checkSystemAction(caller, PrivilegeType.OPERATE),
                "The parent principal must have the ordinary privilege through its current group mapping");
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        state.setAuthenticationMgr(previousAuthenticationMgr);
        state.setAuthorizationMgr(previousAuthorizationMgr);
        Config.authorization_enable_priv_collection_cache = previousCacheEnabled;
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testSynchronousCacheSelectContextPreservesOrdinaryGroupPrivilege() {
        DataCacheSelectStatement statement = (DataCacheSelectStatement) SqlParser.parse(
                "CACHE SELECT * FROM context_compatibility_table", caller.getSessionVariable()).get(0);
        statement.setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);

        ConnectContext derived = DataCacheSelectExecutor.buildCacheSelectConnectContext(statement, caller, true);

        assertEquals(QueryDetail.QuerySource.EXTERNAL, derived.getQuerySource());
        assertTrue(derived.getCurrentUserIdentity().isEphemeral());
        assertEquals(caller.getCurrentRoleIds(), derived.getCurrentRoleIds());
        assertDoesNotThrow(() -> Authorizer.checkSystemAction(derived, PrivilegeType.OPERATE),
                "A synchronous cache-select child must retain its principal's ordinary group privilege");
    }

    @Test
    public void testSchemaScannerIdentityRoundTripPreservesOrdinaryGroupPrivilege() {
        // SchemaScanNode sends this identity and role list; BE schema scanners pass the identity back to FE.
        TUserIdentity forwarded = UserIdentityUtils.toThrift(caller.getCurrentUserIdentity());
        forwarded.setCurrent_role_ids(new TUserRoles().setRole_id_list(new ArrayList<>(caller.getCurrentRoleIds())));
        ConnectContext restored = new ConnectContext();
        restored.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        UserIdentityUtils.setAuthInfoFromThrift(restored, forwarded);

        assertTrue(restored.getCurrentUserIdentity().isEphemeral());
        assertEquals(caller.getCurrentRoleIds(), restored.getCurrentRoleIds());
        assertDoesNotThrow(() -> Authorizer.checkSystemAction(restored, PrivilegeType.OPERATE),
                "A schema-scanner identity round trip must retain ordinary group privileges");
    }

    private static void execute(ConnectContext context, String sql) throws Exception {
        try (var ignored = context.bindScope()) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, context), context);
        }
    }
}
