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

package com.starrocks.http.rest.context;

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.context.ContextMgr;
import com.starrocks.context.ContextReadExecutor;
import com.starrocks.context.service.ContextQueryService;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class ContextRestReadAuthTest {

    private static ContextMgr mgr;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        mgr = GlobalStateMgr.getCurrentState().getContextMgr();
    }

    @AfterEach
    public void clearContext() {
        ConnectContext.remove();
    }

    @Test
    public void testAuthorizeHistoryEntityHidesMissingAndForeignEntity() throws Exception {
        String owned = "ctx_rest_history_owned";
        String foreign = "ctx_rest_history_foreign";
        mgr.createContextBase(owned, Map.of("_owner_user", "alice@phoenixdata.ai"), false);
        mgr.createContextBase(foreign, Map.of("_owner_user", "bob@phoenixdata.ai"), false);
        try {
            long ownedId = mgr.getContextBase(owned).getId();
            long foreignId = mgr.getContextBase(foreign).getId();
            StubReadExecutor reader = new StubReadExecutor(Map.of(11L, ownedId, 22L, foreignId));
            ephemeralCtx("alice@phoenixdata.ai", "10.0.0.1");

            Assertions.assertEquals(owned,
                    ContextRestReadAuth.authorizeHistoryEntity(mgr, reader, 11L).getName());

            AccessDeniedException missing = Assertions.assertThrows(AccessDeniedException.class,
                    () -> ContextRestReadAuth.authorizeHistoryEntity(mgr, reader, 999L));
            Assertions.assertEquals(ContextRestReadAuth.HIDDEN_ENTITY_MESSAGE, missing.getMessage());

            AccessDeniedException foreignDenied = Assertions.assertThrows(AccessDeniedException.class,
                    () -> ContextRestReadAuth.authorizeHistoryEntity(mgr, reader, 22L));
            Assertions.assertEquals(ContextRestReadAuth.HIDDEN_ENTITY_MESSAGE, foreignDenied.getMessage());
        } finally {
            mgr.dropContextBase(owned, true);
            mgr.dropContextBase(foreign, true);
        }
    }

    @Test
    public void testAuthorizeGetRequestHidesMissingAndForeignIdWithinDeclaredBase() throws Exception {
        String owned = "ctx_rest_get_owned";
        String foreign = "ctx_rest_get_foreign";
        mgr.createContextBase(owned, Map.of("_owner_user", "alice@phoenixdata.ai"), false);
        mgr.createContextBase(foreign, Map.of("_owner_user", "bob@phoenixdata.ai"), false);
        try {
            long ownedId = mgr.getContextBase(owned).getId();
            long foreignId = mgr.getContextBase(foreign).getId();
            StubReadExecutor reader = new StubReadExecutor(Map.of(11L, ownedId, 22L, foreignId));
            ephemeralCtx("alice@phoenixdata.ai", "10.0.0.2");

            ContextQueryService.ReadRequest ownRequest = new ContextQueryService.ReadRequest();
            ownRequest.contextBase = owned;
            ownRequest.id = 11L;
            Assertions.assertDoesNotThrow(() -> ContextRestReadAuth.authorizeGetRequest(mgr, reader, ownRequest));

            ContextQueryService.ReadRequest missingRequest = new ContextQueryService.ReadRequest();
            missingRequest.contextBase = owned;
            missingRequest.id = 999L;
            AccessDeniedException missing = Assertions.assertThrows(AccessDeniedException.class,
                    () -> ContextRestReadAuth.authorizeGetRequest(mgr, reader, missingRequest));
            Assertions.assertEquals(ContextRestReadAuth.HIDDEN_ENTITY_MESSAGE, missing.getMessage());

            ContextQueryService.ReadRequest foreignRequest = new ContextQueryService.ReadRequest();
            foreignRequest.contextBase = owned;
            foreignRequest.id = 22L;
            AccessDeniedException foreignDenied = Assertions.assertThrows(AccessDeniedException.class,
                    () -> ContextRestReadAuth.authorizeGetRequest(mgr, reader, foreignRequest));
            Assertions.assertEquals(ContextRestReadAuth.HIDDEN_ENTITY_MESSAGE, foreignDenied.getMessage());
        } finally {
            mgr.dropContextBase(owned, true);
            mgr.dropContextBase(foreign, true);
        }
    }

    @Test
    public void testAuthorizeGetRequestAllowsUnscopedOwnedId() throws Exception {
        String owned = "ctx_rest_get_unscoped";
        mgr.createContextBase(owned, Map.of("_owner_user", "alice@phoenixdata.ai"), false);
        try {
            long ownedId = mgr.getContextBase(owned).getId();
            StubReadExecutor reader = new StubReadExecutor(Map.of(11L, ownedId));
            ephemeralCtx("alice@phoenixdata.ai", "10.0.0.3");

            ContextQueryService.ReadRequest request = new ContextQueryService.ReadRequest();
            request.id = 11L;
            Assertions.assertDoesNotThrow(() -> ContextRestReadAuth.authorizeGetRequest(mgr, reader, request));
        } finally {
            mgr.dropContextBase(owned, true);
        }
    }

    @Test
    public void testAuthorizeGetRequestRequiresScopeForEntityKeyWithoutGlobalLookup() throws Exception {
        StubReadExecutor reader = new StubReadExecutor(Map.of());
        ephemeralCtx("alice@phoenixdata.ai", "10.0.0.4");

        ContextQueryService.ReadRequest request = new ContextQueryService.ReadRequest();
        request.entityKey = "demo-key";
        AccessDeniedException denied = Assertions.assertThrows(AccessDeniedException.class,
                () -> ContextRestReadAuth.authorizeGetRequest(mgr, reader, request));
        Assertions.assertEquals("\"contextbase\" is required when looking up by entity_key",
                denied.getMessage());
        Assertions.assertEquals(0, reader.resolveEntityIdByKeyCalls);
    }

    private static ConnectContext ephemeralCtx(String principal, String host) {
        ConnectContext ctx = new ConnectContext(null);
        ctx.setCurrentUserIdentity(UserIdentity.createEphemeralUserIdent(principal, host));
        ctx.setCurrentRoleIds(new java.util.HashSet<>());
        ctx.setQualifiedUser(principal);
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx.setThreadLocalInfo();
        return ctx;
    }

    private static final class StubReadExecutor extends ContextReadExecutor {
        private final Map<Long, Long> contextBaseByEntityId;
        private int resolveEntityIdByKeyCalls;

        private StubReadExecutor(Map<Long, Long> contextBaseByEntityId) {
            this.contextBaseByEntityId = new HashMap<>(contextBaseByEntityId);
        }

        @Override
        public long resolveContextBaseIdForEntity(long entityId) {
            return contextBaseByEntityId.getOrDefault(entityId, -1L);
        }

        @Override
        public long resolveEntityIdByKey(String entityKey) {
            resolveEntityIdByKeyCalls++;
            return -1L;
        }
    }
}
