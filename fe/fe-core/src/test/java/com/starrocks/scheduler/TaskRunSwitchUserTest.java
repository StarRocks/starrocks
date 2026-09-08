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

package com.starrocks.scheduler;

import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.JWTTokenProvider;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskRunSwitchUserTest {

    private MockedStatic<GlobalStateMgr> globalStateMgrStatic;
    private GlobalStateMgr mockGsm;
    private boolean savedCreatorBasedAuth;

    @BeforeEach
    public void setUp() {
        savedCreatorBasedAuth = Config.mv_use_creator_based_authorization;
        globalStateMgrStatic = Mockito.mockStatic(GlobalStateMgr.class, Mockito.CALLS_REAL_METHODS);
        // RETURNS_DEEP_STUBS lets ConnectContext's constructor chain
        // (getVariableMgr().newSessionVariable()) resolve without NPE
        mockGsm = mock(GlobalStateMgr.class, Mockito.RETURNS_DEEP_STUBS);
        globalStateMgrStatic.when(GlobalStateMgr::getCurrentState).thenReturn(mockGsm);
    }

    @AfterEach
    public void tearDown() {
        Config.mv_use_creator_based_authorization = savedCreatorBasedAuth;
        globalStateMgrStatic.close();
    }

    // ---- helpers ----

    private TaskRun buildTaskRun(String user, UserIdentity userIdentity) {
        Task task = new Task("test-task");
        task.setId(1L);
        task.setDefinition("SELECT 1");
        task.setCreateUser(user);
        task.setUserIdentity(userIdentity);
        TaskRun taskRun = new TaskRun();
        taskRun.setTask(task);
        taskRun.initStatus(UUID.randomUUID().toString(), System.currentTimeMillis());
        return taskRun;
    }

    // ---- test cases ----

    @Test
    public void testCreatorBasedDisabledUsesRootNoToken() {
        Config.mv_use_creator_based_authorization = false;
        TaskRun taskRun = buildTaskRun("alice@corp.apple.com", null);

        ConnectContext ctx = new ConnectContext();
        taskRun.switchUser(ctx);

        assertEquals(AuthenticationMgr.ROOT_USER, ctx.getQualifiedUser());
        assertEquals(UserIdentity.ROOT, ctx.getCurrentUserIdentity());
        assertNull(ctx.getAuthToken());
    }

    @Test
    public void testCallerJwtTokenUsesRootWithCallerToken() {
        Config.mv_use_creator_based_authorization = true;
        TaskRun taskRun = buildTaskRun("alice@corp.apple.com", null);
        ConnectContext parentCtx = new ConnectContext();
        parentCtx.setAuthToken("caller-jwt-token-abc");
        taskRun.setConnectContext(parentCtx);

        ConnectContext ctx = new ConnectContext();
        taskRun.switchUser(ctx);

        assertEquals(UserIdentity.ROOT, ctx.getCurrentUserIdentity());
        assertEquals(AuthenticationMgr.ROOT_USER, ctx.getQualifiedUser());
        assertEquals("caller-jwt-token-abc", ctx.getAuthToken());
    }

    @Test
    public void testBotJwtTokenUsesRootWithBotToken() throws AuthenticationException {
        Config.mv_use_creator_based_authorization = true;
        TaskRun taskRun = buildTaskRun("alice@corp.apple.com", null);

        JWTTokenProvider mockProvider = mock(JWTTokenProvider.class);
        when(mockProvider.getToken()).thenReturn("bot-jwt-token-xyz");
        when(mockGsm.getTokenProvider()).thenReturn(mockProvider);

        ConnectContext ctx = new ConnectContext();
        taskRun.switchUser(ctx);

        assertEquals(UserIdentity.ROOT, ctx.getCurrentUserIdentity());
        assertEquals(AuthenticationMgr.ROOT_USER, ctx.getQualifiedUser());
        assertEquals("bot-jwt-token-xyz", ctx.getAuthToken());
    }

    @Test
    public void testNoJwtTokenUsesEphemeralCreatorIdentity() {
        Config.mv_use_creator_based_authorization = true;
        TaskRun taskRun = buildTaskRun("alice@corp.apple.com", null);
        when(mockGsm.getTokenProvider()).thenReturn(null);

        ConnectContext ctx = new ConnectContext();
        taskRun.switchUser(ctx);

        assertEquals("alice@corp.apple.com", ctx.getQualifiedUser());
        assertTrue(ctx.getCurrentUserIdentity().isEphemeral());
        assertNull(ctx.getAuthToken());
    }

    @Test
    public void testNoJwtTokenPersistentCreatorIdentityPreferredOverEphemeral() {
        Config.mv_use_creator_based_authorization = true;
        UserIdentity creatorIdentity = UserIdentity.createAnalyzedUserIdentWithIp("alice@corp.apple.com", "%");
        TaskRun taskRun = buildTaskRun("alice@corp.apple.com", creatorIdentity);
        when(mockGsm.getTokenProvider()).thenReturn(null);

        ConnectContext ctx = new ConnectContext();
        taskRun.switchUser(ctx);

        assertEquals(creatorIdentity, ctx.getCurrentUserIdentity());
    }

    @Test
    public void testBotTokenFetchFailsFallsBackToCreatorIdentity() throws AuthenticationException {
        Config.mv_use_creator_based_authorization = true;
        TaskRun taskRun = buildTaskRun("alice@corp.apple.com", null);

        JWTTokenProvider mockProvider = mock(JWTTokenProvider.class);
        when(mockProvider.getToken()).thenThrow(new AuthenticationException("IAM unavailable"));
        when(mockGsm.getTokenProvider()).thenReturn(mockProvider);

        ConnectContext ctx = new ConnectContext();
        taskRun.switchUser(ctx);

        // token fetch failed — falls back to creator ephemeral identity, no auth token
        assertEquals("alice@corp.apple.com", ctx.getQualifiedUser());
        assertTrue(ctx.getCurrentUserIdentity().isEphemeral());
        assertNull(ctx.getAuthToken());
    }

    @Test
    public void testCallerTokenTakesPriorityOverBotToken() throws AuthenticationException {
        Config.mv_use_creator_based_authorization = true;
        TaskRun taskRun = buildTaskRun("alice@corp.apple.com", null);
        ConnectContext parentCtx = new ConnectContext();
        parentCtx.setAuthToken("caller-token");
        taskRun.setConnectContext(parentCtx);

        JWTTokenProvider mockProvider = mock(JWTTokenProvider.class);
        when(mockProvider.getToken()).thenReturn("bot-token");
        when(mockGsm.getTokenProvider()).thenReturn(mockProvider);

        ConnectContext ctx = new ConnectContext();
        taskRun.switchUser(ctx);

        // caller token wins; bot token should not be fetched
        assertEquals("caller-token", ctx.getAuthToken());
        assertEquals(UserIdentity.ROOT, ctx.getCurrentUserIdentity());
        Mockito.verify(mockProvider, Mockito.never()).getToken();
    }
}
