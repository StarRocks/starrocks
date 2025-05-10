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

package com.starrocks.service.arrow.flight.sql;

import com.starrocks.authorization.PrivilegeException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectScheduler;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.arrow.flight.sql.session.ArrowFlightSqlSessionManager;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TUniqueId;
import org.apache.arrow.flight.FlightRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ArrowFlightSqlSessionManagerTest {

    private ArrowFlightSqlSessionManager sessionManager;

    private final UserIdentity mockUser = new UserIdentity("testUser", "127.0.0.1");

    private final String mockToken = "mock-token-123";
    private final UUID mockUUID = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    private final TUniqueId mockTUniqueId = new TUniqueId(1L, 2L);
    private ConnectScheduler mockScheduler;
    private ArrowFlightSqlConnectContext mockContext;

    @BeforeEach
    public void setUp() {
        sessionManager = new ArrowFlightSqlSessionManager();
        mockScheduler = mock(ConnectScheduler.class);
        mockContext = mock(ArrowFlightSqlConnectContext.class);
    }

    private void mockGlobalStateMgr(MockedStatic<GlobalStateMgr> mockedGlobalState) {
        GlobalStateMgr mockGlobalState = mock(GlobalStateMgr.class);
        VariableMgr mockVariableMgr = mock(VariableMgr.class);
        SessionVariable mockSessionVariable = mock(SessionVariable.class);
        com.starrocks.authorization.AuthorizationMgr mockAuthMgr = mock(com.starrocks.authorization.AuthorizationMgr.class);

        mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(mockGlobalState);
        when(mockGlobalState.getVariableMgr()).thenReturn(mockVariableMgr);
        when(mockVariableMgr.newSessionVariable()).thenReturn(mockSessionVariable);
        when(mockGlobalState.getAuthorizationMgr()).thenReturn(mockAuthMgr);
        try {
            when(mockAuthMgr.getDefaultRoleIdsByUser(any())).thenReturn(Set.of(1L, 2L, 3L));
        } catch (PrivilegeException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testInitializeSession_success() {
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class);
                MockedStatic<UUIDUtil> mockedUUID = mockStatic(UUIDUtil.class);
                MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class)) {

            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getNextConnectionId()).thenReturn(123);
            when(mockScheduler.registerConnection(any())).thenReturn(Pair.create(true, ""));

            mockedUUID.when(UUIDUtil::genUUID).thenReturn(mockUUID);
            mockedUUID.when(() -> UUIDUtil.toTUniqueId(mockUUID)).thenReturn(mockTUniqueId);

            mockGlobalStateMgr(mockedGlobalState);

            String token = sessionManager.initializeSession(mockUser);
            assertNotNull(token);
        }
    }

    @Test
    public void testInitializeSession_registerConnectionFail() {
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class);
                MockedStatic<UUIDUtil> mockedUUID = mockStatic(UUIDUtil.class);
                MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class)) {

            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getNextConnectionId()).thenReturn(123);
            when(mockScheduler.registerConnection(any())).thenReturn(Pair.create(false, "register failed"));

            mockedUUID.when(UUIDUtil::genUUID).thenReturn(mockUUID);
            mockedUUID.when(() -> UUIDUtil.toTUniqueId(mockUUID)).thenReturn(mockTUniqueId);

            mockGlobalStateMgr(mockedGlobalState);

            assertThrows(FlightRuntimeException.class, () -> sessionManager.initializeSession(mockUser));
        }
    }

    @Test
    public void testValidateToken_success() {
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class);
                MockedStatic<UUIDUtil> mockedUUID = mockStatic(UUIDUtil.class);
                MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class)) {

            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getNextConnectionId()).thenReturn(123);
            when(mockScheduler.registerConnection(any())).thenReturn(Pair.create(true, ""));

            mockedUUID.when(UUIDUtil::genUUID).thenReturn(mockUUID);
            mockedUUID.when(() -> UUIDUtil.toTUniqueId(mockUUID)).thenReturn(mockTUniqueId);

            mockGlobalStateMgr(mockedGlobalState);

            String token = sessionManager.initializeSession(mockUser);
            assertDoesNotThrow(() -> sessionManager.validateToken(token));
        }
    }

    @Test
    public void testValidateToken_emptyOrNull() {
        assertThrows(IllegalArgumentException.class, () -> sessionManager.validateToken(null));
        assertThrows(IllegalArgumentException.class, () -> sessionManager.validateToken(""));
    }

    @Test
    public void testValidateToken_invalidToken() {
        assertThrows(IllegalArgumentException.class, () -> sessionManager.validateToken("non-exist-token"));
    }

    @Test
    public void testCloseSession() {
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class);
                MockedStatic<UUIDUtil> mockedUUID = mockStatic(UUIDUtil.class);
                MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class)) {

            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getNextConnectionId()).thenReturn(123);
            when(mockScheduler.registerConnection(any())).thenReturn(Pair.create(true, ""));

            mockedUUID.when(UUIDUtil::genUUID).thenReturn(mockUUID);
            mockedUUID.when(() -> UUIDUtil.toTUniqueId(mockUUID)).thenReturn(mockTUniqueId);

            mockGlobalStateMgr(mockedGlobalState);

            String token = sessionManager.initializeSession(mockUser);
            sessionManager.validateToken(token);

            sessionManager.closeSession(token);
            assertThrows(IllegalArgumentException.class, () -> sessionManager.validateToken(token));
        }
    }

    @Test
    public void testValidateAndGetConnectContext_success() {
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class)) {
            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getArrowFlightSqlConnectContext(mockToken)).thenReturn(mockContext);

            ArrowFlightSqlConnectContext ctx = sessionManager.validateAndGetConnectContext(mockToken);
            assertNotNull(ctx);
        }
    }

    @Test
    public void testValidateAndGetConnectContext_notFound() {
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class)) {
            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getArrowFlightSqlConnectContext(mockToken)).thenReturn(null);

            assertThrows(FlightRuntimeException.class, () -> sessionManager.validateAndGetConnectContext(mockToken));
        }
    }
}
