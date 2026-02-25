// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.service.arrow.flight.sql;

import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectScheduler;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.arrow.flight.sql.session.ArrowFlightSqlSessionManager;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TUniqueId;
import org.apache.arrow.flight.FlightRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ArrowFlightSqlSessionManagerTest {

    private ArrowFlightSqlSessionManager sessionManager;
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
        com.starrocks.authorization.AuthorizationMgr mockAuthMgr =
                mock(com.starrocks.authorization.AuthorizationMgr.class);

        mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(mockGlobalState);
        when(mockGlobalState.getVariableMgr()).thenReturn(mockVariableMgr);
        when(mockVariableMgr.newSessionVariable()).thenReturn(mockSessionVariable);
        when(mockGlobalState.getAuthorizationMgr()).thenReturn(mockAuthMgr);
        try {
            when(mockAuthMgr.getDefaultRoleIdsByUser(any())).thenReturn(Set.of(1L, 2L, 3L));
        } catch (PrivilegeException e) {
            throw new RuntimeException(e);
        }

        NodeMgr mockNodeMgr = mock(NodeMgr.class);
        when(mockGlobalState.getNodeMgr()).thenReturn(mockNodeMgr);
        when(mockNodeMgr.getSelfNode()).thenReturn(Pair.create("localhost", 9010));
    }

    private void mockAuthentication(MockedStatic<AuthenticationHandler> mockedAuth) {
        mockedAuth.when(() -> AuthenticationHandler.authenticate(any(), any(), any(), any()))
                .thenAnswer(invocation -> {
                    ConnectContext ctx = invocation.getArgument(0);
                    ctx.setCurrentUserIdentity(null);
                    ctx.setQualifiedUser("testUser");
                    return null;
                });
    }

    @Test
    public void testInitializeSession_success() {
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class);
                MockedStatic<UUIDUtil> mockedUUID = mockStatic(UUIDUtil.class);
                MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class);
                MockedStatic<AuthenticationHandler> mockedAuth = mockStatic(AuthenticationHandler.class)) {

            mockAuthentication(mockedAuth);

            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getNextConnectionId()).thenReturn(123);
            when(mockScheduler.registerConnection(any())).thenReturn(Pair.create(true, ""));

            mockedUUID.when(UUIDUtil::genUUID).thenReturn(mockUUID);
            mockedUUID.when(() -> UUIDUtil.toTUniqueId(mockUUID)).thenReturn(mockTUniqueId);

            mockGlobalStateMgr(mockedGlobalState);

            String token = sessionManager.initializeSession("testUser", "127.0.0.1", "testPassword");
            assertNotNull(token);
        }
    }

    @Test
    public void testInitializeSession_registerConnectionFail() {
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class);
                MockedStatic<UUIDUtil> mockedUUID = mockStatic(UUIDUtil.class);
                MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class);
                MockedStatic<AuthenticationHandler> mockedAuth = mockStatic(AuthenticationHandler.class)) {

            mockAuthentication(mockedAuth);

            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getNextConnectionId()).thenReturn(123);
            when(mockScheduler.registerConnection(any())).thenReturn(Pair.create(false, "register failed"));

            mockedUUID.when(UUIDUtil::genUUID).thenReturn(mockUUID);
            mockedUUID.when(() -> UUIDUtil.toTUniqueId(mockUUID)).thenReturn(mockTUniqueId);

            mockGlobalStateMgr(mockedGlobalState);

            assertThrows(FlightRuntimeException.class, () -> sessionManager
                    .initializeSession("testUser", "127.0.0.1", "testPassword"));
        }
    }

    @Test
    public void testValidateToken() {
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class);
                MockedStatic<UUIDUtil> mockedUUID = mockStatic(UUIDUtil.class);
                MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class);
                MockedStatic<AuthenticationHandler> mockedAuth = mockStatic(AuthenticationHandler.class)) {

            mockAuthentication(mockedAuth);

            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getNextConnectionId()).thenReturn(123);
            when(mockScheduler.registerConnection(any())).thenReturn(Pair.create(true, ""));

            mockedUUID.when(UUIDUtil::genUUID).thenReturn(mockUUID);
            mockedUUID.when(() -> UUIDUtil.toTUniqueId(mockUUID)).thenReturn(mockTUniqueId);

            mockGlobalStateMgr(mockedGlobalState);

            String token = sessionManager.initializeSession("testUser", "127.0.0.1", "testPassword");
            assertDoesNotThrow(() -> sessionManager.validateToken(token));

            assertThrows(IllegalArgumentException.class, () -> sessionManager.validateToken(null));
            assertThrows(IllegalArgumentException.class, () -> sessionManager.validateToken(""));
            assertThrows(IllegalArgumentException.class, () -> sessionManager.validateToken("non-exist-token"));
        }
    }

    @Test
    public void testCloseSession() {
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class);
                MockedStatic<UUIDUtil> mockedUUID = mockStatic(UUIDUtil.class);
                MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class);
                MockedStatic<AuthenticationHandler> mockedAuth = mockStatic(AuthenticationHandler.class)) {

            mockAuthentication(mockedAuth);

            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getNextConnectionId()).thenReturn(123);
            when(mockScheduler.registerConnection(any())).thenReturn(Pair.create(true, ""));

            mockedUUID.when(UUIDUtil::genUUID).thenReturn(mockUUID);
            mockedUUID.when(() -> UUIDUtil.toTUniqueId(mockUUID)).thenReturn(mockTUniqueId);

            mockGlobalStateMgr(mockedGlobalState);

            String token = sessionManager.initializeSession("testUser", "127.0.0.1", "testPassword");
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

    @Test
    public void testExtractFeHost() {
        assertEquals("fe1.example.com",
                ArrowFlightSqlSessionManager.extractFeHost("fe1.example.com|123e4567-e89b-12d3-a456-426614174000"));
        assertEquals("127.0.0.1",
                ArrowFlightSqlSessionManager.extractFeHost("127.0.0.1|123e4567-e89b-12d3-a456-426614174000"));
        assertEquals("2001:db8::1",
                ArrowFlightSqlSessionManager.extractFeHost("2001:db8::1|123e4567-e89b-12d3-a456-426614174000"));
        assertNull(ArrowFlightSqlSessionManager.extractFeHost("123e4567-e89b-12d3-a456-426614174000"));
        assertNull(ArrowFlightSqlSessionManager.extractFeHost("simpletoken"));
        assertNull(ArrowFlightSqlSessionManager.extractFeHost(""));
        assertNull(ArrowFlightSqlSessionManager.extractFeHost(null));
    }

    @Test
    public void testIsLocalToken_proxyDisabled() {
        try (MockedStatic<GlobalVariable> mockedGlobalVar = mockStatic(GlobalVariable.class)) {
            mockedGlobalVar.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(false);

            // When proxy is disabled, all tokens are considered local
            assertTrue(sessionManager.isLocalToken("any-token"));
            assertTrue(sessionManager.isLocalToken("fe1.example.com|some-uuid"));
        }
    }

    @Test
    public void testIsLocalToken_proxyEnabled() {
        try (MockedStatic<GlobalVariable> mockedGlobalVar = mockStatic(GlobalVariable.class);
                MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class)) {

            mockedGlobalVar.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);

            GlobalStateMgr mockGlobalState = mock(GlobalStateMgr.class);
            NodeMgr mockNodeMgr = mock(NodeMgr.class);
            mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(mockGlobalState);
            when(mockGlobalState.getNodeMgr()).thenReturn(mockNodeMgr);
            when(mockNodeMgr.getSelfNode()).thenReturn(Pair.create("127.0.0.1", 9010));

            assertTrue(sessionManager.isLocalToken("127.0.0.1|123e4567-e89b-12d3-a456-426614174000"));
            assertFalse(sessionManager.isLocalToken("127.0.0.2|123e4567-e89b-12d3-a456-426614174000"));
            assertTrue(sessionManager.isLocalToken("simpletoken"));
        }
    }

    @Test
    public void testIsValidFeHost() {
        try (MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr mockGlobalState = mock(GlobalStateMgr.class);
            NodeMgr mockNodeMgr = mock(NodeMgr.class);
            Frontend mockFe1 = mock(Frontend.class);
            Frontend mockFe2 = mock(Frontend.class);
            Frontend mockFe3 = mock(Frontend.class);

            mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(mockGlobalState);
            when(mockGlobalState.getNodeMgr()).thenReturn(mockNodeMgr);
            when(mockNodeMgr.getFrontends(null)).thenReturn(List.of(mockFe1, mockFe2, mockFe3));
            when(mockFe1.getHost()).thenReturn("127.0.0.1");
            when(mockFe2.getHost()).thenReturn("127.0.0.2");
            when(mockFe3.getHost()).thenReturn("127.0.0.3");

            assertTrue(ArrowFlightSqlSessionManager.isValidFeHost("127.0.0.1"));
            assertTrue(ArrowFlightSqlSessionManager.isValidFeHost("127.0.0.2"));
            assertTrue(ArrowFlightSqlSessionManager.isValidFeHost("127.0.0.3"));
            assertFalse(ArrowFlightSqlSessionManager.isValidFeHost("malicious.host.com"));
            assertFalse(ArrowFlightSqlSessionManager.isValidFeHost("127.0.0.99"));
            assertFalse(ArrowFlightSqlSessionManager.isValidFeHost(""));
            assertFalse(ArrowFlightSqlSessionManager.isValidFeHost(null));
        }
    }

    @Test
    public void testInitializeSession_tokenFormat() {
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class);
                MockedStatic<UUIDUtil> mockedUUID = mockStatic(UUIDUtil.class);
                MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class);
                MockedStatic<GlobalVariable> mockedGlobalVar = mockStatic(GlobalVariable.class);
                MockedStatic<AuthenticationHandler> mockedAuth = mockStatic(AuthenticationHandler.class)) {

            mockAuthentication(mockedAuth);

            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getNextConnectionId()).thenReturn(123);
            when(mockScheduler.registerConnection(any())).thenReturn(Pair.create(true, ""));

            mockedUUID.when(UUIDUtil::genUUID).thenReturn(mockUUID);
            mockedUUID.when(() -> UUIDUtil.toTUniqueId(mockUUID)).thenReturn(mockTUniqueId);

            GlobalStateMgr mockGlobalState = mock(GlobalStateMgr.class);
            NodeMgr mockNodeMgr = mock(NodeMgr.class);
            VariableMgr mockVariableMgr = mock(VariableMgr.class);
            SessionVariable mockSessionVariable = mock(SessionVariable.class);
            com.starrocks.authorization.AuthorizationMgr mockAuthMgr =
                    mock(com.starrocks.authorization.AuthorizationMgr.class);

            mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(mockGlobalState);
            when(mockGlobalState.getNodeMgr()).thenReturn(mockNodeMgr);
            when(mockNodeMgr.getSelfNode()).thenReturn(Pair.create("127.0.0.1", 9010));
            when(mockGlobalState.getVariableMgr()).thenReturn(mockVariableMgr);
            when(mockVariableMgr.newSessionVariable()).thenReturn(mockSessionVariable);
            when(mockGlobalState.getAuthorizationMgr()).thenReturn(mockAuthMgr);
            try {
                when(mockAuthMgr.getDefaultRoleIdsByUser(any())).thenReturn(Set.of(1L, 2L, 3L));
            } catch (PrivilegeException e) {
                throw new RuntimeException(e);
            }

            // Test with proxy enabled - token includes FE host prefix
            mockedGlobalVar.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);
            String tokenWithProxy = sessionManager.initializeSession("testUser", "127.0.0.1", "testPassword");
            assertNotNull(tokenWithProxy);
            assertTrue(tokenWithProxy.startsWith("127.0.0.1|"), "Token should include FE host prefix");
            assertTrue(tokenWithProxy.contains(mockUUID.toString()), "Token should contain UUID");

            // Test with proxy disabled - token is plain UUID
            mockedGlobalVar.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(false);
            String tokenWithoutProxy = sessionManager.initializeSession("testUser", "127.0.0.1", "testPassword");
            assertNotNull(tokenWithoutProxy);
            assertEquals(mockUUID.toString(), tokenWithoutProxy, "Token should be plain UUID when proxy disabled");
        }
    }
}
