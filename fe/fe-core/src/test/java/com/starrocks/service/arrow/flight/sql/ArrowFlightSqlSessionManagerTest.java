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

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.starrocks.authentication.AuthCredentials;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.jmockit.Deencapsulation;
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
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
        mockAuthentication(mockedAuth, null);
    }

    private void mockAuthentication(MockedStatic<AuthenticationHandler> mockedAuth,
                                    AtomicReference<ArrowFlightSqlConnectContext> contextRef) {
        // Every entry point goes through authenticate(ctx, AuthCredentials) now; a stub left on an
        // old entry silently returns null (mockStatic default) instead of running this answer, which
        // shows up as an NPE in assertAllocatorClosed rather than as an authentication problem.
        mockedAuth.when(() -> AuthenticationHandler.authenticate(any(ConnectContext.class), any(AuthCredentials.class)))
                .thenAnswer(invocation -> {
                    ConnectContext ctx = invocation.getArgument(0);
                    ctx.setCurrentUserIdentity(null);
                    ctx.setQualifiedUser("testUser");
                    if (contextRef != null) {
                        contextRef.set((ArrowFlightSqlConnectContext) ctx);
                    }
                    return null;
                });
    }

    private void assertAllocatorClosed(ArrowFlightSqlConnectContext context) {
        BufferAllocator allocator = Deencapsulation.getField(context, "allocator");
        assertThrows(IllegalStateException.class, allocator::assertOpen);
    }

    @Test
    public void testInitializeSession_success() throws Exception {
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
    public void testInitializeSession_registerConnectionFail() throws Exception {
        AtomicReference<ArrowFlightSqlConnectContext> contextRef = new AtomicReference<>();
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class);
                MockedStatic<UUIDUtil> mockedUUID = mockStatic(UUIDUtil.class);
                MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class);
                MockedStatic<GlobalVariable> mockedGlobalVar = mockStatic(GlobalVariable.class);
                MockedStatic<AuthenticationHandler> mockedAuth = mockStatic(AuthenticationHandler.class)) {

            mockAuthentication(mockedAuth, contextRef);

            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getNextConnectionId()).thenReturn(123);
            when(mockScheduler.registerConnection(any())).thenReturn(Pair.create(false, "register failed"));

            mockedUUID.when(UUIDUtil::genUUID).thenReturn(mockUUID);
            mockedUUID.when(() -> UUIDUtil.toTUniqueId(mockUUID)).thenReturn(mockTUniqueId);
            mockedGlobalVar.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(false);

            mockGlobalStateMgr(mockedGlobalState);

            assertThrows(FlightRuntimeException.class, () -> sessionManager
                    .initializeSession("testUser", "127.0.0.1", "testPassword"));
            assertThrows(IllegalArgumentException.class,
                    () -> sessionManager.validateToken(mockUUID.toString()));
            assertAllocatorClosed(contextRef.get());
        }
    }

    @Test
    public void testInitializeSession_connectionIdExhausted() throws Exception {
        AtomicReference<ArrowFlightSqlConnectContext> contextRef = new AtomicReference<>();
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class);
                MockedStatic<UUIDUtil> mockedUUID = mockStatic(UUIDUtil.class);
                MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class);
                MockedStatic<GlobalVariable> mockedGlobalVar = mockStatic(GlobalVariable.class);
                MockedStatic<AuthenticationHandler> mockedAuth = mockStatic(AuthenticationHandler.class)) {

            mockAuthentication(mockedAuth, contextRef);

            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getNextConnectionId()).thenThrow(
                    new ConnectScheduler.ConnectionIdExhaustedException("No available connection ID"));

            mockedUUID.when(UUIDUtil::genUUID).thenReturn(mockUUID);
            mockedUUID.when(() -> UUIDUtil.toTUniqueId(mockUUID)).thenReturn(mockTUniqueId);
            mockedGlobalVar.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(false);

            mockGlobalStateMgr(mockedGlobalState);

            FlightRuntimeException error = assertThrows(FlightRuntimeException.class, () -> sessionManager
                    .initializeSession("testUser", "127.0.0.1", "testPassword"));
            assertEquals(CallStatus.RESOURCE_EXHAUSTED.code(), error.status().code());
            assertThrows(IllegalArgumentException.class,
                    () -> sessionManager.validateToken(mockUUID.toString()));
            assertAllocatorClosed(contextRef.get());
        }
    }

    @Test
    public void testValidateToken() throws Exception {
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
    public void testCloseSession() throws Exception {
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
    public void testInitializeSession_tokenFormat() throws Exception {
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

    // ---- JWT sessions: opened like Basic ones, released like Basic ones, and never outliving the JWT.
    // The handler is mocked, so these pin the session rules, not the verification.

    private interface SessionEnvBody {
        void run(MockedStatic<AuthenticationHandler> mockedAuth) throws Exception;
    }

    /** Everything initializeSession needs, with a real UUID per session so two sessions differ. */
    private void withSessionEnv(SessionEnvBody body) throws Exception {
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class);
                MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class);
                MockedStatic<AuthenticationHandler> mockedAuth = mockStatic(AuthenticationHandler.class)) {
            mockAuthentication(mockedAuth);
            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getNextConnectionId()).thenReturn(123);
            when(mockScheduler.registerConnection(any())).thenReturn(Pair.create(true, ""));
            mockGlobalStateMgr(mockedGlobalState);
            body.run(mockedAuth);
        }
    }

    /** A well-formed JWT; the signature is irrelevant because the handler is mocked. */
    private static String jwt(Date exp) throws Exception {
        JWTClaimsSet.Builder claims = new JWTClaimsSet.Builder().issuer("https://idp.test").subject("alice");
        if (exp != null) {
            claims.expirationTime(exp);
        }
        SignedJWT signed = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims.build());
        signed.sign(new MACSigner(new byte[32]));
        return signed.serialize();
    }

    @Test
    public void testEachJwtAuthenticationOpensItsOwnSessionLikeBasic() throws Exception {
        // Symmetric with Basic: a connection that presents a JWT gets its own session, and a second
        // connection with the same JWT gets another. Sessions are per connection, not per token, so
        // USE / SET on one connection never leak into another.
        withSessionEnv(mockedAuth -> {
            String token = jwt(new Date(System.currentTimeMillis() + 60_000));
            String first = sessionManager.initializeSessionWithJwt(token, "0.0.0.0");
            String second = sessionManager.initializeSessionWithJwt(token, "0.0.0.0");
            assertNotEquals(first, second);
            verify(mockScheduler, times(2)).registerConnection(any());
            assertDoesNotThrow(() -> sessionManager.validateToken(first));
            assertDoesNotThrow(() -> sessionManager.validateToken(second));
            // ... and each is released on its own CloseSession.
            sessionManager.closeSession(first);
            assertThrows(IllegalArgumentException.class, () -> sessionManager.validateToken(first));
            assertDoesNotThrow(() -> sessionManager.validateToken(second));
        });
    }

    @Test
    public void testJwtSessionDoesNotOutliveTheJwt() throws Exception {
        // The client switches to the session token after the first call, so without this the
        // session would run on for arrow_token_cache_expire_second (days) after the JWT expired.
        withSessionEnv(mockedAuth -> {
            // exp is serialized in whole seconds, so pick the second boundary 2 s ahead: the first
            // validation has at least a second of margin, and sleeping past the boundary is deterministic.
            Date exp = new Date((System.currentTimeMillis() / 1000 + 2) * 1000);
            String sessionToken = sessionManager.initializeSessionWithJwt(jwt(exp), "0.0.0.0");
            assertDoesNotThrow(() -> sessionManager.validateToken(sessionToken));
            Thread.sleep(Math.max(0, exp.getTime() - System.currentTimeMillis()) + 200);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> sessionManager.validateToken(sessionToken));
            assertTrue(e.getMessage().contains("expired"), e.getMessage());
            // and it is gone for good, not just refused once
            assertThrows(IllegalArgumentException.class, () -> sessionManager.validateToken(sessionToken));
        });
    }

    @Test
    public void testJwtWithoutExpAndBasicSessionsHaveNoDeadlineOfTheirOwn() throws Exception {
        withSessionEnv(mockedAuth -> {
            String fromJwt = sessionManager.initializeSessionWithJwt(jwt(null), "0.0.0.0");
            String fromBasic = sessionManager.initializeSession("testUser", "127.0.0.1", "testPassword");
            Thread.sleep(50);
            assertDoesNotThrow(() -> sessionManager.validateToken(fromJwt),
                    "no exp means only the configured cache TTL applies");
            assertDoesNotThrow(() -> sessionManager.validateToken(fromBasic));
        });
    }

    @Test
    public void testJwtAuthenticationHonoursTheBearerSwitch() throws Exception {
        // Nothing is cached ahead of the handler, so the kill switch is honoured on every
        // authentication simply because every authentication goes through the handler.
        withSessionEnv(mockedAuth -> {
            mockedAuth.when(() -> AuthenticationHandler.authenticate(any(ConnectContext.class), any(AuthCredentials.class)))
                    .thenAnswer(invocation -> {
                        if (!Config.enable_rest_bearer_authentication) {
                            throw new AuthenticationException("Bearer token authentication is disabled");
                        }
                        ConnectContext ctx = invocation.getArgument(0);
                        ctx.setQualifiedUser("testUser");
                        return null;
                    });
            String token = jwt(new Date(System.currentTimeMillis() + 60_000));
            assertNotNull(sessionManager.initializeSessionWithJwt(token, "0.0.0.0"));
            boolean saved = Config.enable_rest_bearer_authentication;
            Config.enable_rest_bearer_authentication = false;
            try {
                FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                        () -> sessionManager.initializeSessionWithJwt(token, "0.0.0.0"));
                assertEquals(CallStatus.UNAUTHENTICATED.code(), e.status().code());
            } finally {
                Config.enable_rest_bearer_authentication = saved;
            }
        });
    }
}
