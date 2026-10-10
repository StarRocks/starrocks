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

import com.starrocks.qe.GlobalVariable;
import com.starrocks.service.arrow.flight.sql.auth2.ArrowFlightSqlAuthenticator;
import com.starrocks.service.arrow.flight.sql.session.ArrowFlightSqlSessionManager;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ArrowFlightSqlAuthenticatorTest {

    @Test
    void testAuthenticate_withBearerToken() {
        ArrowFlightSqlSessionManager sessionManager = mock(ArrowFlightSqlSessionManager.class);
        ArrowFlightSqlAuthenticator authenticator = new ArrowFlightSqlAuthenticator(sessionManager);

        CallHeaders headers = mock(CallHeaders.class);
        when(headers.get(Auth2Constants.AUTHORIZATION_HEADER)).thenReturn(Auth2Constants.BEARER_PREFIX + "token-123");

        CallHeaderAuthenticator.AuthResult result = authenticator.authenticate(headers);

        assertEquals("token-123", result.getPeerIdentity());

        CallHeaders outgoing = mock(CallHeaders.class);
        result.appendToOutgoingHeaders(outgoing);
        verify(outgoing).insert(Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + "token-123");
    }

    @Test
    void testAuthenticate_withBasicAuth() {
        ArrowFlightSqlSessionManager sessionManager = mock(ArrowFlightSqlSessionManager.class);
        ArrowFlightSqlAuthenticator authenticator = spy(new ArrowFlightSqlAuthenticator(sessionManager));

        CallHeaders headers = mock(CallHeaders.class);
        when(headers.get(Auth2Constants.AUTHORIZATION_HEADER)).thenReturn("Basic abc123");

        CallHeaderAuthenticator.AuthResult mockResult = mock(CallHeaderAuthenticator.AuthResult.class);
        doReturn(mockResult).when(authenticator).validateBasicAuth(headers);

        CallHeaderAuthenticator.AuthResult result = authenticator.authenticate(headers);
        assertEquals(mockResult, result);
    }

    @Test
    void testValidateBearerToken_invalid() {
        ArrowFlightSqlSessionManager sessionManager = mock(ArrowFlightSqlSessionManager.class);
        ArrowFlightSqlAuthenticator authenticator = new ArrowFlightSqlAuthenticator(sessionManager);

        doThrow(new IllegalArgumentException("Invalid token")).when(sessionManager).validateToken("bad-token");

        try (MockedStatic<GlobalVariable> mockedStatic = mockStatic(GlobalVariable.class)) {
            mockedStatic.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(false);

            CallHeaders headers = mock(CallHeaders.class);
            when(headers.get(Auth2Constants.AUTHORIZATION_HEADER)).thenReturn(Auth2Constants.BEARER_PREFIX + "bad-token");

            RuntimeException ex = assertThrows(RuntimeException.class, () -> authenticator.authenticate(headers));
            assertTrue(ex.getMessage().contains("Invalid token"));
        }
    }

    @Test
    void testValidateBearerToken_unknownTokenForwardedWhenProxyEnabledAndClaimedFeIsDifferentAndKnown() {
        ArrowFlightSqlSessionManager sessionManager = mock(ArrowFlightSqlSessionManager.class);
        ArrowFlightSqlAuthenticator authenticator = new ArrowFlightSqlAuthenticator(sessionManager);

        // Token claims to belong to a different, known FE and isn't found in this FE's local
        // cache - i.e. it's genuinely misrouted, not necessarily forged. It must be let through so
        // it can be forwarded to that FE (see ArrowFlightSqlServiceImpl's forward*ToRemoteFE
        // methods), which will run this exact same check against its own cache before trusting it.
        String remoteToken = "10.0.6.7|some-uuid";
        doThrow(new IllegalArgumentException("Invalid token")).when(sessionManager).validateToken(remoteToken);
        when(sessionManager.isLocalToken(remoteToken)).thenReturn(false);

        try (MockedStatic<GlobalVariable> mockedGlobalVar = mockStatic(GlobalVariable.class);
                MockedStatic<ArrowFlightSqlSessionManager> mockedSessionMgr =
                        mockStatic(ArrowFlightSqlSessionManager.class, Mockito.CALLS_REAL_METHODS)) {
            mockedGlobalVar.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);
            mockedSessionMgr.when(() -> ArrowFlightSqlSessionManager.isValidFeHost("10.0.6.7")).thenReturn(true);

            CallHeaders headers = mock(CallHeaders.class);
            when(headers.get(Auth2Constants.AUTHORIZATION_HEADER)).thenReturn(Auth2Constants.BEARER_PREFIX + remoteToken);

            CallHeaderAuthenticator.AuthResult result = authenticator.authenticate(headers);
            assertEquals(remoteToken, result.getPeerIdentity());
        }
    }

    @Test
    void testValidateBearerToken_unknownTokenRejectedWhenClaimedFeIsSelf() {
        ArrowFlightSqlSessionManager sessionManager = mock(ArrowFlightSqlSessionManager.class);
        ArrowFlightSqlAuthenticator authenticator = new ArrowFlightSqlAuthenticator(sessionManager);

        // Token claims to be owned by this very FE (isLocalToken == true) but is missing from
        // this FE's local cache, meaning it is forged rather than merely misrouted. This FE is
        // the one and only authority for tokens naming itself, so it must reject outright and
        // never forward - forwarding here would just be trusting the caller's own claim.
        String forgedToken = "10.0.6.7|forged-uuid";
        doThrow(new IllegalArgumentException("Invalid token")).when(sessionManager).validateToken(forgedToken);
        when(sessionManager.isLocalToken(forgedToken)).thenReturn(true);

        try (MockedStatic<GlobalVariable> mockedGlobalVar = mockStatic(GlobalVariable.class)) {
            mockedGlobalVar.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);

            CallHeaders headers = mock(CallHeaders.class);
            when(headers.get(Auth2Constants.AUTHORIZATION_HEADER)).thenReturn(Auth2Constants.BEARER_PREFIX + forgedToken);

            assertThrows(RuntimeException.class, () -> authenticator.authenticate(headers));
        }
    }

    @Test
    void testValidateBearerToken_unknownTokenRejectedWhenClaimedFeIsUnknown() {
        ArrowFlightSqlSessionManager sessionManager = mock(ArrowFlightSqlSessionManager.class);
        ArrowFlightSqlAuthenticator authenticator = new ArrowFlightSqlAuthenticator(sessionManager);

        // Token claims to be from a host that isn't a known FE in the cluster at all. Even though
        // it's not "local", it must never be forwarded - doing so would let a caller direct this FE
        // to open a connection to an arbitrary/malicious host.
        String maliciousToken = "malicious.host|some-uuid";
        doThrow(new IllegalArgumentException("Invalid token")).when(sessionManager).validateToken(maliciousToken);
        when(sessionManager.isLocalToken(maliciousToken)).thenReturn(false);

        try (MockedStatic<GlobalVariable> mockedGlobalVar = mockStatic(GlobalVariable.class);
                MockedStatic<ArrowFlightSqlSessionManager> mockedSessionMgr =
                        mockStatic(ArrowFlightSqlSessionManager.class, Mockito.CALLS_REAL_METHODS)) {
            mockedGlobalVar.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);
            mockedSessionMgr.when(() -> ArrowFlightSqlSessionManager.isValidFeHost("malicious.host")).thenReturn(false);

            CallHeaders headers = mock(CallHeaders.class);
            when(headers.get(Auth2Constants.AUTHORIZATION_HEADER)).thenReturn(Auth2Constants.BEARER_PREFIX + maliciousToken);

            assertThrows(RuntimeException.class, () -> authenticator.authenticate(headers));
        }
    }
}
