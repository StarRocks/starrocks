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
    void testValidateBearerToken_unknownTokenRejectedEvenWhenProxyEnabledAndClaimedFeIsValid() {
        ArrowFlightSqlSessionManager sessionManager = mock(ArrowFlightSqlSessionManager.class);
        ArrowFlightSqlAuthenticator authenticator = new ArrowFlightSqlAuthenticator(sessionManager);

        // Token claims to be from a valid remote FE, but is not found in the local session cache.
        // The "FE_HOST|UUID" prefix is caller-supplied and unauthenticated, so it must never be
        // trusted as proof the token is valid on that host - the token must always be rejected.
        String remoteToken = "10.0.6.7|some-uuid";
        doThrow(new IllegalArgumentException("Invalid token")).when(sessionManager).validateToken(remoteToken);

        try (MockedStatic<GlobalVariable> mockedGlobalVar = mockStatic(GlobalVariable.class)) {
            mockedGlobalVar.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);

            CallHeaders headers = mock(CallHeaders.class);
            when(headers.get(Auth2Constants.AUTHORIZATION_HEADER)).thenReturn(Auth2Constants.BEARER_PREFIX + remoteToken);

            assertThrows(RuntimeException.class, () -> authenticator.authenticate(headers));
        }
    }
}
