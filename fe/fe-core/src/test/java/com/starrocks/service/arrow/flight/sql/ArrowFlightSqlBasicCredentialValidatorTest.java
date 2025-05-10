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

import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.qe.ConnectContext;
import com.starrocks.service.arrow.flight.sql.auth2.ArrowFlightSqlBasicCredentialValidator;
import com.starrocks.service.arrow.flight.sql.session.ArrowFlightSqlSessionManager;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ArrowFlightSqlBasicCredentialValidatorTest {

    @Test
    void testValidate_success() throws Exception {
        ArrowFlightSqlSessionManager sessionManager = mock(ArrowFlightSqlSessionManager.class);
        ArrowFlightSqlBasicCredentialValidator validator = new ArrowFlightSqlBasicCredentialValidator(sessionManager);

        UserIdentity mockUser = mock(UserIdentity.class);
        when(sessionManager.initializeSession(mockUser)).thenReturn("token-123");

        try (MockedStatic<AuthenticationHandler> mocked = mockStatic(AuthenticationHandler.class)) {
            mocked.when(() -> AuthenticationHandler
                            .authenticate(any(ConnectContext.class), eq("user"), eq("0.0.0.0"), any()))
                    .thenReturn(mockUser);

            CallHeaderAuthenticator.AuthResult result = validator.validate("user", "pass");

            assertEquals("token-123", result.getPeerIdentity());

            CallHeaders headers = mock(CallHeaders.class);
            result.appendToOutgoingHeaders(headers);
            verify(headers).insert(Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + "token-123");
        }
    }

    @Test
    void testValidate_failure() {
        ArrowFlightSqlSessionManager sessionManager = mock(ArrowFlightSqlSessionManager.class);
        ArrowFlightSqlBasicCredentialValidator validator = new ArrowFlightSqlBasicCredentialValidator(sessionManager);

        try (MockedStatic<AuthenticationHandler> mocked = mockStatic(AuthenticationHandler.class)) {
            mocked.when(() -> AuthenticationHandler
                            .authenticate(any(ConnectContext.class), eq("user"), eq("0.0.0.0"), any()))
                    .thenThrow(new AuthenticationException("bad credentials"));

            RuntimeException ex = assertThrows(RuntimeException.class, () -> validator.validate("user", "pass"));
            assertTrue(ex.getMessage().contains("Access denied"));
        }
    }
}
