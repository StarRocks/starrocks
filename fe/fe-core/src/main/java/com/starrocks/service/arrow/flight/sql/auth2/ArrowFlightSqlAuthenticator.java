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

package com.starrocks.service.arrow.flight.sql.auth2;

import com.starrocks.service.arrow.flight.sql.session.ArrowFlightSqlSessionManager;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.AuthUtilities;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

public class ArrowFlightSqlAuthenticator implements CallHeaderAuthenticator {

    private final ArrowFlightSqlSessionManager sessionManager;
    private final BasicCallHeaderAuthenticator basicAuthenticator;

    public ArrowFlightSqlAuthenticator(ArrowFlightSqlSessionManager sessionManager) {
        this.sessionManager = sessionManager;
        this.basicAuthenticator = new BasicCallHeaderAuthenticator(new ArrowFlightSqlBasicCredentialValidator(sessionManager));
    }

    /**
     * <ul>
     *   <li> For the first request of a connection, the header includes `Basic <Base64Encode(username:password)>`.
     *        After successful authentication, the FE returns a Bearer token in UUID format,
     *        which serves as the unique identifier for that connection.
     *   <li> All the subsequent requests include the header with `Bearer <token>`.
     * </ul>
     *
     * @param headers The incoming headers to authenticate.
     * @return The auth result with bearer token.
     */
    @Override
    public AuthResult authenticate(CallHeaders headers) {

        final String token = AuthUtilities.getValueFromAuthHeader(headers, Auth2Constants.BEARER_PREFIX);
        if (token == null) {
            return validateBasicAuth(headers);
        } else {
            return validateBearerToken(token);
        }
    }

    AuthResult validateBasicAuth(CallHeaders headers) {
        return this.basicAuthenticator.authenticate(headers);
    }

    AuthResult validateBearerToken(String token) {
        try {
            sessionManager.validateToken(token);
        } catch (IllegalArgumentException e) {
            throw CallStatus.UNAUTHENTICATED.withCause(e).withDescription(e.getMessage()).toRuntimeException();
        }

        return createAuthResult(token);
    }

    private AuthResult createAuthResult(String token) {
        return new AuthResult() {
            @Override
            public String getPeerIdentity() {
                return token;
            }

            @Override
            public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
                outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + token);
            }
        };
    }
}