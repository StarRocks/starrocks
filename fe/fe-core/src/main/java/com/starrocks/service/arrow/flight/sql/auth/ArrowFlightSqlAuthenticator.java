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

package com.starrocks.service.arrow.flight.sql.auth;

import com.starrocks.service.arrow.flight.sql.session.ArrowFlightSqlTokenManager;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.AuthUtilities;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

public class ArrowFlightSqlAuthenticator implements org.apache.arrow.flight.auth2.CallHeaderAuthenticator {

    private final ArrowFlightSqlTokenManager arrowFlightSqlTokenManager;

    public ArrowFlightSqlAuthenticator(ArrowFlightSqlTokenManager arrowFlightSqlTokenManager) {
        this.arrowFlightSqlTokenManager = arrowFlightSqlTokenManager;
    }

    @Override
    public AuthResult authenticate(CallHeaders incomingHeaders) {
        final String token = AuthUtilities.getValueFromAuthHeader(incomingHeaders,
                Auth2Constants.BEARER_PREFIX);
        if (token == null) {
            BasicCallHeaderAuthenticator basicCallHeaderAuthenticator =
                    new BasicCallHeaderAuthenticator(new ArrowFlightSqlCredentialValidator(arrowFlightSqlTokenManager));
            AuthResult authResult = basicCallHeaderAuthenticator.authenticate(incomingHeaders);
            return authResult;
        }
        return validateToken(token);
    }

    AuthResult validateToken(String token) {
        try {
            arrowFlightSqlTokenManager.validateToken(token);
            return createAuthResult(token);
        } catch (Exception e) {
            throw CallStatus.UNAUTHENTICATED.withCause(e).withDescription(e.getMessage()).toRuntimeException();
        }
    }

    private CallHeaderAuthenticator.AuthResult createAuthResult(String token) {
        return new CallHeaderAuthenticator.AuthResult() {
            @Override
            public String getPeerIdentity() {
                return token;
            }

            @Override
            public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
                outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER,
                        Auth2Constants.BEARER_PREFIX + token);
            }
        };
    }
}

