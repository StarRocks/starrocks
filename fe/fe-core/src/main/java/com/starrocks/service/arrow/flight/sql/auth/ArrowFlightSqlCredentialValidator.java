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

package com.starrocks.service.arrow.flight.sql.auth;

import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.qe.ConnectContext;
import com.starrocks.service.arrow.flight.sql.session.ArrowFlightSqlTokenManager;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

import java.nio.charset.StandardCharsets;

public class ArrowFlightSqlCredentialValidator implements BasicCallHeaderAuthenticator.CredentialValidator {

    private final ArrowFlightSqlTokenManager arrowFlightSqlTokenManager;

    public ArrowFlightSqlCredentialValidator(ArrowFlightSqlTokenManager arrowFlightSqlTokenManager) {
        this.arrowFlightSqlTokenManager = arrowFlightSqlTokenManager;
    }

    @Override
    public CallHeaderAuthenticator.AuthResult validate(String username, String password) throws Exception {
        UserIdentity currentUser;
        try {
            currentUser = AuthenticationHandler.authenticate(new ConnectContext(), username, "0.0.0.0",
                    password.getBytes(StandardCharsets.UTF_8), null);
        } catch (AuthenticationException e) {
            throw CallStatus.UNAUTHENTICATED.withDescription("Access denied for " + username).toRuntimeException();
        }

        String encryptedToken = arrowFlightSqlTokenManager.createToken(currentUser);
        return createAuthToken(encryptedToken);
    }

    private CallHeaderAuthenticator.AuthResult createAuthToken(String token) {
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
