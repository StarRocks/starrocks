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

import com.starrocks.qe.GlobalVariable;
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

    public AuthResult validateBasicAuth(CallHeaders headers) {
        return this.basicAuthenticator.authenticate(headers);
    }

    AuthResult validateBearerToken(String token) {
        try {
            sessionManager.validateToken(token);
        } catch (IllegalArgumentException e) {
            // Token not found in this FE's local session cache. If proxy mode is enabled and the
            // "FE_HOST|UUID" prefix names a different, known FE, let the call through so it can be
            // forwarded there (see ArrowFlightSqlServiceImpl's forward*ToRemoteFE methods). The
            // forwarded call reuses this exact token as its own bearer credential, so the FE named
            // in the prefix runs this same check against its own cache. A caller can claim any
            // hostname prefix it likes, but it can never forge a cache hit on the FE that prefix
            // names — so isForwardableToAnotherFe() below must return false whenever the prefix
            // names *this* FE, otherwise a token forged with our own host would be wrongly trusted.
            if (isProxyEnabled() && isForwardableToAnotherFe(token)) {
                return createAuthResult(token);
            }
            throw CallStatus.UNAUTHENTICATED.withCause(e).withDescription(e.getMessage()).toRuntimeException();
        }

        return createAuthResult(token);
    }

    private boolean isProxyEnabled() {
        return GlobalVariable.isArrowFlightProxyEnabled();
    }

    /**
     * True only if the token's "FE_HOST|UUID" prefix names a different, known FE in the cluster.
     * A token claiming to belong to this FE must never take this path: since it missed the local
     * cache lookup above, that would mean trusting an unverified, caller-supplied claim as proof
     * of validity on the one FE actually authoritative for it.
     */
    private boolean isForwardableToAnotherFe(String token) {
        if (sessionManager.isLocalToken(token)) {
            return false;
        }
        String feHost = ArrowFlightSqlSessionManager.extractFeHost(token);
        return ArrowFlightSqlSessionManager.isValidFeHost(feHost);
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