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
     * Three kinds of credential reach this port, and this is the one place that tells them apart.
     * Basic carries a user name and password in the handshake. A <b>JWT</b> is an IdP-issued token
     * this FE only verifies. A <b>session token</b> is the UUID (or {@code FE_HOST|UUID} with the
     * proxy on) this FE issues once a Basic or JWT credential has been accepted. Where a method
     * means either kind of token, the word is plain {@code token}.
     *
     * <ul>
     *   <li> The first request of a connection carries either {@code Basic <Base64(username:password)>}
     *        or {@code Bearer <jwt>}. After successful authentication the FE hands back a session
     *        token in the response headers ({@link AuthResult#appendToOutgoingHeaders}).
     *   <li> All subsequent requests carry {@code Bearer <session token>}. That exchange is Arrow
     *        Flight's own bearer scheme, and the JWT path rides on it exactly as the Basic path does:
     *        one connection, one session, released by CloseSession.
     * </ul>
     *
     * <p>The Bearer value's shape says which it is: a JWT has two dots, a session token has none.
     *
     * @param headers The incoming headers to authenticate.
     * @return The auth result carrying the session token as peer identity.
     */
    @Override
    public AuthResult authenticate(CallHeaders headers) {
        final String bearerValue = AuthUtilities.getValueFromAuthHeader(headers, Auth2Constants.BEARER_PREFIX);
        if (bearerValue == null) {
            return validateBasicAuth(headers);
        }
        if (isJwt(bearerValue)) {
            return validateJwtAuth(bearerValue);
        }
        return validateSessionTokenAuth(bearerValue);
    }

    public AuthResult validateBasicAuth(CallHeaders headers) {
        return this.basicAuthenticator.authenticate(headers);
    }

    /**
     * An IdP-issued JWT: verified by the central handler (issuer routing plus signature check) and
     * exchanged for a session token, the same way the Basic branch exchanges a user name and
     * password for one. The session it opens will not outlive the JWT's {@code exp}.
     *
     * <p>Unlike the Basic branch this does not go through {@code basicAuthenticator}: that exists
     * to pull a user and a password out of the handshake headers, and a JWT is neither.
     */
    AuthResult validateJwtAuth(String jwt) {
        String sessionToken = sessionManager.initializeSessionWithJwt(jwt, "0.0.0.0");
        return createAuthResult(sessionToken);
    }

    /** A session token issued earlier by this FE or, with the proxy on, by another FE. */
    AuthResult validateSessionTokenAuth(String sessionToken) {
        try {
            sessionManager.validateToken(sessionToken);
        } catch (IllegalArgumentException e) {
            // Not found locally - if proxy is enabled and the token names a valid FE, let it through
            // for the request handlers to forward.
            if (isProxyEnabled()) {
                String feHost = ArrowFlightSqlSessionManager.extractFeHost(sessionToken);
                if (feHost != null && ArrowFlightSqlSessionManager.isValidFeHost(feHost)) {
                    return createAuthResult(sessionToken);
                }
            }
            throw CallStatus.UNAUTHENTICATED.withCause(e).withDescription(e.getMessage()).toRuntimeException();
        }

        return createAuthResult(sessionToken);
    }

    private boolean isProxyEnabled() {
        return GlobalVariable.isArrowFlightProxyEnabled();
    }

    // The two shapes a session token can have are both known (ArrowFlightSqlSessionManager
    // #generateToken): a UUID, which has no dots, or "host|uuid" with the proxy on, which has a "|".
    // A JWT in compact serialization is three base64url segments joined by two dots and never
    // contains "|". So: two dots and no "|" is a JWT, anything else is a session token. Counting
    // dots alone was not enough - a host name with two dots (fe1.example.com) sent every request
    // after the handshake down the JWT path, where every user, Basic included, then failed.
    public static boolean isJwt(String bearerValue) {
        return bearerValue != null && bearerValue.indexOf('|') < 0 && bearerValue.split("\\.", -1).length == 3;
    }

    private AuthResult createAuthResult(String sessionToken) {
        return new AuthResult() {
            @Override
            public String getPeerIdentity() {
                return sessionToken;
            }

            @Override
            public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
                outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + sessionToken);
            }
        };
    }
}