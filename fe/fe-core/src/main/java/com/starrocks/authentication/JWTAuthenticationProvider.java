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

package com.starrocks.authentication;

import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jwt.SignedJWT;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.mysql.MysqlCodec;
import com.starrocks.server.GlobalStateMgr;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;

public class JWTAuthenticationProvider implements AuthenticationProvider {
    public static final String JWT_JWKS_URL = "jwks_url";
    public static final String JWT_PRINCIPAL_FIELD = "principal_field";
    public static final String JWT_REQUIRED_ISSUER = "required_issuer";
    public static final String JWT_REQUIRED_AUDIENCE = "required_audience";

    private final String jwksUrl;
    private final String principalFiled;
    private final String[] requiredIssuer;
    private final String[] requiredAudience;

    public JWTAuthenticationProvider(String jwksUrl, String principalFiled,
                                     String[] requiredIssuer, String[] requiredAudience) {
        this.jwksUrl = jwksUrl;
        this.principalFiled = principalFiled;
        this.requiredIssuer = requiredIssuer;
        this.requiredAudience = requiredAudience;
    }

    @Override
    public void authenticate(AccessControlContext authContext, UserIdentity userIdentity, byte[] authResponse)
            throws AuthenticationException {
        String idToken;
        try {
            ByteBuffer authBuffer = ByteBuffer.wrap(authResponse);
            //1 Byte for capability mysql client
            MysqlCodec.readInt1(authBuffer);
            idToken = new String(MysqlCodec.readLenEncodedString(authBuffer));
        } catch (Exception e) {
            throw new AuthenticationException(e.getMessage());
        }
        authenticateRawToken(authContext, userIdentity, idToken);
    }

    /**
     * Authenticate using a raw token string presented directly by a non-MySQL-wire client (e.g. Arrow
     * Flight SQL), bypassing the MySQL auth-response byte framing used by {@link #authenticate}.
     *
     * @param authContext  authentication context
     * @param userIdentity best matched user
     * @param rawToken     the raw JWT string presented by the client
     * @throws AuthenticationException when authentication fails
     */
    public void authenticateRawToken(AccessControlContext authContext, UserIdentity userIdentity, String rawToken)
            throws AuthenticationException {
        try {
            JWKSet jwkSet = GlobalStateMgr.getCurrentState().getJwkMgr().getJwkSet(jwksUrl);
            OpenIdConnectVerifier.verify(rawToken, userIdentity.getUser(), jwkSet, principalFiled, requiredIssuer,
                    requiredAudience);
            authContext.setAuthToken(rawToken);
        } catch (Exception e) {
            throw new AuthenticationException(e.getMessage());
        }
    }

    /**
     * Read the iss claim without verifying the signature, to pick the matching integration before a JWKS fetch.
     *
     * @param rawToken the raw JWT string
     * @return the issuer claim, or null if the token has none
     * @throws AuthenticationException if the token cannot be parsed as a JWT
     */
    public static String extractIssuerWithoutVerification(String rawToken) throws AuthenticationException {
        try {
            SignedJWT signedJWT = SignedJWT.parse(rawToken);
            return signedJWT.getJWTClaimsSet().getIssuer();
        } catch (ParseException e) {
            throw new AuthenticationException("Failed to parse token as JWT: " + e.getMessage());
        }
    }

    /**
     * Same issuer rule as {@link OpenIdConnectVerifier}, used to skip non-matching integrations before
     * fetching their JWKS.
     */
    public boolean isIssuerAccepted(String issuer) {
        return requiredIssuer == null || requiredIssuer.length == 0 || Arrays.asList(requiredIssuer).contains(issuer);
    }

    /**
     * Read the exp claim without verifying the signature. Only call this on a token that has already been verified.
     *
     * @return the expiration time, or null if the token has none
     */
    public static Date extractExpirationTimeWithoutVerification(String rawToken) throws AuthenticationException {
        try {
            return SignedJWT.parse(rawToken).getJWTClaimsSet().getExpirationTime();
        } catch (ParseException e) {
            throw new AuthenticationException("Failed to parse token as JWT: " + e.getMessage());
        }
    }
}
