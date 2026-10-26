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

import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.server.GlobalStateMgr;


public class JWTAuthenticationProvider implements AuthenticationProvider {
    public static final String JWT_JWKS_URL = "jwks_url";
    public static final String JWT_PRINCIPAL_FIELD = "principal_field";
    public static final String JWT_REQUIRED_ISSUER = "required_issuer";
    public static final String JWT_REQUIRED_AUDIENCE = "required_audience";

    private final String jwksUrl;
    private final String principalFiled;
    private final String[] requiredIssuer;
    private final String[] requiredAudience;
    private final GroupSource groupSource;
    private final String groupClaim;
    private final String integrationName;

    public JWTAuthenticationProvider(String jwksUrl, String principalFiled,
                                     String[] requiredIssuer, String[] requiredAudience) {
        this(jwksUrl, principalFiled, requiredIssuer, requiredAudience, GroupSource.GROUP_PROVIDER, null, null);
    }

    public JWTAuthenticationProvider(String jwksUrl, String principalFiled,
                                     String[] requiredIssuer, String[] requiredAudience,
                                     GroupSource groupSource, String groupClaim, String integrationName) {
        this.jwksUrl = jwksUrl;
        this.principalFiled = principalFiled;
        this.requiredIssuer = requiredIssuer;
        this.requiredAudience = requiredAudience;
        this.groupSource = groupSource == null ? GroupSource.GROUP_PROVIDER : groupSource;
        this.groupClaim = groupClaim;
        this.integrationName = integrationName;
    }

    @Override
    public UserIdentity authenticate(AccessControlContext authContext, UserIdentity userIdentity, AuthCredentials cred)
            throws AuthenticationException {
        // One shape only. The MySQL OpenID Connect frame is unwrapped at the entry point
        // (AuthenticationHandler#fromMysqlHandshake), so a MySQL JWT user and an HTTP Bearer
        // request arrive as the same thing here. PLAIN_PASSWORD is refused on purpose: stuffing a
        // JWT into a Basic password slot is an anti-pattern.
        if (cred.kind() != AuthCredentialKind.BEARER_TOKEN) {
            throw new AuthenticationException(
                    "JWT user must authenticate via Bearer token, not " + cred.kind());
        }
        String idToken = cred.tokenString();

        // userName source is asymmetric: MySQL / Basic supply the declared user (the principal
        // claim is then checked against it); Bearer has no declared user, so the principal claim
        // itself becomes the user name.
        String userName = (userIdentity != null) ? userIdentity.getUser()
                : OpenIdConnectVerifier.extractPrincipal(idToken, principalFiled);

        try {
            JWKSource<SecurityContext> jwkSource = GlobalStateMgr.getCurrentState().getJwkMgr().getJwkSource(jwksUrl);
            JWTClaimsSet claims = OpenIdConnectVerifier.verify(idToken, userName, jwkSource, principalFiled,
                    requiredIssuer, requiredAudience);
            // Only after the signature holds. Reading a claim out of an unverified token would mean
            // the token's holder gets to pick their own groups.
            if (groupSource.usesAuthenticatorGroups()) {
                authContext.setAuthenticatorGroups(
                        ClaimGroupExtractor.extract(claims, groupClaim, integrationName));
            }
            authContext.setAuthToken(idToken);
        } catch (AuthenticationException e) {
            throw e;
        } catch (Exception e) {
            throw new AuthenticationException(e.getMessage());
        }

        // Bearer: build an ephemeral identity from the token-derived user name; otherwise keep the
        // passed-in one.
        return (userIdentity != null) ? userIdentity
                : UserIdentity.createEphemeralUserIdent(userName, cred.remoteHost());
    }

    @Override
    public boolean supportsCredentialKind(AuthCredentialKind kind) {
        return kind == AuthCredentialKind.BEARER_TOKEN;
    }

    @Override
    public boolean isGroupProviderUsed() {
        return groupSource.usesGroupProvider();
    }

    @Override
    public boolean isAuthenticatorGroupsUsed() {
        return groupSource.usesAuthenticatorGroups();
    }
}
