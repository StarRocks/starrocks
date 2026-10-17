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

<<<<<<< HEAD
import com.nimbusds.jose.jwk.JWKSet;
=======
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
>>>>>>> dc172843477... [Feature] [PRE-574] Read a security integration user's groups from a claim of the verified token (#63504)
import com.starrocks.catalog.UserIdentity;
import com.starrocks.mysql.MysqlCodec;
import com.starrocks.server.GlobalStateMgr;

import java.nio.ByteBuffer;

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
    public void authenticate(AccessControlContext authContext, UserIdentity userIdentity, byte[] authResponse)
            throws AuthenticationException {
        try {
<<<<<<< HEAD
            ByteBuffer authBuffer = ByteBuffer.wrap(authResponse);
            //1 Byte for capability mysql client
            MysqlCodec.readInt1(authBuffer);
            byte[] idToken = MysqlCodec.readLenEncodedString(authBuffer);
            JWKSet jwkSet = GlobalStateMgr.getCurrentState().getJwkMgr().getJwkSet(jwksUrl);
            OpenIdConnectVerifier.verify(new String(idToken), userIdentity.getUser(), jwkSet, principalFiled, requiredIssuer,
                    requiredAudience);
            authContext.setAuthToken(new String(idToken));
=======
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
>>>>>>> dc172843477... [Feature] [PRE-574] Read a security integration user's groups from a claim of the verified token (#63504)
        } catch (Exception e) {
            throw new AuthenticationException(e.getMessage());
        }
    }
<<<<<<< HEAD
}
=======

    @Override
    public boolean isGroupProviderUsed() {
        return groupSource.usesGroupProvider();
    }

    @Override
    public boolean isAuthenticatorGroupsUsed() {
        return groupSource.usesAuthenticatorGroups();
    }
}
>>>>>>> dc172843477... [Feature] [PRE-574] Read a security integration user's groups from a claim of the verified token (#63504)
