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
        try {
            ByteBuffer authBuffer = ByteBuffer.wrap(authResponse);
            //1 Byte for capability mysql client
            MysqlCodec.readInt1(authBuffer);
            byte[] idToken = MysqlCodec.readLenEncodedString(authBuffer);
            JWKSet jwkSet = GlobalStateMgr.getCurrentState().getJwkMgr().getJwkSet(jwksUrl);
            OpenIdConnectVerifier.verify(new String(idToken), userIdentity.getUser(), jwkSet, principalFiled, requiredIssuer,
                    requiredAudience);
            authContext.setAuthToken(new String(idToken));
        } catch (Exception e) {
            throw new AuthenticationException(e.getMessage());
        }
    }
}