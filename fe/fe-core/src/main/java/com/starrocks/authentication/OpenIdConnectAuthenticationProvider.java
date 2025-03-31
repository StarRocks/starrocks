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
import com.starrocks.mysql.MysqlCodec;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserIdentity;

import java.nio.ByteBuffer;

public class OpenIdConnectAuthenticationProvider implements AuthenticationProvider {
    private final String jwksUrl;
    private final String principalFiled;
    private final String requiredIssuer;
    private final String requiredAudience;

    public OpenIdConnectAuthenticationProvider(String jwksUrl, String principalFiled,
                                               String requiredIssuer, String requiredAudience) {
        this.jwksUrl = jwksUrl;
        this.principalFiled = principalFiled;
        this.requiredIssuer = requiredIssuer;
        this.requiredAudience = requiredAudience;
    }

    @Override
    public UserAuthenticationInfo analyzeAuthOption(UserIdentity userIdentity, UserAuthOption userAuthOption)
            throws AuthenticationException {
        UserAuthenticationInfo info = new UserAuthenticationInfo();
        info.setAuthPlugin(AuthPlugin.Server.AUTHENTICATION_OPENID_CONNECT.name());
        info.setPassword(MysqlPassword.EMPTY_PASSWORD);
        info.setOrigUserHost(userIdentity.getUser(), userIdentity.getHost());
        info.setAuthString(userAuthOption == null ? null : userAuthOption.getAuthString());
        return info;
    }

    @Override
    public void authenticate(ConnectContext context, String user, String host, byte[] authResponse, byte[] randomString,
                             UserAuthenticationInfo authenticationInfo) throws AuthenticationException {
        try {
            ByteBuffer authBuffer = ByteBuffer.wrap(authResponse);
            //1 Byte for capability mysql client
            MysqlCodec.readInt1(authBuffer);
            byte[] idToken = MysqlCodec.readLenEncodedString(authBuffer);
            JWKSet jwkSet = GlobalStateMgr.getCurrentState().getJwkMgr().getJwkSet(jwksUrl);
            OpenIdConnectVerifier.verify(new String(idToken), user, jwkSet, principalFiled, requiredIssuer, requiredAudience);
            context.setAuthToken(new String(idToken));
        } catch (Exception e) {
            throw new AuthenticationException(e.getMessage());
        }
    }
}