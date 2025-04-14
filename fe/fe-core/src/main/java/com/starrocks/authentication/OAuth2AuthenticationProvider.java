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

import com.starrocks.mysql.MysqlCodec;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserIdentity;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

public class OAuth2AuthenticationProvider implements AuthenticationProvider {
    public static final String OAUTH2_AUTH_SERVER_URL = "auth_server_url";
    public static final String OAUTH2_TOKEN_SERVER_URL = "token_server_url";
    public static final String OAUTH2_REDIRECT_URL = "redirect_url";
    public static final String OAUTH2_CLIENT_ID = "client_id";
    public static final String OAUTH2_CLIENT_SECRET = "client_secret";
    public static final String OAUTH2_JWKS_URL = "jwks_url";
    public static final String OAUTH2_PRINCIPAL_FIELD = "principal_field";
    public static final String OAUTH2_REQUIRED_ISSUER = "required_issuer";
    public static final String OAUTH2_REQUIRED_AUDIENCE = "required_audience";
    public static final String OAUTH2_CONNECT_WAIT_TIMEOUT = "connect_wait_timeout";

    private final OAuth2Context oAuth2Context;

    public OAuth2AuthenticationProvider(OAuth2Context oAuth2Context) {
        this.oAuth2Context = oAuth2Context;
    }

    @Override
    public UserAuthenticationInfo analyzeAuthOption(UserIdentity userIdentity, UserAuthOption userAuthOption)
            throws AuthenticationException {
        UserAuthenticationInfo info = new UserAuthenticationInfo();
        info.setAuthPlugin(AuthPlugin.Server.AUTHENTICATION_OAUTH2.name());
        info.setPassword(MysqlPassword.EMPTY_PASSWORD);
        info.setOrigUserHost(userIdentity.getUser(), userIdentity.getHost());
        info.setAuthString(userAuthOption == null ? null : userAuthOption.getAuthString());
        return info;
    }

    @Override
    public void authenticate(ConnectContext context, String user, String host, byte[] password,
                             UserAuthenticationInfo authenticationInfo) throws AuthenticationException {
        /*
          If the auth plugin used by the client for this authentication is not AUTHENTICATION_OAUTH2_CLIENT,
          then the authentication success will be directly returned.
          However, this is not a true authentication success, the user cannot perform any operations on this connection.
          Any operation will return an auth url to guide the user to perform oauth2 authentication.
          This is because in many scenarios, the client cannot directly launch a web browser.
         */
        if (!AuthPlugin.Client.AUTHENTICATION_OAUTH2_CLIENT.toString().equals(context.getAuthPlugin())) {
            return;
        }

        long startTime = System.currentTimeMillis();
        String token;
        while (true) {
            token = context.getAuthToken();
            if (token != null) {
                break;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (System.currentTimeMillis() - startTime > oAuth2Context.connectWaitTimeout() * 1000) {
                break;
            }
        }

        if (token == null) {
            throw new AuthenticationException("OAuth2 authentication wait callback timeout");
        }
    }

    @Override
    public byte[] authMoreDataPacket(ConnectContext context, String user, String host) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] bytes = oAuth2Context.authServerUrl().getBytes(StandardCharsets.UTF_8);
        MysqlCodec.writeInt2(outputStream, bytes.length);
        MysqlCodec.writeBytes(outputStream, bytes);

        bytes = oAuth2Context.clientId().getBytes(StandardCharsets.UTF_8);
        MysqlCodec.writeInt2(outputStream, bytes.length);
        MysqlCodec.writeBytes(outputStream, bytes);

        bytes = oAuth2Context.redirectUrl().getBytes(StandardCharsets.UTF_8);
        MysqlCodec.writeInt2(outputStream, bytes.length);
        MysqlCodec.writeBytes(outputStream, bytes);

        context.setOAuth2Context(oAuth2Context);

        return outputStream.toByteArray();
    }

    @Override
    public byte[] authSwitchRequestPacket(ConnectContext context, String user, String host) {
        return authMoreDataPacket(context, user, host);
    }
}