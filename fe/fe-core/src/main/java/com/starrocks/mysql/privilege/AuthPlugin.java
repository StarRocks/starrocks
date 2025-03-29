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

package com.starrocks.mysql.privilege;

import com.starrocks.authentication.AuthenticationProvider;
import com.starrocks.authentication.LDAPAuthProviderForNative;
import com.starrocks.authentication.OpenIdConnectAuthenticationProvider;
import com.starrocks.authentication.PlainPasswordAuthenticationProvider;
import com.starrocks.common.Config;
import org.json.JSONObject;

public class AuthPlugin {
    private static final PlainPasswordAuthenticationProvider PLAIN_PASSWORD_AUTHENTICATION_PROVIDER =
            new PlainPasswordAuthenticationProvider();
    private static final LDAPAuthProviderForNative LDAP_AUTH_PROVIDER = new LDAPAuthProviderForNative();

    public enum Server {
        MYSQL_NATIVE_PASSWORD,
        AUTHENTICATION_LDAP_SIMPLE,
        AUTHENTICATION_OPENID_CONNECT;

        public AuthenticationProvider getProvider(String authString) {
            AuthPlugin.Server authPlugin = this;
            switch (authPlugin) {
                case MYSQL_NATIVE_PASSWORD -> {
                    return PLAIN_PASSWORD_AUTHENTICATION_PROVIDER;
                }

                case AUTHENTICATION_LDAP_SIMPLE -> {
                    return LDAP_AUTH_PROVIDER;
                }

                case AUTHENTICATION_OPENID_CONNECT -> {
                    if (authString == null) {
                        authString = "{}";
                    }
                    JSONObject authStringJSON = new JSONObject(authString);
                    String jwksUrl = authStringJSON.optString("jwks_url", Config.oidc_jwks_url);
                    String principalFiled = authStringJSON.optString("principal_field", Config.oidc_principal_field);
                    String requiredIssuer = authStringJSON.optString("required_issuer", Config.oidc_required_issuer);
                    String requiredAudience = authStringJSON.optString("required_audience", Config.oidc_required_audience);

                    return new OpenIdConnectAuthenticationProvider(jwksUrl, principalFiled, requiredIssuer, requiredAudience);
                }
            }

            return null;
        }
    }

    public enum Client {
        MYSQL_NATIVE_PASSWORD,
        MYSQL_CLEAR_PASSWORD,
        AUTHENTICATION_OPENID_CONNECT_CLIENT;

        @Override
        public String toString() {
            //In the MySQL protocol, the authPlugin passed by the client is in lowercase.
            return name().toLowerCase();
        }
    }

    public static String covertFromServerToClient(String serverPluginName) {
        if (serverPluginName.equalsIgnoreCase(Server.MYSQL_NATIVE_PASSWORD.toString())) {
            return Client.MYSQL_NATIVE_PASSWORD.toString();
        } else if (serverPluginName.equalsIgnoreCase(Server.AUTHENTICATION_LDAP_SIMPLE.toString())) {
            return Client.MYSQL_CLEAR_PASSWORD.toString();
        } else if (serverPluginName.equalsIgnoreCase(Server.AUTHENTICATION_OPENID_CONNECT.toString())) {
            return Client.AUTHENTICATION_OPENID_CONNECT_CLIENT.toString();
        }
        return null;
    }
}
