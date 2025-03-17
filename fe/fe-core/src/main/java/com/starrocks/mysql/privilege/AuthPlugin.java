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

import com.google.common.collect.ImmutableMap;
import com.starrocks.authentication.AuthenticationProvider;
import com.starrocks.authentication.LDAPAuthProviderForNative;
import com.starrocks.authentication.OpenIdConnectAuthenticationProvider;
import com.starrocks.authentication.PlainPasswordAuthenticationProvider;
import com.starrocks.common.Config;

import java.util.Map;

public class AuthPlugin {

    public enum Server {
        MYSQL_NATIVE_PASSWORD,
        AUTHENTICATION_LDAP_SIMPLE,
        AUTHENTICATION_OPENID_CONNECT;

        private static final Map<String, AuthenticationProvider> PLUGIN_NAME_TO_AUTHENTICATION_PROVIDER =
                ImmutableMap.<String, AuthenticationProvider>builder()
                        .put(AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.name(), new PlainPasswordAuthenticationProvider())
                        .put(AuthPlugin.Server.AUTHENTICATION_LDAP_SIMPLE.name(), new LDAPAuthProviderForNative())
                        .put(AuthPlugin.Server.AUTHENTICATION_OPENID_CONNECT.name(), new OpenIdConnectAuthenticationProvider(
                                Config.oidc_jwks_url,
                                Config.oidc_principal_field,
                                Config.oidc_required_issuer,
                                Config.oidc_required_audience))
                        .build();

        public AuthenticationProvider getProvider() {
            return PLUGIN_NAME_TO_AUTHENTICATION_PROVIDER.getOrDefault(name(), null);
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
        if (serverPluginName.equals(Server.MYSQL_NATIVE_PASSWORD.toString())) {
            return Client.MYSQL_NATIVE_PASSWORD.toString();
        } else if (serverPluginName.equals(Server.AUTHENTICATION_LDAP_SIMPLE.toString())) {
            return Client.MYSQL_CLEAR_PASSWORD.toString();
        } else if (serverPluginName.equals(Server.AUTHENTICATION_OPENID_CONNECT.toString())) {
            return Client.AUTHENTICATION_OPENID_CONNECT_CLIENT.toString();
        }
        return null;
    }
}
