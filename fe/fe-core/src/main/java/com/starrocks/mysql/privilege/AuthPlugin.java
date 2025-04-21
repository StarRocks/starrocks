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

import com.google.re2j.Pattern;
import com.starrocks.authentication.AuthenticationProvider;
import com.starrocks.authentication.LDAPAuthProviderForNative;
import com.starrocks.authentication.OAuth2AuthenticationProvider;
import com.starrocks.authentication.OAuth2Context;
import com.starrocks.authentication.OpenIdConnectAuthenticationProvider;
import com.starrocks.authentication.PlainPasswordAuthenticationProvider;
import com.starrocks.common.Config;
import com.starrocks.mysql.MysqlPassword;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

public class AuthPlugin {
    private static final Pattern COMMA_SPLIT = Pattern.compile("\\s*,\\s*");

    public enum Server {
        MYSQL_NATIVE_PASSWORD,
        AUTHENTICATION_LDAP_SIMPLE,
        AUTHENTICATION_OPENID_CONNECT,
        AUTHENTICATION_OAUTH2;

        public AuthenticationProvider getProvider(String authString) {
            AuthPlugin.Server authPlugin = this;
            switch (authPlugin) {
                case MYSQL_NATIVE_PASSWORD -> {
                    return new PlainPasswordAuthenticationProvider(
                            authString == null ? MysqlPassword.EMPTY_PASSWORD : authString.getBytes(StandardCharsets.UTF_8));
                }

                case AUTHENTICATION_LDAP_SIMPLE -> {
                    return new LDAPAuthProviderForNative(
                            Config.authentication_ldap_simple_server_host,
                            Config.authentication_ldap_simple_server_port,
                            Config.authentication_ldap_simple_bind_root_dn,
                            Config.authentication_ldap_simple_bind_root_pwd,
                            Config.authentication_ldap_simple_bind_base_dn,
                            Config.authentication_ldap_simple_user_search_attr,
                            authString);
                }

                case AUTHENTICATION_OPENID_CONNECT -> {
                    JSONObject authStringJSON = new JSONObject(authString == null ? "{}" : authString);

                    String jwksUrl = authStringJSON.optString(OpenIdConnectAuthenticationProvider.OIDC_JWKS_URL,
                            Config.oidc_jwks_url);
                    String principalFiled = authStringJSON.optString(OpenIdConnectAuthenticationProvider.OIDC_PRINCIPAL_FIELD,
                            Config.oidc_principal_field);
                    String requiredIssuer = authStringJSON.optString(OpenIdConnectAuthenticationProvider.OIDC_REQUIRED_ISSUER,
                            String.join(",", Config.oidc_required_issuer));
                    String requiredAudience = authStringJSON.optString(OpenIdConnectAuthenticationProvider.OIDC_REQUIRED_AUDIENCE,
                            String.join(",", Config.oidc_required_audience));

                    return new OpenIdConnectAuthenticationProvider(jwksUrl, principalFiled,
                            COMMA_SPLIT.split(requiredIssuer.trim()), COMMA_SPLIT.split(requiredAudience.trim()));
                }

                case AUTHENTICATION_OAUTH2 -> {
                    JSONObject authStringJSON = new JSONObject(authString == null ? "{}" : authString);

                    String oauth2AuthServerUrl = authStringJSON.optString(OAuth2AuthenticationProvider.OAUTH2_AUTH_SERVER_URL,
                            Config.oauth2_auth_server_url);
                    String oauth2TokenServerUrl = authStringJSON.optString(OAuth2AuthenticationProvider.OAUTH2_TOKEN_SERVER_URL,
                            Config.oauth2_token_server_url);
                    String oauth2RedirectUrl = authStringJSON.optString(OAuth2AuthenticationProvider.OAUTH2_REDIRECT_URL,
                            Config.oauth2_redirect_url);
                    String oauth2ClientId = authStringJSON.optString(OAuth2AuthenticationProvider.OAUTH2_CLIENT_ID,
                            Config.oauth2_client_id);
                    String oauth2ClientSecret = authStringJSON.optString(OAuth2AuthenticationProvider.OAUTH2_CLIENT_SECRET,
                            Config.oauth2_client_secret);
                    String oidcJwksUrl = authStringJSON.optString(OAuth2AuthenticationProvider.OAUTH2_JWKS_URL,
                            Config.oauth2_jwks_url);
                    String oidcPrincipalField = authStringJSON.optString(OAuth2AuthenticationProvider.OAUTH2_PRINCIPAL_FIELD,
                            Config.oauth2_principal_field);
                    String oidcRequiredIssuer = authStringJSON.optString(OAuth2AuthenticationProvider.OAUTH2_REQUIRED_ISSUER,
                            Config.oauth2_required_issuer);
                    String oidcRequiredAudience = authStringJSON.optString(OAuth2AuthenticationProvider.OAUTH2_REQUIRED_AUDIENCE,
                            Config.oauth2_required_audience);
                    Long oauthConnectWaitTimeout = authStringJSON.optLong(
                            OAuth2AuthenticationProvider.OAUTH2_CONNECT_WAIT_TIMEOUT,
                            Config.oauth2_connect_wait_timeout);

                    return new OAuth2AuthenticationProvider(new OAuth2Context(
                            oauth2AuthServerUrl,
                            oauth2TokenServerUrl,
                            oauth2RedirectUrl,
                            oauth2ClientId,
                            oauth2ClientSecret,
                            oidcJwksUrl,
                            oidcPrincipalField,
                            COMMA_SPLIT.split(oidcRequiredIssuer.trim()),
                            COMMA_SPLIT.split(oidcRequiredAudience.trim()),
                            oauthConnectWaitTimeout));
                }
            }

            return null;
        }
    }

    public enum Client {
        MYSQL_NATIVE_PASSWORD,
        MYSQL_CLEAR_PASSWORD,
        AUTHENTICATION_OPENID_CONNECT_CLIENT,
        AUTHENTICATION_OAUTH2_CLIENT;

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
        } else if (serverPluginName.equalsIgnoreCase(Server.AUTHENTICATION_OAUTH2.toString())) {
            return Client.AUTHENTICATION_OAUTH2_CLIENT.toString();
        }
        return null;
    }

    public static boolean isStarRocksCustomAuthPlugin(String pluginName) {
        if (AuthPlugin.Client.AUTHENTICATION_OAUTH2_CLIENT.toString().equalsIgnoreCase(pluginName)) {
            return true;
        } else {
            return false;
        }
    }
}
