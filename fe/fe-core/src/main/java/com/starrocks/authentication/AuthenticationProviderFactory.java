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

import com.google.common.collect.ImmutableMap;
import com.starrocks.common.Config;

import java.util.Map;

public class AuthenticationProviderFactory {
    private static final Map<String, AuthenticationProvider> PLUGIN_NAME_TO_AUTHENTICATION_PROVIDER =
            ImmutableMap.<String, AuthenticationProvider>builder()
                    .put(PlainPasswordAuthenticationProvider.PLUGIN_NAME, new PlainPasswordAuthenticationProvider())
                    .put(LDAPAuthProviderForNative.PLUGIN_NAME, new LDAPAuthProviderForNative())
                    .put(KerberosAuthenticationProvider.PLUGIN_NAME, new KerberosAuthenticationProvider())
                    .put(OpenIdConnectAuthenticationProvider.PLUGIN_NAME, new OpenIdConnectAuthenticationProvider(
                            Config.oidc_jwks_url,
                            Config.oidc_principal_field,
                            Config.oidc_required_issuer,
                            Config.oidc_required_audience))
                    .build();

    private AuthenticationProviderFactory() {}

    public static AuthenticationProvider create(String plugin) throws AuthenticationException {
        if (!PLUGIN_NAME_TO_AUTHENTICATION_PROVIDER.containsKey(plugin)) {
            throw new AuthenticationException("Cannot find " + plugin + " from "
                + PLUGIN_NAME_TO_AUTHENTICATION_PROVIDER.keySet());
        }
        return PLUGIN_NAME_TO_AUTHENTICATION_PROVIDER.get(plugin);
    }
}
