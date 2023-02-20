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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class AuthenticationProviderFactory {
    private static final Logger LOG = LogManager.getLogger(AuthenticationProviderFactory.class);
    private static Map<String, AuthenticationProvider> pluginNameToAuthenticationProvider = new HashMap<>();

    private AuthenticationProviderFactory() {}

    public static void installPlugin(String pluginName, AuthenticationProvider provider) {
        if (pluginNameToAuthenticationProvider.containsKey(pluginName)) {
            LOG.warn("Plugin {} has already been installed!", pluginName);
        }
        pluginNameToAuthenticationProvider.put(pluginName, provider);
    }

    public static void uninstallPlugin(String pluginName) {
        if (!pluginNameToAuthenticationProvider.containsKey(pluginName)) {
            LOG.warn("Cannot find {} from {} ", pluginName, pluginNameToAuthenticationProvider.keySet());
        }
        pluginNameToAuthenticationProvider.remove(pluginName);
    }

    public static AuthenticationProvider create(String plugin) throws AuthenticationException {
        if (! pluginNameToAuthenticationProvider.containsKey(plugin)) {
            throw new AuthenticationException("Cannot find " + plugin + " from "
                + pluginNameToAuthenticationProvider.keySet());
        }
        return pluginNameToAuthenticationProvider.get(plugin);
    }
}
