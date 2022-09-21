// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
            LOG.warn("Plugin {} has already installed!", pluginName);
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
