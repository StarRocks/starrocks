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

package com.starrocks.plugin;

import com.starrocks.common.util.DigitalVersion;
import com.starrocks.extension.ExtensionManager;
import com.starrocks.plugin.PluginInfo.PluginType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.Supplier;

/**
 * Registers an audit plugin provided by a static extension as a built-in audit plugin.
 *
 * <p>Extensions load before PluginMgr initializes, so an extension registers its plugin in
 * onLoad() with {@code ctx.register(AuditPlugin.class, plugin)}, and PluginMgr adds it next to
 * AuditLogBuilder. The instance must extend {@link Plugin}. Like other built-in plugins, it does
 * not receive {@link Plugin#init}, so it must be ready when registered.
 */
public final class ExtensionAuditPluginRegistrar {
    private static final Logger LOG = LogManager.getLogger(ExtensionAuditPluginRegistrar.class);

    static final String NAME_PREFIX = PluginMgr.BUILTIN_PLUGIN_PREFIX + "ext_";

    private ExtensionAuditPluginRegistrar() {
    }

    public static boolean register(PluginMgr pluginMgr) {
        return register(pluginMgr, () -> ExtensionManager.getComponent(AuditPlugin.class));
    }

    static boolean register(PluginMgr pluginMgr, Supplier<AuditPlugin> lookup) {
        AuditPlugin auditPlugin;
        try {
            auditPlugin = lookup.get();
        } catch (IllegalStateException e) {
            // Nothing registered: the extension context falls back to constructing the interface, which fails.
            LOG.info("no extension audit plugin registered");
            return false;
        }

        if (!(auditPlugin instanceof Plugin plugin)) {
            LOG.warn("extension audit plugin {} does not extend Plugin, not registered",
                    auditPlugin == null ? null : auditPlugin.getClass().getName());
            return false;
        }

        String className = plugin.getClass().getName();
        PluginInfo info = new PluginInfo(NAME_PREFIX + plugin.getClass().getSimpleName(), PluginType.AUDIT,
                "audit plugin provided by a static extension", DigitalVersion.CURRENT_PLUGIN_VERSION,
                DigitalVersion.JDK_1_8_0, className, null, null);
        if (!pluginMgr.registerBuiltinPlugin(info, plugin)) {
            LOG.warn("failed to register extension audit plugin {} as {}", className, info.getName());
            return false;
        }

        LOG.info("registered extension audit plugin {} as {}", className, info.getName());
        return true;
    }
}
