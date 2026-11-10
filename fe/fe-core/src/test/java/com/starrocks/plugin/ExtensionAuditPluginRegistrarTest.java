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

import com.starrocks.extension.DefaultExtensionContext;
import com.starrocks.plugin.PluginInfo.PluginType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExtensionAuditPluginRegistrarTest {

    private static class TestAuditPlugin extends Plugin implements AuditPlugin {
        @Override
        public boolean eventFilter(AuditEvent.EventType type) {
            return type == AuditEvent.EventType.AFTER_QUERY;
        }

        @Override
        public void exec(AuditEvent event) {
        }
    }

    private static class NotAPlugin implements AuditPlugin {
        @Override
        public boolean eventFilter(AuditEvent.EventType type) {
            return true;
        }

        @Override
        public void exec(AuditEvent event) {
        }
    }

    @Test
    public void testRegistersExtensionPlugin() {
        PluginMgr pluginMgr = new PluginMgr();
        TestAuditPlugin plugin = new TestAuditPlugin();
        DefaultExtensionContext context = new DefaultExtensionContext();
        context.register(AuditPlugin.class, plugin);

        assertTrue(ExtensionAuditPluginRegistrar.register(pluginMgr, () -> context.get(AuditPlugin.class)));

        List<Plugin> active = pluginMgr.getActivePluginList(PluginType.AUDIT);
        assertEquals(1, active.size());
        assertSame(plugin, active.get(0));
        assertSame(plugin, pluginMgr.getActivePlugin(
                ExtensionAuditPluginRegistrar.NAME_PREFIX + "TestAuditPlugin", PluginType.AUDIT));
    }

    @Test
    public void testNoExtensionPluginRegistered() {
        PluginMgr pluginMgr = new PluginMgr();
        DefaultExtensionContext context = new DefaultExtensionContext();

        assertFalse(ExtensionAuditPluginRegistrar.register(pluginMgr, () -> context.get(AuditPlugin.class)));
        assertTrue(pluginMgr.getActivePluginList(PluginType.AUDIT).isEmpty());
    }

    @Test
    public void testRejectsAuditPluginThatIsNotAPlugin() {
        PluginMgr pluginMgr = new PluginMgr();

        assertFalse(ExtensionAuditPluginRegistrar.register(pluginMgr, NotAPlugin::new));
        assertFalse(ExtensionAuditPluginRegistrar.register(pluginMgr, () -> null));
        assertTrue(pluginMgr.getActivePluginList(PluginType.AUDIT).isEmpty());
    }

    @Test
    public void testDuplicateRegistrationKeepsFirstPlugin() {
        PluginMgr pluginMgr = new PluginMgr();
        TestAuditPlugin first = new TestAuditPlugin();

        assertTrue(ExtensionAuditPluginRegistrar.register(pluginMgr, () -> first));
        assertFalse(ExtensionAuditPluginRegistrar.register(pluginMgr, TestAuditPlugin::new));

        List<Plugin> active = pluginMgr.getActivePluginList(PluginType.AUDIT);
        assertEquals(1, active.size());
        assertSame(first, active.get(0));
    }
}
