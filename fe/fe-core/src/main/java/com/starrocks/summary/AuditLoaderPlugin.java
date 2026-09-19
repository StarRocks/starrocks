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

package com.starrocks.summary;

import com.starrocks.common.Config;
import com.starrocks.common.util.DigitalVersion;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.plugin.AuditEvent.EventType;
import com.starrocks.plugin.AuditPlugin;
import com.starrocks.plugin.Plugin;
import com.starrocks.plugin.PluginInfo;
import com.starrocks.plugin.PluginInfo.PluginType;
import com.starrocks.plugin.PluginMgr;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Builtin AUDIT plugin that hands audit events to {@link AuditLoaderMgr}, which persists them into an
 * internal table. Registered at FE startup and gated by the {@code enable_audit_loader} FE config
 * (disabled by default, mutable via ADMIN SET FRONTEND CONFIG). Stays inert while an external dynamic
 * AUDIT plugin is installed, to avoid importing audit data twice.
 */
public class AuditLoaderPlugin extends Plugin implements AuditPlugin {
    private static final Logger LOG = LogManager.getLogger(AuditLoaderPlugin.class);

    private final PluginInfo pluginInfo;

    public AuditLoaderPlugin() {
        pluginInfo = new PluginInfo(PluginMgr.BUILTIN_PLUGIN_PREFIX + "AuditLoader", PluginType.AUDIT,
                "builtin audit loader", DigitalVersion.fromString("0.1.0"),
                DigitalVersion.fromString("1.8.31"), AuditLoaderPlugin.class.getName(), null, null);
    }

    public PluginInfo getPluginInfo() {
        return pluginInfo;
    }

    @Override
    public boolean eventFilter(EventType type) {
        if (!Config.enable_audit_loader) {
            return false;
        }
        if (type != EventType.AFTER_QUERY && type != EventType.CONNECTION) {
            return false;
        }
        AuditLoaderMgr mgr = GlobalStateMgr.getCurrentState().getAuditLoaderMgr();
        return mgr != null && !mgr.isDisabledByConflict();
    }

    @Override
    public void exec(AuditEvent event) {
        // Runs on the single audit-event worker thread: must be lightweight and must never throw, so a
        // failure here can never stall the audit pipeline.
        try {
            AuditLoaderMgr mgr = GlobalStateMgr.getCurrentState().getAuditLoaderMgr();
            if (mgr != null) {
                mgr.offerEvent(event);
            }
        } catch (Throwable t) {
            LOG.warn("failed to buffer audit event", t);
        }
    }
}
