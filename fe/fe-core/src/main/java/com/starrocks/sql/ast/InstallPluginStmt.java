// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.plugin.DynamicPluginLoader;

import java.util.Map;

public class InstallPluginStmt extends DdlStmt {

    private final String pluginPath;
    private final Map<String, String> properties;

    public InstallPluginStmt(String pluginPath, Map<String, String> properties) {
        this.pluginPath = pluginPath;
        this.properties = properties;
    }

    public String getPluginPath() {
        return pluginPath;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getMd5sum() {
        return properties == null ? null : properties.get(DynamicPluginLoader.MD5SUM_KEY);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitInstallPluginStatement(this, context);
    }
}

