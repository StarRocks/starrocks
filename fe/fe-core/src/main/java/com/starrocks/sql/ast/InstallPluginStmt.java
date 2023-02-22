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


package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.plugin.DynamicPluginLoader;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public class InstallPluginStmt extends DdlStmt {

    private final String pluginPath;
    private final Map<String, String> properties;

    public InstallPluginStmt(String pluginPath, Map<String, String> properties) {
        this(pluginPath, properties, NodePosition.ZERO);
    }

    public InstallPluginStmt(String pluginPath, Map<String, String> properties, NodePosition pos) {
        super(pos);
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

