// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/InstallPluginStmt.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.plugin.DynamicPluginLoader;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;

import java.util.Map;

public class InstallPluginStmt extends DdlStmt {

    private String pluginPath;
    private Map<String, String> properties;

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
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        if (!Config.plugin_enable) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_OPERATION_DISABLED, "INSTALL PLUGIN",
                    "Please enable it by setting 'plugin_enable' = 'true'");
        }

        // check operation privilege
        if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("INSTALL PLUGIN FROM ").append("\"" + pluginPath + "\"");
        if (properties != null && !properties.isEmpty()) {
            sb.append("\nPROPERTIES (");
            sb.append(new PrintableMap<String, String>(properties, " = ", true, true, true));
            sb.append(")");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
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

