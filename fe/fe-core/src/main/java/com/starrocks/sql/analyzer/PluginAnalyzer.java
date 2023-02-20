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

package com.starrocks.sql.analyzer;

import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.InstallPluginStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UninstallPluginStmt;

public class PluginAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext context) {
        new PluginAnalyzerVisitor().visit(statement, context);
    }

    static class PluginAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        @Override
        public Void visitInstallPluginStatement(InstallPluginStmt statement, ConnectContext context) {
            if (!Config.plugin_enable) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_OPERATION_DISABLED, "INSTALL PLUGIN",
                        "Please enable it by setting 'plugin_enable' = 'true'");
            }
            return null;
        }

        @Override
        public Void visitUninstallPluginStatement(UninstallPluginStmt statement, ConnectContext context) {
            if (!Config.plugin_enable) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_OPERATION_DISABLED, "INSTALL PLUGIN",
                        "Please enable it by setting 'plugin_enable' = 'true'");
            }
            return null;
        }
    }
}
