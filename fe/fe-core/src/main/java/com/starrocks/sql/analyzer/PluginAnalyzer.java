// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
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
