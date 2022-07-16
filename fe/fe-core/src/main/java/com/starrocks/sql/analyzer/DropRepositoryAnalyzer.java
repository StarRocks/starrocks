// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.DropRepositoryStmt;
import com.starrocks.backup.Repository;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;

public class DropRepositoryAnalyzer {

    public static void analyze(DropRepositoryStmt dropRepositoryStmt, ConnectContext session) {
        new DropRepositoryAnalyzerVisitor().analyze(dropRepositoryStmt, session);
    }

    public static class DropRepositoryAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(DdlStmt statement, ConnectContext session) {
            visit(statement, session);

        }

        @Override
        public Void visitDropRepositoryStmt(DropRepositoryStmt dropRepositoryStmt, ConnectContext context) {
            String repoName = dropRepositoryStmt.getRepoName();
            if (Strings.isNullOrEmpty(repoName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Repository does not empty");
            }

            Repository repo =
                    GlobalStateMgr.getCurrentState().getBackupHandler().getRepoMgr().getRepo(repoName);
            if (repo == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Repository does not exist");
            }

            try {
                dropRepositoryStmt.analyze(new Analyzer(context.getGlobalStateMgr(), context));
            } catch (UserException e) {
                throw new SemanticException(e.getMessage());
            }

            return null;
        }

    }

}
