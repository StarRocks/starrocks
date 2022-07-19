// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.ShowSnapshotStmt;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.backup.Repository;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;

public class ShowSnapshotAnalyzer {

    public static void analyze(ShowSnapshotStmt showSnapshotStmt, ConnectContext session) {
        new ShowSnapshotAnalyzerVisitor().analyze(showSnapshotStmt, session);
    }

    public static class ShowSnapshotAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(ShowStmt statement, ConnectContext session) {
            visit(statement, session);

        }

        @Override
        public Void visitShowSnapshotStmt(ShowSnapshotStmt showSnapshotStmt, ConnectContext context) {
            String repoName = showSnapshotStmt.getRepoName();

            Repository repo =
                    GlobalStateMgr.getCurrentState().getBackupHandler().getRepoMgr().getRepo(repoName);
            if (repo == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Repository [" + repoName + "] does not exist");
            }

            try {
                showSnapshotStmt.analyze(new Analyzer(context.getGlobalStateMgr(), context));
            } catch (UserException e) {
                throw new SemanticException(e.getMessage());
            }

            return null;
        }

    }
}
