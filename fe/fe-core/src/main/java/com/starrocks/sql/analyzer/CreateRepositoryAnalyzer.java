// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.CreateRepositoryStmt;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;

public class CreateRepositoryAnalyzer {

    public static void analyze(CreateRepositoryStmt createRepositoryStmt, ConnectContext session) {
        new CreateRepositoryAnalyzerVisitor().analyze(createRepositoryStmt, session);
    }

    public static class CreateRepositoryAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(DdlStmt statement, ConnectContext session) {
            visit(statement, session);

        }

        @Override
        public Void visitCreateRepositoryStmt(CreateRepositoryStmt createRepositoryStmt, ConnectContext context) {

            String repoName = createRepositoryStmt.getName();
            if (Strings.isNullOrEmpty(repoName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Repository does not empty");
            }

            String brokerName = createRepositoryStmt.getBrokerName();
            FsBroker brokerAddr = null;
            if (Strings.isNullOrEmpty(brokerName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "broker does not empty");
            }
            String location = createRepositoryStmt.getLocation();
            if (Strings.isNullOrEmpty(location)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Location does not empty");
            }

            try {
                brokerAddr = context.getGlobalStateMgr().getBrokerMgr().getBroker(brokerName, location);
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "failed to get address of broker " + brokerName);
            }

            if (brokerAddr == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "failed to get address of broker " + brokerName);
            }

            try {
                createRepositoryStmt.analyze(new Analyzer(context.getGlobalStateMgr(), context));
            } catch (UserException e) {
                throw new SemanticException(e.getMessage());
            }

            return null;
        }

    }

}
