// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.CreateRepositoryStmt;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
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

            String location = createRepositoryStmt.getLocation();
            if (Strings.isNullOrEmpty(location)) {
                throw new SemanticException("You must specify a location on the repository");
            }

            String brokerName = createRepositoryStmt.getBrokerName();
            if (createRepositoryStmt.hasBroker()) {
                if (Strings.isNullOrEmpty(brokerName)) {
                    throw new SemanticException("You must specify the broker of the repository");
                }

                FsBroker brokerAddr = null;
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
            }

            try {
                FeNameFormat.checkCommonName("repository", repoName);
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }

            return null;
        }

    }

}
