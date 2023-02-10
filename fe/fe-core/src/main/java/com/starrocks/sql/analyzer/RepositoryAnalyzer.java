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

import com.google.common.base.Strings;
import com.starrocks.backup.Repository;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateRepositoryStmt;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.DropRepositoryStmt;

public class RepositoryAnalyzer {

    public static void analyze(CreateRepositoryStmt createRepositoryStmt, ConnectContext session) {
        new RepositoryAnalyzerVisitor().analyze(createRepositoryStmt, session);
    }

    public static void analyze(DropRepositoryStmt dropRepositoryStmt, ConnectContext session) {
        new RepositoryAnalyzerVisitor().analyze(dropRepositoryStmt, session);
    }

    public static class RepositoryAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(DdlStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCreateRepositoryStatement(CreateRepositoryStmt createRepositoryStmt, ConnectContext context) {
            String repoName = createRepositoryStmt.getName();
            checkRepoName(repoName);

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

            return null;
        }

        @Override
        public Void visitDropRepositoryStatement(DropRepositoryStmt dropRepositoryStmt, ConnectContext context) {
            String repoName = dropRepositoryStmt.getRepoName();
            checkRepoName(repoName);
            Repository repo =
                    GlobalStateMgr.getCurrentState().getBackupHandler().getRepoMgr().getRepo(repoName);
            if (repo == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Repository does not exist");
            }

            return null;
        }
    }

    public static void checkRepoName(String repoName) {
        if (Strings.isNullOrEmpty(repoName)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Repository does not empty");
        }
        FeNameFormat.checkCommonName("repository", repoName);
    }
}
