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
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.DropSnapshotStmt;

public class DropSnapshotAnalyzer {

    public static void analyze(DropSnapshotStmt dropSnapshotStmt, ConnectContext session) {
        new DropSnapshotAnalyzerVisitor().analyze(dropSnapshotStmt, session);
    }

    public static class DropSnapshotAnalyzerVisitor implements AstVisitorExtendInterface<Void, ConnectContext> {
        public void analyze(DdlStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitDropSnapshotStatement(DropSnapshotStmt statement, ConnectContext context) {
            checkRepoExists(statement.getRepoName());

            if (Strings.isNullOrEmpty(statement.getSnapshotName())) {
                throw new SemanticException("Must specify a snapshot name");
            }

            return null;
        }

        private void checkRepoExists(String repoName) {
            RepositoryAnalyzer.checkRepoName(repoName);
            Repository repo = GlobalStateMgr.getCurrentState().getBackupHandler().getRepoMgr().getRepo(repoName);
            if (repo == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Repository [" + repoName + "] does not exist");
            }
        }
    }
}
