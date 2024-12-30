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

import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOffStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOnStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.StatementBase;

public class ClusterSnapshotAnalyzer {
    public static void analyze(StatementBase stmt, ConnectContext session) {
        new ClusterSnapshotAnalyzer.ClusterSnapshotAnalyzerVisitor().visit(stmt, session);
    }

    static class ClusterSnapshotAnalyzerVisitor implements AstVisitor<Void, ConnectContext> {
        @Override
        public Void visitAdminSetAutomatedSnapshotOnStatement(AdminSetAutomatedSnapshotOnStmt statement, ConnectContext context) {
            if (GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().isAutomatedSnapshotOn()) {
                throw new SemanticException("Automated snapshot has been turn on");
            }

            String storageVolumeName = statement.getStorageVolumeName();
            StorageVolumeMgr storageVolumeMgr = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
            try {
                if (!storageVolumeMgr.exists(storageVolumeName)) {
                    throw new SemanticException("Unknown storage volume: %s", storageVolumeName);
                }
            } catch (DdlException e) {
                throw new SemanticException("Failed to get storage volume", e);
            }

            return null;
        }

        @Override
        public Void visitAdminSetAutomatedSnapshotOffStatement(AdminSetAutomatedSnapshotOffStmt statement,
                                                               ConnectContext context) {
            if (!GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().isAutomatedSnapshotOn()) {
                throw new SemanticException("Automated snapshot has not been turn on");
            }

            return null;
        }
    }
}
