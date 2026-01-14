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

import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.DdlException;
import com.starrocks.lake.snapshot.ClusterSnapshotJob;
import com.starrocks.lake.snapshot.ClusterSnapshotMgr;
import com.starrocks.lake.snapshot.ClusterSnapshotMgrEPack;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.sql.ast.AdminAlterAutomatedSnapshotIntervalStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOffStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOnStmt;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.CreateClusterSnapshotStmt;
import com.starrocks.sql.ast.DropClusterSnapshotStmt;
import com.starrocks.sql.ast.RestoreTableFromSnapshotStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.expression.IntervalLiteral;

import static com.starrocks.common.util.PropertyAnalyzer.PROPERTIES_SNAPSHOT_SCOPE;

public class ClusterSnapshotAnalyzer {
    public static void analyze(StatementBase stmt, ConnectContext session) {
        new ClusterSnapshotAnalyzer.ClusterSnapshotAnalyzerVisitor().visit(stmt, session);
    }

    static class ClusterSnapshotAnalyzerVisitor implements AstVisitorExtendInterface<Void, ConnectContext> {
        @Override
        public Void visitAdminSetAutomatedSnapshotOnStatement(AdminSetAutomatedSnapshotOnStmt statement, ConnectContext context) {
            if (!RunMode.isSharedDataMode()) {
                throw new SemanticException("Automated snapshot only support share data mode");
            }

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

            IntervalLiteral intervalLiteral = statement.getIntervalLiteral();
            long intervalSeconds = 0;
            if (intervalLiteral != null) {
                try {
                    intervalSeconds = intervalLiteral.toSeconds();
                } catch (IllegalArgumentException e) {
                    throw new SemanticException(e.getMessage(), intervalLiteral.getPos());
                }
            }
            statement.setIntervalSeconds(intervalSeconds);

            if (statement.getProperties() != null) {
                String scope = statement.getProperties().get(PROPERTIES_SNAPSHOT_SCOPE);
                if (scope != null) {
                    String scopeLower = scope.toLowerCase();
                    if (!scopeLower.equals("local") && !scopeLower.equals("external")) {
                        throw new
                            SemanticException("Invalid snapshot_scope property value: %s. Must be 'local' or 'external'", scope);
                    }
                }
            }

            return null;
        }

        @Override
        public Void visitAdminSetAutomatedSnapshotOffStatement(AdminSetAutomatedSnapshotOffStmt statement,
                                                               ConnectContext context) {
            if (!RunMode.isSharedDataMode()) {
                throw new SemanticException("Automated snapshot only support share data mode");
            }

            if (!GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().isAutomatedSnapshotOn()) {
                throw new SemanticException("Automated snapshot has not been turn on");
            }

            return null;
        }

        public Void visitAdminAlterAutomatedSnapshotIntervalStatement(AdminAlterAutomatedSnapshotIntervalStmt statement,
                                                                      ConnectContext context) {
            if (!RunMode.isSharedDataMode()) {
                throw new SemanticException("Automated snapshot only support share data mode");
            }

            if (!GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().isAutomatedSnapshotOn()) {
                throw new SemanticException("Automated snapshot has not been turn on");
            }

            IntervalLiteral intervalLiteral = statement.getIntervalLiteral();
            if (intervalLiteral == null) {
                throw new SemanticException("Interval literal is required");
            }
            long intervalSeconds;
            try {
                intervalSeconds = intervalLiteral.toSeconds();
            } catch (IllegalArgumentException e) {
                throw new SemanticException(e.getMessage(), intervalLiteral.getPos());
            }
            statement.setIntervalSeconds(intervalSeconds);

            return null;
        }

        @Override
        public Void visitCreateClusterSnapshotStatement(CreateClusterSnapshotStmt statement, ConnectContext context) {
            if (!RunMode.isSharedDataMode()) {
                throw new SemanticException("Create cluster snapshot only support shared data mode");
            }
            ClusterSnapshotMgrEPack clusterSnapshotMgrEPack =
                            (ClusterSnapshotMgrEPack) GlobalStateMgr.getCurrentState().getClusterSnapshotMgr();

            if (statement.getClusterSnapshotName().equalsIgnoreCase(ClusterSnapshotMgr.AUTOMATED_NAME_PREFIX)) {
                throw new SemanticException("Manual cluster snapshot name can not be same as automated snapshot name");
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

            String snapshotName = statement.getClusterSnapshotName();
            if (!statement.isIfNotExists() && clusterSnapshotMgrEPack.isManualClusterSnapshotNameValid(snapshotName)) {
                throw new SemanticException("Manual Cluster Snapshot Job has existed, snapshot name: {}", snapshotName);
            }

            return null;
        }

        @Override
        public Void visitDropClusterSnapshotStatement(DropClusterSnapshotStmt statement, ConnectContext context) {
            if (!RunMode.isSharedDataMode()) {
                throw new SemanticException("Drop cluster snapshot only support shared data mode");
            }

            ClusterSnapshotMgrEPack clusterSnapshotMgrEPack =
                            (ClusterSnapshotMgrEPack) GlobalStateMgr.getCurrentState().getClusterSnapshotMgr();
            String snapshotName = statement.getSnapshotName();
            if (!statement.getIfExists() && clusterSnapshotMgrEPack.getClusterSnapshotJobByName(snapshotName) == null) {
                throw new SemanticException("Manual Snapshot: %s doest not exist", snapshotName);
            }

            return null;
        }

        @Override
        public Void visitRestoreTableFromSnapshotStatement(RestoreTableFromSnapshotStmt statement, ConnectContext context) {
            if (!RunMode.isSharedDataMode()) {
                throw new SemanticException("Table snapshot recovery only supports shared data mode");
            }

            String snapshotName = statement.getSnapshotName();
            if (snapshotName.isEmpty()) {
                throw new SemanticException("Snapshot name cannot be empty");
            }

            TableRef sourceTableRef = statement.getSourceTable();
            TableRef targetTableRef = statement.getTargetTable();
            TableName sourceTable = new TableName(sourceTableRef.getCatalogName(),
                    sourceTableRef.getDbName(), sourceTableRef.getTableName());
            sourceTable.normalization(context);
            TableName targetTable = new TableName(targetTableRef.getCatalogName(),
                    targetTableRef.getDbName(), targetTableRef.getTableName());
            targetTable.normalization(context);

            ClusterSnapshotMgr clusterSnapshotMgr = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr();
            if (clusterSnapshotMgr == null) {
                throw new SemanticException("Cluster snapshot manager is not initialized");
            }

            ClusterSnapshotJob job = clusterSnapshotMgr.getClusterSnapshotJobByName(snapshotName);
            if (job == null || !job.isFinished()) {
                throw new SemanticException("Cluster snapshot '%s' is not available for restore", snapshotName);
            }

            // Check if target table already exists
            MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
            Table existingTable = metadataMgr.getTable(context,
                    targetTable.getCatalog(),
                    targetTable.getDb(),
                    targetTable.getTbl());
            if (existingTable != null) {
                throw new SemanticException("Table '%s' already exists in database '%s'",
                        targetTable.getTbl(), targetTable.getDb());
            }

            return null;
        }
    }
}
