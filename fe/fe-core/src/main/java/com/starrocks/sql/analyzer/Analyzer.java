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

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AddSqlBlackListStmt;
import com.starrocks.sql.ast.AdminCancelRepairTableStmt;
import com.starrocks.sql.ast.AdminCheckTabletsStmt;
import com.starrocks.sql.ast.AdminRepairTableStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOffStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOnStmt;
import com.starrocks.sql.ast.AdminSetConfigStmt;
import com.starrocks.sql.ast.AdminSetPartitionVersionStmt;
import com.starrocks.sql.ast.AdminSetReplicaStatusStmt;
import com.starrocks.sql.ast.AdminShowConfigStmt;
import com.starrocks.sql.ast.AdminShowReplicaDistributionStmt;
import com.starrocks.sql.ast.AdminShowReplicaStatusStmt;
import com.starrocks.sql.ast.AlterCatalogStmt;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import com.starrocks.sql.ast.AlterDatabaseRenameStatement;
import com.starrocks.sql.ast.AlterLoadStmt;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterResourceGroupStmt;
import com.starrocks.sql.ast.AlterResourceStmt;
import com.starrocks.sql.ast.AlterRoutineLoadStmt;
import com.starrocks.sql.ast.AlterStorageVolumeStmt;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BackupStmt;
import com.starrocks.sql.ast.BaseCreateAlterUserStmt;
import com.starrocks.sql.ast.BaseGrantRevokePrivilegeStmt;
import com.starrocks.sql.ast.BaseGrantRevokeRoleStmt;
import com.starrocks.sql.ast.CancelAlterSystemStmt;
import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.sql.ast.CancelCompactionStmt;
import com.starrocks.sql.ast.CancelExportStmt;
import com.starrocks.sql.ast.CancelLoadStmt;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStmt;
import com.starrocks.sql.ast.ClearDataCacheRulesStmt;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.CreateDataCacheRuleStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateDictionaryStmt;
import com.starrocks.sql.ast.CreateFileStmt;
import com.starrocks.sql.ast.CreateFunctionStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.CreateRepositoryStmt;
import com.starrocks.sql.ast.CreateResourceGroupStmt;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.CreateStorageVolumeStmt;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateTemporaryTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTemporaryTableLikeStmt;
import com.starrocks.sql.ast.CreateTemporaryTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DescStorageVolumeStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.DropDataCacheRuleStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropDictionaryStmt;
import com.starrocks.sql.ast.DropFileStmt;
import com.starrocks.sql.ast.DropFunctionStmt;
import com.starrocks.sql.ast.DropHistogramStmt;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropRepositoryStmt;
import com.starrocks.sql.ast.DropResourceGroupStmt;
import com.starrocks.sql.ast.DropResourceStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.DropStatsStmt;
import com.starrocks.sql.ast.DropStorageVolumeStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.DropTemporaryTableStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.ExecuteStmt;
import com.starrocks.sql.ast.ExportStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.InstallPluginStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.PauseRoutineLoadStmt;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.sql.ast.RecoverPartitionStmt;
import com.starrocks.sql.ast.RecoverTableStmt;
import com.starrocks.sql.ast.RefreshDictionaryStmt;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.ast.RestoreStmt;
import com.starrocks.sql.ast.ResumeRoutineLoadStmt;
import com.starrocks.sql.ast.SetCatalogStmt;
import com.starrocks.sql.ast.SetDefaultRoleStmt;
import com.starrocks.sql.ast.SetDefaultStorageVolumeStmt;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetUserPropertyStmt;
import com.starrocks.sql.ast.ShowAnalyzeJobStmt;
import com.starrocks.sql.ast.ShowAnalyzeStatusStmt;
import com.starrocks.sql.ast.ShowAuthenticationStmt;
import com.starrocks.sql.ast.ShowBackupStmt;
import com.starrocks.sql.ast.ShowBasicStatsMetaStmt;
import com.starrocks.sql.ast.ShowCatalogsStmt;
import com.starrocks.sql.ast.ShowCreateDbStmt;
import com.starrocks.sql.ast.ShowDataCacheRulesStmt;
import com.starrocks.sql.ast.ShowDictionaryStmt;
import com.starrocks.sql.ast.ShowDynamicPartitionStmt;
import com.starrocks.sql.ast.ShowExportStmt;
import com.starrocks.sql.ast.ShowGrantsStmt;
import com.starrocks.sql.ast.ShowHistogramStatsMetaStmt;
import com.starrocks.sql.ast.ShowResourcesStmt;
import com.starrocks.sql.ast.ShowRestoreStmt;
import com.starrocks.sql.ast.ShowSmallFilesStmt;
import com.starrocks.sql.ast.ShowSnapshotStmt;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.ShowStorageVolumesStmt;
import com.starrocks.sql.ast.ShowTransactionStmt;
import com.starrocks.sql.ast.ShowUserPropertyStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.StopRoutineLoadStmt;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.ast.UninstallPluginStmt;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.UseCatalogStmt;
import com.starrocks.sql.ast.UseDbStmt;
import com.starrocks.sql.ast.pipe.AlterPipeStmt;
import com.starrocks.sql.ast.pipe.CreatePipeStmt;
import com.starrocks.sql.ast.pipe.DescPipeStmt;
import com.starrocks.sql.ast.pipe.DropPipeStmt;
import com.starrocks.sql.ast.pipe.ShowPipeStmt;
import com.starrocks.sql.ast.translate.TranslateStmt;
import com.starrocks.sql.ast.warehouse.CreateWarehouseStmt;
import com.starrocks.sql.ast.warehouse.DropWarehouseStmt;
import com.starrocks.sql.ast.warehouse.ResumeWarehouseStmt;
import com.starrocks.sql.ast.warehouse.SetWarehouseStmt;
import com.starrocks.sql.ast.warehouse.ShowClustersStmt;
import com.starrocks.sql.ast.warehouse.ShowNodesStmt;
import com.starrocks.sql.ast.warehouse.ShowWarehousesStmt;
import com.starrocks.sql.ast.warehouse.SuspendWarehouseStmt;

public class Analyzer {
    private final AnalyzerVisitor analyzerVisitor;

    public Analyzer(AnalyzerVisitor analyzerVisitor) {
        this.analyzerVisitor = analyzerVisitor;
    }

    public static void analyze(StatementBase statement, ConnectContext context) {
        GlobalStateMgr.getCurrentState().getAnalyzer().analyzerVisitor.visit(statement, context);
    }

    public static class AnalyzerVisitor implements AstVisitor<Void, ConnectContext> {
        private static final Analyzer.AnalyzerVisitor INSTANCE = new Analyzer.AnalyzerVisitor();

        public static Analyzer.AnalyzerVisitor getInstance() {
            return INSTANCE;
        }

        protected AnalyzerVisitor() {
        }

        // ---------------------------------------- Database Statement -----------------------------------------------------

        @Override
        public Void visitUseDbStatement(UseDbStmt statement, ConnectContext context) {
            BasicDbStmtAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitShowCreateDbStatement(ShowCreateDbStmt statement, ConnectContext context) {
            BasicDbStmtAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitRecoverDbStatement(RecoverDbStmt statement, ConnectContext context) {
            BasicDbStmtAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCreateTableStatement(CreateTableStmt statement, ConnectContext context) {
            CreateTableAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCreateTableLikeStatement(CreateTableLikeStmt statement, ConnectContext context) {
            CreateTableLikeAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCreateTemporaryTableStatement(CreateTemporaryTableStmt statement, ConnectContext context) {
            CreateTableAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCreateTemporaryTableLikeStatement(CreateTemporaryTableLikeStmt statement, ConnectContext context) {
            CreateTableLikeAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitAlterTableStatement(AlterTableStmt statement, ConnectContext context) {
            AlterTableStatementAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCancelAlterTableStatement(CancelAlterTableStmt statement, ConnectContext context) {
            CancelAlterTableStatementAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitAlterResourceGroupStatement(AlterResourceGroupStmt statement, ConnectContext session) {
            statement.analyze();
            return null;
        }

        @Override
        public Void visitDropResourceGroupStatement(DropResourceGroupStmt statement, ConnectContext session) {
            statement.analyze();
            return null;
        }

        @Override
        public Void visitAdminSetReplicaStatusStatement(AdminSetReplicaStatusStmt statement, ConnectContext session) {
            AdminStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitAdminShowReplicaStatusStatement(AdminShowReplicaStatusStmt statement, ConnectContext session) {
            AdminStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitAdminShowReplicaDistributionStatement(AdminShowReplicaDistributionStmt statement,
                                                               ConnectContext session) {
            AdminStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitAdminRepairTableStatement(AdminRepairTableStmt statement, ConnectContext session) {
            AdminStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitAdminCancelRepairTableStatement(AdminCancelRepairTableStmt statement, ConnectContext session) {
            AdminStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitAdminCheckTabletsStatement(AdminCheckTabletsStmt statement, ConnectContext session) {
            AdminStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitAdminSetPartitionVersionStmt(AdminSetPartitionVersionStmt statement, ConnectContext context) {
            AdminStmtAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitShowUserPropertyStatement(ShowUserPropertyStmt statement, ConnectContext session) {
            ShowUserPropertyAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitSetUserPropertyStatement(SetUserPropertyStmt statement, ConnectContext session) {
            SetUserPropertyAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitCreateViewStatement(CreateViewStmt statement, ConnectContext session) {
            ViewAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitAlterViewStatement(AlterViewStmt statement, ConnectContext session) {
            ViewAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitCreateTableAsSelectStatement(CreateTableAsSelectStmt statement, ConnectContext session) {
            // this phrase do not analyze insertStmt, insertStmt will analyze in
            // StmtExecutor.handleCreateTableAsSelectStmt because planner will not do meta operations
            CTASAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitCreateTemporaryTableAsSelectStatement(
                CreateTemporaryTableAsSelectStmt statement, ConnectContext session) {
            CTASAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitSubmitTaskStatement(SubmitTaskStmt statement, ConnectContext context) {
            StatementBase taskStmt = null;
            if (statement.getCreateTableAsSelectStmt() != null) {
                CreateTableAsSelectStmt createTableAsSelectStmt = statement.getCreateTableAsSelectStmt();
                QueryStatement queryStatement = createTableAsSelectStmt.getQueryStatement();
                Analyzer.analyze(queryStatement, context);
                taskStmt = queryStatement;
            } else if (statement.getInsertStmt() != null) {
                InsertStmt insertStmt = statement.getInsertStmt();
                InsertAnalyzer.analyze(insertStmt, context);
                taskStmt = insertStmt;
            } else if (statement.getDataCacheSelectStmt() != null) {
                DataCacheStmtAnalyzer.analyze(statement.getDataCacheSelectStmt(), context);
                taskStmt = statement.getDataCacheSelectStmt();
            } else {
                throw new SemanticException("Submit task statement is not supported");
            }
            boolean hasTemporaryTable = AnalyzerUtils.hasTemporaryTables(taskStmt);
            if (hasTemporaryTable) {
                throw new SemanticException("Cannot submit task based on temporary table");
            }

            OriginStatement origStmt = statement.getOrigStmt();
            String sqlText = origStmt.originStmt.substring(statement.getSqlBeginIndex());
            statement.setSqlText(sqlText);
            TaskAnalyzer.analyzeSubmitTaskStmt(statement, context);
            return null;
        }

        @Override
        public Void visitCreateResourceGroupStatement(CreateResourceGroupStmt statement, ConnectContext session) {
            statement.analyze();
            return null;
        }

        @Override
        public Void visitCreateResourceStatement(CreateResourceStmt stmt, ConnectContext session) {
            ResourceAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitDropResourceStatement(DropResourceStmt stmt, ConnectContext session) {
            ResourceAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitAlterResourceStatement(AlterResourceStmt stmt, ConnectContext session) {
            ResourceAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitShowResourceStatement(ShowResourcesStmt stmt, ConnectContext session) {
            ResourceAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitInsertStatement(InsertStmt statement, ConnectContext session) {
            InsertAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitShowStatement(ShowStmt statement, ConnectContext session) {
            ShowStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitShowDynamicPartitionStatement(ShowDynamicPartitionStmt statement, ConnectContext session) {
            ShowStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitAdminSetConfigStatement(AdminSetConfigStmt adminSetConfigStmt, ConnectContext session) {
            AdminStmtAnalyzer.analyze(adminSetConfigStmt, session);
            return null;
        }

        @Override
        public Void visitSetStatement(SetStmt setStmt, ConnectContext session) {
            SetStmtAnalyzer.analyze(setStmt, session);
            return null;
        }

        @Override
        public Void visitAdminShowConfigStatement(AdminShowConfigStmt adminShowConfigStmt, ConnectContext session) {
            AdminStmtAnalyzer.analyze(adminShowConfigStmt, session);
            return null;
        }

        @Override
        public Void visitDropTableStatement(DropTableStmt statement, ConnectContext session) {
            DropStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitDropTemporaryTableStatement(DropTemporaryTableStmt statement, ConnectContext session) {
            DropStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitQueryStatement(QueryStatement stmt, ConnectContext session) {
            new QueryAnalyzer(session).analyze(stmt);
            return null;
        }

        @Override
        public Void visitUpdateStatement(UpdateStmt node, ConnectContext context) {
            UpdateAnalyzer.analyze(node, context);
            return null;
        }

        @Override
        public Void visitDeleteStatement(DeleteStmt node, ConnectContext context) {
            DeleteAnalyzer.analyze(node, context);
            return null;
        }

        @Override
        public Void visitCreateMaterializedViewStatement(CreateMaterializedViewStatement statement,
                                                         ConnectContext context) {
            MaterializedViewAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCreateMaterializedViewStmt(CreateMaterializedViewStmt statement, ConnectContext context) {
            statement.analyze(context);
            return null;
        }

        @Override
        public Void visitDropMaterializedViewStatement(DropMaterializedViewStmt statement, ConnectContext context) {
            MaterializedViewAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitAlterMaterializedViewStatement(AlterMaterializedViewStmt statement,
                                                        ConnectContext context) {
            MaterializedViewAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitRefreshMaterializedViewStatement(RefreshMaterializedViewStatement statement,
                                                          ConnectContext context) {
            MaterializedViewAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCancelRefreshMaterializedViewStatement(CancelRefreshMaterializedViewStmt statement,
                                                                ConnectContext context) {
            MaterializedViewAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitDropFunctionStatement(DropFunctionStmt statement, ConnectContext context) {
            DropStmtAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCreateFunctionStatement(CreateFunctionStmt statement, ConnectContext context) {
            new CreateFunctionAnalyzer().analyze(statement, context);
            return null;
        }

        @Override
        public Void visitRefreshTableStatement(RefreshTableStmt statement, ConnectContext context) {
            RefreshTableStatementAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitAlterDatabaseQuotaStatement(AlterDatabaseQuotaStmt statement, ConnectContext context) {
            AlterDbQuotaAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCreateDbStatement(CreateDbStmt statement, ConnectContext context) {
            CreateDbAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitDropDbStatement(DropDbStmt statement, ConnectContext context) {
            DropStmtAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitAlterDatabaseRenameStatement(AlterDatabaseRenameStatement statement, ConnectContext context) {
            AlterDatabaseRenameStatementAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitRecoverTableStatement(RecoverTableStmt statement, ConnectContext context) {
            RecoverTableAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitTruncateTableStatement(TruncateTableStmt statement, ConnectContext context) {
            TruncateTableAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitRecoverPartitionStatement(RecoverPartitionStmt statement, ConnectContext context) {
            RecoverPartitionAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCreateRoutineLoadStatement(CreateRoutineLoadStmt statement, ConnectContext session) {
            CreateRoutineLoadAnalyzer.analyze(statement, session);
            return null;
        }

        public Void visitAlterRoutineLoadStatement(AlterRoutineLoadStmt statement, ConnectContext session) {
            AlterRoutineLoadAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitAlterLoadStatement(AlterLoadStmt statement, ConnectContext session) {
            AlterLoadAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitStopRoutineLoadStatement(StopRoutineLoadStmt statement, ConnectContext session) {
            StopRoutineLoadAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitResumeRoutineLoadStatement(ResumeRoutineLoadStmt statement, ConnectContext session) {
            ResumeRoutineLoadAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitPauseRoutineLoadStatement(PauseRoutineLoadStmt statement, ConnectContext session) {
            PauseRoutineLoadAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitAdminSetAutomatedSnapshotOnStatement(AdminSetAutomatedSnapshotOnStmt statement, ConnectContext context) {
            ClusterSnapshotAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitAdminSetAutomatedSnapshotOffStatement(AdminSetAutomatedSnapshotOffStmt statement,
                                                               ConnectContext context) {
            ClusterSnapshotAnalyzer.analyze(statement, context);
            return null;
        }

        // ---------------------------------------- Catalog Statement -------------------------------------------

        @Override
        public Void visitCreateCatalogStatement(CreateCatalogStmt statement, ConnectContext context) {
            CatalogAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitDropCatalogStatement(DropCatalogStmt statement, ConnectContext context) {
            CatalogAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitShowCatalogsStatement(ShowCatalogsStmt statement, ConnectContext context) {
            CatalogAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitUseCatalogStatement(UseCatalogStmt statement, ConnectContext context) {
            CatalogAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitSetCatalogStatement(SetCatalogStmt statement, ConnectContext context) {
            CatalogAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitAlterCatalogStatement(AlterCatalogStmt statement, ConnectContext context) {
            CatalogAnalyzer.analyze(statement, context);
            return null;
        }

        // ------------------------------------------- Cluster Management Statement ----------------------------------------

        @Override
        public Void visitAlterSystemStatement(AlterSystemStmt statement, ConnectContext context) {
            new AlterSystemStmtAnalyzer().analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCancelAlterSystemStatement(CancelAlterSystemStmt statement, ConnectContext context) {
            new AlterSystemStmtAnalyzer().analyze(statement, context);
            return null;
        }

        // ------------------------------------------- Analyze Statement ---------------------------------------------------

        @Override
        public Void visitAnalyzeStatement(AnalyzeStmt statement, ConnectContext session) {
            AnalyzeStmtAnalyzer.analyze(statement, session);
            return null;
        }

        public Void visitCreateAnalyzeJobStatement(CreateAnalyzeJobStmt statement, ConnectContext session) {
            AnalyzeStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitDropStatsStatement(DropStatsStmt statement, ConnectContext session) {
            AnalyzeStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitDropHistogramStatement(DropHistogramStmt statement, ConnectContext session) {
            AnalyzeStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitShowAnalyzeJobStatement(ShowAnalyzeJobStmt statement, ConnectContext session) {
            ShowStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitShowAnalyzeStatusStatement(ShowAnalyzeStatusStmt statement, ConnectContext session) {
            ShowStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitShowBasicStatsMetaStatement(ShowBasicStatsMetaStmt statement, ConnectContext session) {
            ShowStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitShowHistogramStatsMetaStatement(ShowHistogramStatsMetaStmt statement, ConnectContext session) {
            ShowStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitShowTransactionStatement(ShowTransactionStmt statement, ConnectContext session) {
            ShowStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitLoadStatement(LoadStmt statement, ConnectContext context) {
            LoadStmtAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCancelLoadStatement(CancelLoadStmt statement, ConnectContext context) {
            CancelLoadStmtAnalyzer.analyze(statement, context);
            return null;
        }

        // ---------------------------------------- Authentication Statement ------------------------------------------------

        @Override
        public Void visitBaseCreateAlterUserStmt(BaseCreateAlterUserStmt stmt, ConnectContext session) {
            AuthenticationAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitDropUserStatement(DropUserStmt stmt, ConnectContext session) {
            AuthenticationAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitShowAuthenticationStatement(ShowAuthenticationStmt statement, ConnectContext context) {
            AuthenticationAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitExecuteAsStatement(ExecuteAsStmt stmt, ConnectContext session) {
            AuthenticationAnalyzer.analyze(stmt, session);
            return null;
        }

        // ---------------------------------------- Authorization Statement ------------------------------------------------
        @Override
        public Void visitCreateRoleStatement(CreateRoleStmt stmt, ConnectContext session) {
            AuthorizationAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitDropRoleStatement(DropRoleStmt stmt, ConnectContext session) {
            AuthorizationAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitGrantRevokeRoleStatement(BaseGrantRevokeRoleStmt stmt, ConnectContext session) {
            AuthorizationAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitSetRoleStatement(SetRoleStmt stmt, ConnectContext session) {
            AuthorizationAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitSetDefaultRoleStatement(SetDefaultRoleStmt stmt, ConnectContext session) {
            AuthorizationAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitGrantRevokePrivilegeStatement(BaseGrantRevokePrivilegeStmt stmt, ConnectContext session) {
            AuthorizationAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitShowGrantsStatement(ShowGrantsStmt stmt, ConnectContext session) {
            AuthorizationAnalyzer.analyze(stmt, session);
            return null;
        }

        // -------------------------------------- Data Cache Management Statement -----------------------------------------

        @Override
        public Void visitCreateDataCacheRuleStatement(CreateDataCacheRuleStmt stmt, ConnectContext context) {
            DataCacheStmtAnalyzer.analyze(stmt, context);
            return null;
        }

        @Override
        public Void visitShowDataCacheRulesStatement(ShowDataCacheRulesStmt stmt, ConnectContext context) {
            DataCacheStmtAnalyzer.analyze(stmt, context);
            return null;
        }

        @Override
        public Void visitDropDataCacheRuleStatement(DropDataCacheRuleStmt statement, ConnectContext context) {
            DataCacheStmtAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitClearDataCacheRulesStatement(ClearDataCacheRulesStmt statement, ConnectContext context) {
            DataCacheStmtAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitDataCacheSelectStatement(DataCacheSelectStatement statement, ConnectContext context) {
            DataCacheStmtAnalyzer.analyze(statement, context);
            return null;
        }

        // ---------------------------------------- Backup Restore Statement -------------------------------------------

        @Override
        public Void visitBackupStatement(BackupStmt statement, ConnectContext context) {
            BackupRestoreAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitRestoreStatement(RestoreStmt statement, ConnectContext context) {
            BackupRestoreAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitShowBackupStatement(ShowBackupStmt statement, ConnectContext context) {
            BackupRestoreAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitShowRestoreStatement(ShowRestoreStmt statement, ConnectContext context) {
            BackupRestoreAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitShowSnapshotStatement(ShowSnapshotStmt statement, ConnectContext context) {
            ShowSnapshotAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCreateRepositoryStatement(CreateRepositoryStmt statement, ConnectContext context) {
            RepositoryAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitDropRepositoryStatement(DropRepositoryStmt statement, ConnectContext context) {
            RepositoryAnalyzer.analyze(statement, context);
            return null;
        }

        // ------------------------------------ Sql BlackList And WhiteList Statement ----------------------------------

        @Override
        public Void visitAddSqlBlackListStatement(AddSqlBlackListStmt statement, ConnectContext session) {
            statement.analyze();
            return null;
        }

        // ------------------------------------------- Export Statement ------------------------------------------------

        @Override
        public Void visitExportStatement(ExportStmt statement, ConnectContext context) {
            ExportStmtAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitShowExportStatement(ShowExportStmt statement, ConnectContext context) {
            ExportStmtAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCancelExportStatement(CancelExportStmt statement, ConnectContext context) {
            ExportStmtAnalyzer.analyze(statement, context);
            return null;
        }

        // ------------------------------------------- Plugin Statement ------------------------------------------------

        public Void visitInstallPluginStatement(InstallPluginStmt statement, ConnectContext context) {
            PluginAnalyzer.analyze(statement, context);
            return null;
        }

        public Void visitUninstallPluginStatement(UninstallPluginStmt statement, ConnectContext context) {
            PluginAnalyzer.analyze(statement, context);
            return null;
        }

        // --------------------------------------- File Statement ------------------------------------------------------

        public Void visitCreateFileStatement(CreateFileStmt statement, ConnectContext context) {
            FileAnalyzer.analyze(statement, context);
            return null;
        }

        public Void visitDropFileStatement(DropFileStmt statement, ConnectContext context) {
            FileAnalyzer.analyze(statement, context);
            return null;
        }

        public Void visitShowSmallFilesStatement(ShowSmallFilesStmt statement, ConnectContext context) {
            FileAnalyzer.analyze(statement, context);
            return null;
        }

        // ---------------------------------------- Storage Volume Statement -------------------------------------------
        @Override
        public Void visitCreateStorageVolumeStatement(CreateStorageVolumeStmt statement, ConnectContext context) {
            StorageVolumeAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitAlterStorageVolumeStatement(AlterStorageVolumeStmt statement, ConnectContext context) {
            StorageVolumeAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitDropStorageVolumeStatement(DropStorageVolumeStmt statement, ConnectContext context) {
            StorageVolumeAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitShowStorageVolumesStatement(ShowStorageVolumesStmt statement, ConnectContext context) {
            StorageVolumeAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitDescStorageVolumeStatement(DescStorageVolumeStmt statement, ConnectContext context) {
            StorageVolumeAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitSetDefaultStorageVolumeStatement(SetDefaultStorageVolumeStmt statement, ConnectContext context) {
            StorageVolumeAnalyzer.analyze(statement, context);
            return null;
        }

        // -------------------------------------------- Pipe Statement -------------------------------------------------
        @Override
        public Void visitCreatePipeStatement(CreatePipeStmt stmt, ConnectContext context) {
            PipeAnalyzer.analyze(stmt, context);
            return null;
        }

        @Override
        public Void visitDropPipeStatement(DropPipeStmt stmt, ConnectContext context) {
            PipeAnalyzer.analyze(stmt, context);
            return null;
        }

        @Override
        public Void visitAlterPipeStatement(AlterPipeStmt stmt, ConnectContext context) {
            PipeAnalyzer.analyze(stmt, context);
            return null;
        }

        @Override
        public Void visitShowPipeStatement(ShowPipeStmt stmt, ConnectContext context) {
            PipeAnalyzer.analyze(stmt, context);
            return null;
        }

        @Override
        public Void visitDescPipeStatement(DescPipeStmt stmt, ConnectContext context) {
            PipeAnalyzer.analyze(stmt, context);
            return null;
        }

        // ---------------------------------------- Cancel Compaction Statement -------------------------------------------
        @Override
        public Void visitCancelCompactionStatement(CancelCompactionStmt statement, ConnectContext context) {
            CancelCompactionStmtAnalyzer.analyze(statement, context);
            return null;
        }

        // ---------------------------------------- Prepare Statement -------------------------------------------
        @Override
        public Void visitPrepareStatement(PrepareStmt statement, ConnectContext context) {
            new PrepareAnalyzer(context).analyze(statement);
            return null;
        }

        public Void visitExecuteStatement(ExecuteStmt statement, ConnectContext context) {
            new PrepareAnalyzer(context).analyze(statement);
            return null;
        }

        // ---------------------------------------- Dictionary Statement -------------------------------------------
        @Override
        public Void visitCreateDictionaryStatement(CreateDictionaryStmt statement, ConnectContext context) {
            DictionaryAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitDropDictionaryStatement(DropDictionaryStmt statement, ConnectContext context) {
            DictionaryAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitRefreshDictionaryStatement(RefreshDictionaryStmt statement, ConnectContext context) {
            DictionaryAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitShowDictionaryStatement(ShowDictionaryStmt statement, ConnectContext context) {
            DictionaryAnalyzer.analyze(statement, context);
            return null;
        }

        // ---------------------------------------- Warehouse Statement ---------------------------------------------------

        @Override
        public Void visitShowWarehousesStatement(ShowWarehousesStmt statement, ConnectContext context) {
            return null;
        }

        @Override
        public Void visitShowClusterStatement(ShowClustersStmt statement, ConnectContext context) {
            return null;
        }

        @Override
        public Void visitCreateWarehouseStatement(CreateWarehouseStmt statement, ConnectContext context) {
            WarehouseAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitDropWarehouseStatement(DropWarehouseStmt statement, ConnectContext context) {
            WarehouseAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitSuspendWarehouseStatement(SuspendWarehouseStmt statement, ConnectContext context) {
            WarehouseAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitResumeWarehouseStatement(ResumeWarehouseStmt statement, ConnectContext context) {
            WarehouseAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitSetWarehouseStatement(SetWarehouseStmt statement, ConnectContext context) {
            WarehouseAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitShowNodesStatement(ShowNodesStmt statement, ConnectContext context) {
            return null;
        }

        // ---------------------------------------- Translate Statement --------------------------------------------------
        @Override
        public Void visitTranslateStatement(TranslateStmt statement, ConnectContext context) {
            TranslateAnalyzer.analyze(statement, context);
            return null;
        }
    }
}
