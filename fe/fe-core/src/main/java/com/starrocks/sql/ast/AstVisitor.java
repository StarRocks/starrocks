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

package com.starrocks.sql.ast;

import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.ArraySliceExpr;
import com.starrocks.analysis.ArrowExpr;
import com.starrocks.analysis.BetweenPredicate;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CloneExpr;
import com.starrocks.analysis.CollectionElementExpr;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.ExistsPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.GroupByClause;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.InformationFunction;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MultiInPredicate;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.SubfieldExpr;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.analysis.VariableExpr;
import com.starrocks.connector.parser.trino.PlaceholderExpr;

public abstract class AstVisitor<R, C> {
    public R visit(ParseNode node) {
        return visit(node, null);
    }

    public R visit(ParseNode node, C context) {
        return node.accept(this, context);
    }

    public R visitNode(ParseNode node, C context) {
        return null;
    }

    public R visitStatement(StatementBase statement, C context) {
        return visitNode(statement, context);
    }

    public R visitDDLStatement(DdlStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ---------------------------------------- Query Statement --------------------------------------------------------------

    public R visitQueryStatement(QueryStatement statement, C context) {
        return visitStatement(statement, context);
    }

    // ---------------------------------------- Warehouse Statement ----------------------------------------------------

    public R visitCreateWarehouseStatement(CreateWarehouseStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowWarehousesStatement(ShowWarehousesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitAlterWarehouseStatement(AlterWarehouseStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowClusterStatement(ShowClustersStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitSuspendWarehouseStatement(SuspendWarehouseStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitResumeWarehouseStatement(ResumeWarehouseStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitDropWarehouseStatement(DropWarehouseStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitUseWarehouseStatement(UseWarehouseStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ---------------------------------------- Database Statement -----------------------------------------------------

    public R visitUseDbStatement(UseDbStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitUseCatalogStatement(UseCatalogStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowDatabasesStatement(ShowDbStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitAlterDatabaseQuotaStatement(AlterDatabaseQuotaStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitCreateDbStatement(CreateDbStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitDropDbStatement(DropDbStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowCreateDbStatement(ShowCreateDbStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitAlterDatabaseRenameStatement(AlterDatabaseRenameStatement statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitRecoverDbStatement(RecoverDbStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowDataStatement(ShowDataStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ---------------------------------------- Show Statement ---------------------------------------------------------

    public R visitShowStatement(ShowStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowBackendsStatement(ShowBackendsStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowBrokerStatement(ShowBrokerStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowDeleteStatement(ShowDeleteStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowDynamicPartitionStatement(ShowDynamicPartitionStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowFrontendsStatement(ShowFrontendsStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowTransactionStatement(ShowTransactionStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ---------------------------------------- Table Statement --------------------------------------------------------

    public R visitCreateTableStatement(CreateTableStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitCreateTableAsSelectStatement(CreateTableAsSelectStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitCreateTableLikeStatement(CreateTableLikeStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitDropTableStatement(DropTableStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitRecoverTableStatement(RecoverTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitTruncateTableStatement(TruncateTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitRefreshTableStatement(RefreshTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitAlterTableStatement(AlterTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitCancelAlterTableStatement(CancelAlterTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitShowTableStatement(ShowTableStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitDescTableStmt(DescribeStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowTabletStatement(ShowTabletStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowCreateTableStatement(ShowCreateTableStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ---------------------------------------- View Statement ---------------------------------------------------------

    public R visitCreateViewStatement(CreateViewStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitAlterViewStatement(AlterViewStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ---------------------------------------- Task Statement ---------------------------------------------------------

    public R visitSubmitTaskStatement(SubmitTaskStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitDropTaskStmt(DropTaskStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ---------------------------------------- Partition Statement ----------------------------------------------------

    public R visitRecoverPartitionStatement(RecoverPartitionStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowPartitionsStatement(ShowPartitionsStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ---------------------------------------- Materialized View Statement --------------------------------------------

    public R visitCreateMaterializedViewStatement(CreateMaterializedViewStatement statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitCreateMaterializedViewStmt(CreateMaterializedViewStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitAlterMaterializedViewStatement(AlterMaterializedViewStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitRefreshMaterializedViewStatement(RefreshMaterializedViewStatement statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitCancelRefreshMaterializedViewStatement(CancelRefreshMaterializedViewStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitShowMaterializedViewStatement(ShowMaterializedViewsStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitDropMaterializedViewStatement(DropMaterializedViewStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    // ---------------------------------------- Catalog Statement ------------------------------------------------------

    public R visitCreateCatalogStatement(CreateCatalogStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitDropCatalogStatement(DropCatalogStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowCatalogsStatement(ShowCatalogsStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowCreateExternalCatalogStatement(ShowCreateExternalCatalogStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitSetCatalogStatement(SetCatalogStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ------------------------------------------- DML Statement -------------------------------------------------------

    public R visitInsertStatement(InsertStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitUpdateStatement(UpdateStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitDeleteStatement(DeleteStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ------------------------------------------- Routine Statement ---------------------------------------------------

    public R visitCreateRoutineLoadStatement(CreateRoutineLoadStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitAlterRoutineLoadStatement(AlterRoutineLoadStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitAlterLoadStatement(AlterLoadStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitStopRoutineLoadStatement(StopRoutineLoadStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitResumeRoutineLoadStatement(ResumeRoutineLoadStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitPauseRoutineLoadStatement(PauseRoutineLoadStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowRoutineLoadStatement(ShowRoutineLoadStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowRoutineLoadTaskStatement(ShowRoutineLoadTaskStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowStreamLoadStatement(ShowStreamLoadStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ------------------------------------------- Admin Statement -----------------------------------------------------

    public R visitAdminSetConfigStatement(AdminSetConfigStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitAdminSetReplicaStatusStatement(AdminSetReplicaStatusStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitAdminShowConfigStatement(AdminShowConfigStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitAdminShowReplicaDistributionStatement(AdminShowReplicaDistributionStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitAdminShowReplicaStatusStatement(AdminShowReplicaStatusStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitAdminRepairTableStatement(AdminRepairTableStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitAdminCancelRepairTableStatement(AdminCancelRepairTableStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitAdminCheckTabletsStatement(AdminCheckTabletsStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitKillStatement(KillStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitSyncStatement(SyncStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ---------------------------------------- Cluster Management Statement -------------------------------------------

    public R visitAlterSystemStatement(AlterSystemStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitCancelAlterSystemStatement(CancelAlterSystemStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowComputeNodes(ShowComputeNodesStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ------------------------------------------- Analyze Statement ---------------------------------------------------

    public R visitAnalyzeStatement(AnalyzeStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitCreateAnalyzeJobStatement(CreateAnalyzeJobStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitDropHistogramStatement(DropHistogramStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitDropStatsStatement(DropStatsStmt statsStmt, C context) {
        return visitStatement(statsStmt, context);
    }

    public R visitShowAnalyzeJobStatement(ShowAnalyzeJobStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowAnalyzeStatusStatement(ShowAnalyzeStatusStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowBasicStatsMetaStatement(ShowBasicStatsMetaStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowHistogramStatsMetaStatement(ShowHistogramStatsMetaStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitKillAnalyzeStatement(KillAnalyzeStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitDropAnalyzeStatement(DropAnalyzeJobStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    // ---------------------------------------- Resource Group Statement -----------------------------------------------

    public R visitCreateResourceGroupStatement(CreateResourceGroupStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitDropResourceGroupStatement(DropResourceGroupStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitAlterResourceGroupStatement(AlterResourceGroupStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitShowResourceGroupStatement(ShowResourceGroupStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ---------------------------------------- External Resource Statement---------------------------------------------

    public R visitCreateResourceStatement(CreateResourceStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitDropResourceStatement(DropResourceStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitAlterResourceStatement(AlterResourceStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowResourceStatement(ShowResourcesStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ---------------------------------------- UDF Statement-----------------------------------------------------------

    public R visitShowFunctionsStatement(ShowFunctionsStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitDropFunctionStatement(DropFunctionStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitCreateFunctionStatement(CreateFunctionStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    // ---------------------------------------- LOAD Statement----------------------------------------------------------

    public R visitLoadStatement(LoadStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowLoadStatement(ShowLoadStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowLoadWarningsStatement(ShowLoadWarningsStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitCancelLoadStatement(CancelLoadStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ---------------------------------------- Show Statement ---------------------------------------------------------

    public R visitShowWarningStatement(ShowWarningStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowVariablesStatement(ShowVariablesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowProcStmt(ShowProcStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowProcesslistStatement(ShowProcesslistStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowColumnStatement(ShowColumnStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowTableStatusStatement(ShowTableStatusStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowIndexStatement(ShowIndexStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowAlterStatement(ShowAlterStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowUserPropertyStatement(ShowUserPropertyStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowOpenTableStatement(ShowOpenTableStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ---------------------------------------- Privilege Statement ----------------------------------------------------

    public R visitBaseCreateAlterUserStmt(BaseCreateAlterUserStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitCreateUserStatement(CreateUserStmt statement, C context) {
        return visitBaseCreateAlterUserStmt(statement, context);
    }

    public R visitDropUserStatement(DropUserStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitAlterUserStatement(AlterUserStmt statement, C context) {
        return visitBaseCreateAlterUserStmt(statement, context);
    }

    public R visitShowUserStatement(ShowUserStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowAuthenticationStatement(ShowAuthenticationStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitCreateRoleStatement(CreateRoleStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitDropRoleStatement(DropRoleStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowRolesStatement(ShowRolesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitGrantRevokeRoleStatement(BaseGrantRevokeRoleStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitGrantRoleStatement(GrantRoleStmt statement, C context) {
        return visitGrantRevokeRoleStatement(statement, context);
    }

    public R visitRevokeRoleStatement(RevokeRoleStmt statement, C context) {
        return visitGrantRevokeRoleStatement(statement, context);
    }

    public R visitSetRoleStatement(SetRoleStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitSetDefaultRoleStatement(SetDefaultRoleStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitGrantRevokePrivilegeStatement(BaseGrantRevokePrivilegeStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowGrantsStatement(ShowGrantsStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ---------------------------------------- Backup Restore Statement -----------------------------------------------

    public R visitBackupStatement(BackupStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitRestoreStatement(RestoreStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitShowBackupStatement(ShowBackupStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowRestoreStatement(ShowRestoreStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitCancelBackupStatement(CancelBackupStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitShowSnapshotStatement(ShowSnapshotStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitCreateRepositoryStatement(CreateRepositoryStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitDropRepositoryStatement(DropRepositoryStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    // --------------------------------------- Sql BlackList And WhiteList Statement -----------------------------------

    public R visitAddSqlBlackListStatement(AddSqlBlackListStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitDelSqlBlackListStatement(DelSqlBlackListStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowSqlBlackListStatement(ShowSqlBlackListStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowWhiteListStatement(ShowWhiteListStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitExecuteAsStatement(ExecuteAsStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitExecuteScriptStatement(ExecuteScriptStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // --------------------------------------- Export Statement --------------------------------------------------------

    public R visitExportStatement(ExportStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitCancelExportStatement(CancelExportStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowExportStatement(ShowExportStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // --------------------------------------- Plugin Statement --------------------------------------------------------

    public R visitInstallPluginStatement(InstallPluginStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitUninstallPluginStatement(UninstallPluginStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowPluginsStatement(ShowPluginsStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // --------------------------------------- File Statement ----------------------------------------------------------

    public R visitCreateFileStatement(CreateFileStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitDropFileStatement(DropFileStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowSmallFilesStatement(ShowSmallFilesStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ------------------------------------------  Set Statement -----------------------------------------------------------------

    public R visitSetStatement(SetStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitSetUserPropertyStatement(SetUserPropertyStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ------------------------------------------- Unsupported statement ---------------------------------------------------------

    public R visitUnsupportedStatement(UnsupportedStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ------------------------------------------- Alter Clause --------------------------------------------------------

    //Alter system clause

    public R visitFrontendClause(FrontendClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitCreateImageClause(CreateImageClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitCleanTabletSchedQClause(CleanTabletSchedQClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitModifyFrontendHostClause(ModifyFrontendAddressClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitBackendClause(BackendClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitModifyBackendHostClause(ModifyBackendAddressClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitModifyBrokerClause(ModifyBrokerClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitComputeNodeClause(ComputeNodeClause clause, C context) {
        return visitNode(clause, context);
    }

    //Alter table clause

    public R visitCreateIndexClause(CreateIndexClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitDropIndexClause(DropIndexClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitTableRenameClause(TableRenameClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitSwapTableClause(SwapTableClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitModifyTablePropertiesClause(ModifyTablePropertiesClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitAddColumnClause(AddColumnClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitAddColumnsClause(AddColumnsClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitDropColumnClause(DropColumnClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitModifyColumnClause(ModifyColumnClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitColumnRenameClause(ColumnRenameClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitReorderColumnsClause(ReorderColumnsClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitAddRollupClause(AddRollupClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitDropRollupClause(DropRollupClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitRollupRenameClause(RollupRenameClause clause, C context) {
        return visitNode(clause, context);
    }

    //Alter partition clause

    public R visitModifyPartitionClause(ModifyPartitionClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitAddPartitionClause(AddPartitionClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitDropPartitionClause(DropPartitionClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitTruncatePartitionClause(TruncatePartitionClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitReplacePartitionClause(ReplacePartitionClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitPartitionRenameClause(PartitionRenameClause clause, C context) {
        return visitNode(clause, context);
    }

    // Alter View
    public R visitAlterViewClause(AlterViewClause clause, C context) {
        return visitNode(clause, context);
    }

    // ------------------------------------------- Relation ----------------------------------==------------------------

    public R visitRelation(Relation node, C context) {
        return visitNode(node, context);
    }

    public R visitQueryRelation(QueryRelation node, C context) {
        return visitRelation(node, context);
    }

    public R visitSelect(SelectRelation node, C context) {
        return visitRelation(node, context);
    }

    public R visitTable(TableRelation node, C context) {
        return visitRelation(node, context);
    }

    public R visitJoin(JoinRelation node, C context) {
        return visitRelation(node, context);
    }

    public R visitSubquery(SubqueryRelation node, C context) {
        return visitRelation(node, context);
    }

    public R visitSetOp(SetOperationRelation node, C context) {
        return visitRelation(node, context);
    }

    public R visitUnion(UnionRelation node, C context) {
        return visitSetOp(node, context);
    }

    public R visitExcept(ExceptRelation node, C context) {
        return visitSetOp(node, context);
    }

    public R visitIntersect(IntersectRelation node, C context) {
        return visitSetOp(node, context);
    }

    public R visitValues(ValuesRelation node, C context) {
        return visitRelation(node, context);
    }

    public R visitTableFunction(TableFunctionRelation node, C context) {
        return visitRelation(node, context);
    }

    public R visitCTE(CTERelation node, C context) {
        return visitRelation(node, context);
    }

    public R visitView(ViewRelation node, C context) {
        return visitRelation(node, context);
    }

    // ------------------------------------------- Expression --------------------------------==------------------------

    public R visitExpression(Expr node, C context) {
        return visitNode(node, context);
    }

    public R visitArithmeticExpr(ArithmeticExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitAnalyticExpr(AnalyticExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitArrayExpr(ArrayExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitMapExpr(MapExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitCollectionElementExpr(CollectionElementExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitArraySliceExpr(ArraySliceExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitArrowExpr(ArrowExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitBetweenPredicate(BetweenPredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitBinaryPredicate(BinaryPredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitCaseWhenExpr(CaseExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitCastExpr(CastExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitCompoundPredicate(CompoundPredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitDefaultValueExpr(DefaultValueExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitExistsPredicate(ExistsPredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitFieldReference(FieldReference node, C context) {
        return visitExpression(node, context);
    }

    public R visitFunctionCall(FunctionCallExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitGroupingFunctionCall(GroupingFunctionCallExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitInformationFunction(InformationFunction node, C context) {
        return visitExpression(node, context);
    }

    public R visitInPredicate(InPredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitMultiInPredicate(MultiInPredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitIsNullPredicate(IsNullPredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitLikePredicate(LikePredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitLambdaFunctionExpr(LambdaFunctionExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitLambdaArguments(LambdaArgument node, C context) {
        return visitExpression(node, context);
    }

    public R visitLiteral(LiteralExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitSlot(SlotRef node, C context) {
        return visitExpression(node, context);
    }

    public R visitSubfieldExpr(SubfieldExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitSubquery(Subquery node, C context) {
        return visitExpression(node, context);
    }

    public R visitVariableExpr(VariableExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitTimestampArithmeticExpr(TimestampArithmeticExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitCloneExpr(CloneExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitPlaceholderExpr(PlaceholderExpr node, C context) {
        return visitExpression(node, context);
    }

    // ------------------------------------------- AST ---------------------------------------==------------------------

    public R visitLimitElement(LimitElement node, C context) {
        return null;
    }

    public R visitOrderByElement(OrderByElement node, C context) {
        return null;
    }

    public R visitGroupByClause(GroupByClause node, C context) {
        return null;
    }
}
