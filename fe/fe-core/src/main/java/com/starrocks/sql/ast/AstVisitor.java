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
import com.starrocks.analysis.DictQueryExpr;
import com.starrocks.analysis.ExistsPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.GroupByClause;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.HintNode;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.InformationFunction;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MatchExpr;
import com.starrocks.analysis.MultiInPredicate;
import com.starrocks.analysis.NamedArgument;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.Parameter;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SetVarHint;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.SubfieldExpr;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.analysis.UserVariableExpr;
import com.starrocks.analysis.UserVariableHint;
import com.starrocks.analysis.VariableExpr;
import com.starrocks.connector.parser.trino.PlaceholderExpr;
import com.starrocks.sql.ShowTemporaryTableStmt;
import com.starrocks.sql.ast.feedback.AddPlanAdvisorStmt;
import com.starrocks.sql.ast.feedback.ClearPlanAdvisorStmt;
import com.starrocks.sql.ast.feedback.DelPlanAdvisorStmt;
import com.starrocks.sql.ast.feedback.ShowPlanAdvisorStmt;
import com.starrocks.sql.ast.group.CreateGroupProviderStmt;
import com.starrocks.sql.ast.group.DropGroupProviderStmt;
import com.starrocks.sql.ast.group.ShowCreateGroupProviderStmt;
import com.starrocks.sql.ast.group.ShowGroupProvidersStmt;
import com.starrocks.sql.ast.integration.AlterSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.CreateSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.DropSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.ShowCreateSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.ShowSecurityIntegrationStatement;
import com.starrocks.sql.ast.pipe.AlterPipeClause;
import com.starrocks.sql.ast.pipe.AlterPipeStmt;
import com.starrocks.sql.ast.pipe.CreatePipeStmt;
import com.starrocks.sql.ast.pipe.DescPipeStmt;
import com.starrocks.sql.ast.pipe.DropPipeStmt;
import com.starrocks.sql.ast.pipe.PipeName;
import com.starrocks.sql.ast.pipe.ShowPipeStmt;
import com.starrocks.sql.ast.spm.CreateBaselinePlanStmt;
import com.starrocks.sql.ast.spm.DropBaselinePlanStmt;
import com.starrocks.sql.ast.spm.ShowBaselinePlanStmt;
import com.starrocks.sql.ast.translate.TranslateStmt;
import com.starrocks.sql.ast.txn.BeginStmt;
import com.starrocks.sql.ast.txn.CommitStmt;
import com.starrocks.sql.ast.txn.RollbackStmt;
import com.starrocks.sql.ast.warehouse.AlterWarehouseStmt;
import com.starrocks.sql.ast.warehouse.CreateWarehouseStmt;
import com.starrocks.sql.ast.warehouse.DropWarehouseStmt;
import com.starrocks.sql.ast.warehouse.ResumeWarehouseStmt;
import com.starrocks.sql.ast.warehouse.SetWarehouseStmt;
import com.starrocks.sql.ast.warehouse.ShowClustersStmt;
import com.starrocks.sql.ast.warehouse.ShowNodesStmt;
import com.starrocks.sql.ast.warehouse.ShowWarehousesStmt;
import com.starrocks.sql.ast.warehouse.SuspendWarehouseStmt;

public interface AstVisitor<R, C> {
    default R visit(ParseNode node) {
        return visit(node, null);
    }

    default R visit(ParseNode node, C context) {
        return node.accept(this, context);
    }

    default R visitNode(ParseNode node, C context) {
        return null;
    }

    default R visitStatement(StatementBase statement, C context) {
        return visitNode(statement, context);
    }

    default R visitDDLStatement(DdlStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ---------------------------------------- Query Statement --------------------------------------------------------------

    default R visitQueryStatement(QueryStatement statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitPrepareStatement(PrepareStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitExecuteStatement(ExecuteStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitDeallocatePrepareStatement(DeallocateStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ---------------------------------------- Database Statement -----------------------------------------------------

    default R visitUseDbStatement(UseDbStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitShowDatabasesStatement(ShowDbStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitAlterDatabaseQuotaStatement(AlterDatabaseQuotaStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitCreateDbStatement(CreateDbStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropDbStatement(DropDbStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowCreateDbStatement(ShowCreateDbStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitAlterDatabaseRenameStatement(AlterDatabaseRenameStatement statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitRecoverDbStatement(RecoverDbStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowDataStatement(ShowDataStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ---------------------------------------- Table Statement --------------------------------------------------------

    default R visitCreateTableStatement(CreateTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitCreateTableAsSelectStatement(CreateTableAsSelectStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitCreateTableLikeStatement(CreateTableLikeStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitCreateTemporaryTableStatement(CreateTemporaryTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitCreateTemporaryTableAsSelectStatement(CreateTemporaryTableAsSelectStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitCreateTemporaryTableLikeStatement(CreateTemporaryTableLikeStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropTableStatement(DropTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropTemporaryTableStatement(DropTemporaryTableStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitCleanTemporaryTableStatement(CleanTemporaryTableStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitRecoverTableStatement(RecoverTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitTruncateTableStatement(TruncateTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitRefreshTableStatement(RefreshTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAlterTableStatement(AlterTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitCancelAlterTableStatement(CancelAlterTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowTableStatement(ShowTableStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowTemporaryTablesStatement(ShowTemporaryTableStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitDescTableStmt(DescribeStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowTabletStatement(ShowTabletStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowCreateTableStatement(ShowCreateTableStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ---------------------------------------- View Statement ---------------------------------------------------------

    default R visitCreateViewStatement(CreateViewStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAlterViewStatement(AlterViewStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    // ---------------------------------------- Task Statement ---------------------------------------------------------

    default R visitSubmitTaskStatement(SubmitTaskStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropTaskStmt(DropTaskStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    // ---------------------------------------- Partition Statement ----------------------------------------------------

    default R visitRecoverPartitionStatement(RecoverPartitionStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowPartitionsStatement(ShowPartitionsStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ---------------------------------------- Materialized View Statement --------------------------------------------

    default R visitCreateMaterializedViewStatement(CreateMaterializedViewStatement statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitCreateMaterializedViewStmt(CreateMaterializedViewStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAlterMaterializedViewStatement(AlterMaterializedViewStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitRefreshMaterializedViewStatement(RefreshMaterializedViewStatement statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitCancelRefreshMaterializedViewStatement(CancelRefreshMaterializedViewStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowMaterializedViewStatement(ShowMaterializedViewsStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitDropMaterializedViewStatement(DropMaterializedViewStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    // ---------------------------------------- Catalog Statement ------------------------------------------------------

    default R visitCreateCatalogStatement(CreateCatalogStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropCatalogStatement(DropCatalogStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowCatalogsStatement(ShowCatalogsStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowCreateExternalCatalogStatement(ShowCreateExternalCatalogStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitUseCatalogStatement(UseCatalogStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitSetCatalogStatement(SetCatalogStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitAlterCatalogStatement(AlterCatalogStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    // ------------------------------------------- DML Statement -------------------------------------------------------

    default R visitInsertStatement(InsertStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitUpdateStatement(UpdateStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitDeleteStatement(DeleteStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ------------------------------------------- Routine Statement ---------------------------------------------------

    default R visitCreateRoutineLoadStatement(CreateRoutineLoadStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAlterRoutineLoadStatement(AlterRoutineLoadStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAlterLoadStatement(AlterLoadStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitStopRoutineLoadStatement(StopRoutineLoadStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitResumeRoutineLoadStatement(ResumeRoutineLoadStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitPauseRoutineLoadStatement(PauseRoutineLoadStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowRoutineLoadStatement(ShowRoutineLoadStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowCreateRoutineLoadStatement(ShowCreateRoutineLoadStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowRoutineLoadTaskStatement(ShowRoutineLoadTaskStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowStreamLoadStatement(ShowStreamLoadStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ------------------------------------------- Admin Statement -----------------------------------------------------

    default R visitAdminSetConfigStatement(AdminSetConfigStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAdminSetReplicaStatusStatement(AdminSetReplicaStatusStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAdminShowConfigStatement(AdminShowConfigStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitAdminShowReplicaDistributionStatement(AdminShowReplicaDistributionStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitAdminShowReplicaStatusStatement(AdminShowReplicaStatusStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitAdminRepairTableStatement(AdminRepairTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAdminCancelRepairTableStatement(AdminCancelRepairTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAdminCheckTabletsStatement(AdminCheckTabletsStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitKillStatement(KillStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitAdminSetPartitionVersionStmt(AdminSetPartitionVersionStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitSyncStatement(SyncStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAdminSetAutomatedSnapshotOnStatement(AdminSetAutomatedSnapshotOnStmt clause, C context) {
        return visitDDLStatement(clause, context);
    }

    default R visitAdminSetAutomatedSnapshotOffStatement(AdminSetAutomatedSnapshotOffStmt clause, C context) {
        return visitDDLStatement(clause, context);
    }

    // ---------------------------------------- Cluster Management Statement -------------------------------------------

    default R visitAlterSystemStatement(AlterSystemStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitCancelAlterSystemStatement(CancelAlterSystemStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowComputeNodes(ShowComputeNodesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ------------------------------------------- Analyze Statement ---------------------------------------------------

    default R visitAnalyzeStatement(AnalyzeStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitCreateAnalyzeJobStatement(CreateAnalyzeJobStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropHistogramStatement(DropHistogramStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitDropStatsStatement(DropStatsStmt statsStmt, C context) {
        return visitStatement(statsStmt, context);
    }

    default R visitShowAnalyzeJobStatement(ShowAnalyzeJobStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowAnalyzeStatusStatement(ShowAnalyzeStatusStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowBasicStatsMetaStatement(ShowBasicStatsMetaStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowHistogramStatsMetaStatement(ShowHistogramStatsMetaStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitKillAnalyzeStatement(KillAnalyzeStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitDropAnalyzeStatement(DropAnalyzeJobStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    // ---------------------------------------- Analyze Profile Statement ----------------------------------------------

    default R visitAnalyzeProfileStatement(AnalyzeProfileStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ---------------------------------------- Resource Group Statement -----------------------------------------------

    default R visitCreateResourceGroupStatement(CreateResourceGroupStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropResourceGroupStatement(DropResourceGroupStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAlterResourceGroupStatement(AlterResourceGroupStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowResourceGroupStatement(ShowResourceGroupStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ---------------------------------------- External Resource Statement---------------------------------------------

    default R visitCreateResourceStatement(CreateResourceStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropResourceStatement(DropResourceStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAlterResourceStatement(AlterResourceStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowResourceStatement(ShowResourcesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ---------------------------------------- UDF Statement-----------------------------------------------------------

    default R visitShowFunctionsStatement(ShowFunctionsStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitDropFunctionStatement(DropFunctionStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitCreateFunctionStatement(CreateFunctionStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    // ---------------------------------------- LOAD Statement----------------------------------------------------------

    default R visitLoadStatement(LoadStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowLoadStatement(ShowLoadStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowLoadWarningsStatement(ShowLoadWarningsStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitCancelLoadStatement(CancelLoadStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitCancelCompactionStatement(CancelCompactionStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    // ---------------------------------------- Show Statement ---------------------------------------------------------

    default R visitShowStatement(ShowStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitShowAuthorStatement(ShowAuthorStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowBackendsStatement(ShowBackendsStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowBrokerStatement(ShowBrokerStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowDeleteStatement(ShowDeleteStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowDynamicPartitionStatement(ShowDynamicPartitionStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowFrontendsStatement(ShowFrontendsStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowTransactionStatement(ShowTransactionStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitHelpStatement(HelpStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowEnginesStatement(ShowEnginesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowWarningStatement(ShowWarningStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowVariablesStatement(ShowVariablesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowProcStmt(ShowProcStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowProcesslistStatement(ShowProcesslistStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowProfilelistStatement(ShowProfilelistStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowRunningQueriesStatement(ShowRunningQueriesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowResourceGroupUsageStatement(ShowResourceGroupUsageStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowColumnStatement(ShowColumnStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowTableStatusStatement(ShowTableStatusStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowIndexStatement(ShowIndexStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowAlterStatement(ShowAlterStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowUserPropertyStatement(ShowUserPropertyStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowOpenTableStatement(ShowOpenTableStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowCollationStatement(ShowCollationStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowRepositoriesStatement(ShowRepositoriesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowCharsetStatement(ShowCharsetStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowFailPointStatement(ShowFailPointStatement statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowEventStatement(ShowEventsStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowPrivilegeStatement(ShowPrivilegesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowProcedureStatement(ShowProcedureStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowStatusStatement(ShowStatusStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowTriggersStatement(ShowTriggersStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ---------------------------------------- Authz Statement ----------------------------------------------------

    default R visitBaseCreateAlterUserStmt(BaseCreateAlterUserStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitCreateUserStatement(CreateUserStmt statement, C context) {
        return visitBaseCreateAlterUserStmt(statement, context);
    }

    default R visitDropUserStatement(DropUserStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAlterUserStatement(AlterUserStmt statement, C context) {
        return visitBaseCreateAlterUserStmt(statement, context);
    }

    default R visitShowUserStatement(ShowUserStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowAuthenticationStatement(ShowAuthenticationStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitCreateRoleStatement(CreateRoleStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAlterRoleStatement(AlterRoleStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropRoleStatement(DropRoleStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowRolesStatement(ShowRolesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitGrantRevokeRoleStatement(BaseGrantRevokeRoleStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitGrantRoleStatement(GrantRoleStmt statement, C context) {
        return visitGrantRevokeRoleStatement(statement, context);
    }

    default R visitRevokeRoleStatement(RevokeRoleStmt statement, C context) {
        return visitGrantRevokeRoleStatement(statement, context);
    }

    default R visitSetRoleStatement(SetRoleStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitSetDefaultRoleStatement(SetDefaultRoleStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitGrantRevokePrivilegeStatement(BaseGrantRevokePrivilegeStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowGrantsStatement(ShowGrantsStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ------------------------------------------- Security Integration Statement ----------------------------------------------------

    default R visitCreateSecurityIntegrationStatement(CreateSecurityIntegrationStatement statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropSecurityIntegrationStatement(DropSecurityIntegrationStatement statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAlterSecurityIntegrationStatement(AlterSecurityIntegrationStatement statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowCreateSecurityIntegrationStatement(ShowCreateSecurityIntegrationStatement statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowSecurityIntegrationStatement(ShowSecurityIntegrationStatement statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ------------------------------------------- Group Provider Statement ----------------------------------------------------

    default R visitCreateGroupProviderStatement(CreateGroupProviderStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropGroupProviderStatement(DropGroupProviderStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowCreateGroupProviderStatement(ShowCreateGroupProviderStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowGroupProvidersStatement(ShowGroupProvidersStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ---------------------------------------- Backup Restore Statement -----------------------------------------------

    default R visitBackupStatement(BackupStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitRestoreStatement(RestoreStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowBackupStatement(ShowBackupStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowRestoreStatement(ShowRestoreStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitCancelBackupStatement(CancelBackupStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowSnapshotStatement(ShowSnapshotStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitCreateRepositoryStatement(CreateRepositoryStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropRepositoryStatement(DropRepositoryStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    // --------------------------------------- Sql BlackList And WhiteList Statement -----------------------------------

    default R visitAddSqlBlackListStatement(AddSqlBlackListStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitDelSqlBlackListStatement(DelSqlBlackListStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitShowSqlBlackListStatement(ShowSqlBlackListStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowWhiteListStatement(ShowWhiteListStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // --------------------------------------- Backend BlackList -------------------------------------
    default R visitAddBackendBlackListStatement(AddBackendBlackListStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitDelBackendBlackListStatement(DelBackendBlackListStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitShowBackendBlackListStatement(ShowBackendBlackListStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitExecuteAsStatement(ExecuteAsStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitExecuteScriptStatement(ExecuteScriptStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ------------------------------- DataCache Management Statement -------------------------------------------------
    default R visitCreateDataCacheRuleStatement(CreateDataCacheRuleStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowDataCacheRulesStatement(ShowDataCacheRulesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitDropDataCacheRuleStatement(DropDataCacheRuleStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitClearDataCacheRulesStatement(ClearDataCacheRulesStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDataCacheSelectStatement(DataCacheSelectStatement statement, C context) {
        return visitStatement(statement, context);
    }

    // --------------------------------------- Export Statement --------------------------------------------------------

    default R visitExportStatement(ExportStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitCancelExportStatement(CancelExportStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowExportStatement(ShowExportStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // --------------------------------------- Plugin Statement --------------------------------------------------------

    default R visitInstallPluginStatement(InstallPluginStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitUninstallPluginStatement(UninstallPluginStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowPluginsStatement(ShowPluginsStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // --------------------------------------- File Statement ----------------------------------------------------------

    default R visitCreateFileStatement(CreateFileStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropFileStatement(DropFileStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowSmallFilesStatement(ShowSmallFilesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ------------------------------------------  Set Statement -----------------------------------------------------------------

    default R visitSetStatement(SetStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitSetUserPropertyStatement(SetUserPropertyStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    // ---------------------------------------- Storage Volume Statement ----------------------------------------------------

    default R visitCreateStorageVolumeStatement(CreateStorageVolumeStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowStorageVolumesStatement(ShowStorageVolumesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitAlterStorageVolumeStatement(AlterStorageVolumeStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropStorageVolumeStatement(DropStorageVolumeStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDescStorageVolumeStatement(DescStorageVolumeStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitSetDefaultStorageVolumeStatement(SetDefaultStorageVolumeStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitModifyStorageVolumePropertiesClause(ModifyStorageVolumePropertiesClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitAlterStorageVolumeCommentClause(AlterStorageVolumeCommentClause clause, C context) {
        return visitNode(clause, context);
    }

    // -------------------------------------------- Pipe Statement -----------------------------------------------------
    default R visitPipeName(PipeName statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitCreatePipeStatement(CreatePipeStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropPipeStatement(DropPipeStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAlterPipeStatement(AlterPipeStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowPipeStatement(ShowPipeStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitAlterPipeClause(AlterPipeClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitDescPipeStatement(DescPipeStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ---------------------------------------- FailPoint Statement ----------------------------------------------------
    default R visitUpdateFailPointStatusStatement(UpdateFailPointStatusStatement statement, C context) {
        return visitStatement(statement, context);
    }

    // ------------------------------------------- Dictionary Statement ---------------------------------------------------------
    default R visitCreateDictionaryStatement(CreateDictionaryStmt clause, C context) {
        return visitDDLStatement(clause, context);
    }

    default R visitDropDictionaryStatement(DropDictionaryStmt clause, C context) {
        return visitDDLStatement(clause, context);
    }

    default R visitRefreshDictionaryStatement(RefreshDictionaryStmt clause, C context) {
        return visitDDLStatement(clause, context);
    }

    default R visitShowDictionaryStatement(ShowDictionaryStmt clause, C context) {
        return visitShowStatement(clause, context);
    }

    default R visitCancelRefreshDictionaryStatement(CancelRefreshDictionaryStmt clause, C context) {
        return visitDDLStatement(clause, context);
    }

    // ---------------------------------------- Warehouse Statement ----------------------------------------------------

    default R visitShowWarehousesStatement(ShowWarehousesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowClusterStatement(ShowClustersStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitCreateWarehouseStatement(CreateWarehouseStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropWarehouseStatement(DropWarehouseStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitSuspendWarehouseStatement(SuspendWarehouseStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitResumeWarehouseStatement(ResumeWarehouseStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitSetWarehouseStatement(SetWarehouseStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitShowNodesStatement(ShowNodesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitAlterWarehouseStatement(AlterWarehouseStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    // ------------------------------------------- Unsupported statement ---------------------------------------------------------

    default R visitUnsupportedStatement(UnsupportedStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ------------------------------------------- Alter Clause --------------------------------------------------------

    //Alter system clause

    default R visitFrontendClause(FrontendClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitAddFollowerClause(AddFollowerClause clause, C context) {
        return visitFrontendClause(clause, context);
    }

    default R visitDropFollowerClause(DropFollowerClause clause, C context) {
        return visitFrontendClause(clause, context);
    }

    default R visitAddObserverClause(AddObserverClause clause, C context) {
        return visitFrontendClause(clause, context);
    }

    default R visitDropObserverClause(DropObserverClause clause, C context) {
        return visitFrontendClause(clause, context);
    }

    default R visitModifyFrontendHostClause(ModifyFrontendAddressClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitBackendClause(BackendClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitAddBackendClause(AddBackendClause clause, C context) {
        return visitBackendClause(clause, context);
    }

    default R visitDropBackendClause(DropBackendClause clause, C context) {
        return visitBackendClause(clause, context);
    }

    default R visitModifyBackendClause(ModifyBackendClause clause, C context) {
        return visitBackendClause(clause, context);
    }

    default R visitDecommissionBackendClause(DecommissionBackendClause clause, C context) {
        return visitBackendClause(clause, context);
    }

    default R visitModifyBrokerClause(ModifyBrokerClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitComputeNodeClause(ComputeNodeClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitAddComputeNodeClause(AddComputeNodeClause clause, C context) {
        return visitComputeNodeClause(clause, context);
    }

    default R visitDropComputeNodeClause(DropComputeNodeClause clause, C context) {
        return visitComputeNodeClause(clause, context);
    }

    default R visitCreateImageClause(CreateImageClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitCleanTabletSchedQClause(CleanTabletSchedQClause clause, C context) {
        return visitNode(clause, context);
    }

    //Alter table clause

    default R visitCreateIndexClause(CreateIndexClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitDropIndexClause(DropIndexClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitDropPersistentIndexClause(DropPersistentIndexClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitTableRenameClause(TableRenameClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitAlterTableCommentClause(AlterTableCommentClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitSwapTableClause(SwapTableClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitModifyTablePropertiesClause(ModifyTablePropertiesClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitOptimizeClause(OptimizeClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitAddColumnClause(AddColumnClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitAddColumnsClause(AddColumnsClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitDropColumnClause(DropColumnClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitModifyColumnClause(ModifyColumnClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitColumnRenameClause(ColumnRenameClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitReorderColumnsClause(ReorderColumnsClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitAddRollupClause(AddRollupClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitDropRollupClause(DropRollupClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitRollupRenameClause(RollupRenameClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitCompactionClause(CompactionClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitAddFieldClause(AddFieldClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitDropFieldClause(DropFieldClause clause, C context) {
        return visitNode(clause, context);
    }

    //Alter partition clause

    default R visitModifyPartitionClause(ModifyPartitionClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitAddPartitionClause(AddPartitionClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitDropPartitionClause(DropPartitionClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitTruncatePartitionClause(TruncatePartitionClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitReplacePartitionClause(ReplacePartitionClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitPartitionRenameClause(PartitionRenameClause clause, C context) {
        return visitNode(clause, context);
    }

    // Alter View
    default R visitAlterViewClause(AlterViewClause clause, C context) {
        return visitNode(clause, context);
    }

    // Alter Materialized View
    default R visitRefreshSchemeClause(RefreshSchemeClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitAlterMaterializedViewStatusClause(AlterMaterializedViewStatusClause clause, C context) {
        return visitNode(clause, context);
    }

    // ------------------------------------------- Branch/Tag ----------------------------------==------------------------
    default R visitCreateOrReplaceBranchClause(CreateOrReplaceBranchClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitCreateOrReplaceTagClause(CreateOrReplaceTagClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitDropBranchClause(DropBranchClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitDropTagClause(DropTagClause clause, C context) {
        return visitNode(clause, context);
    }

    // ------------------------------------------- Table Operation ----------------------------------==-----------------
    default R visitAlterTableOperationClause(AlterTableOperationClause clause, C context) {
        return visitNode(clause, context);
    }

    // ------------------------------------------- Relation ----------------------------------==------------------------

    default R visitRelation(Relation node, C context) {
        return visitNode(node, context);
    }

    default R visitQueryRelation(QueryRelation node, C context) {
        return visitRelation(node, context);
    }

    default R visitSelect(SelectRelation node, C context) {
        return visitRelation(node, context);
    }

    default R visitTable(TableRelation node, C context) {
        return visitRelation(node, context);
    }

    default R visitJoin(JoinRelation node, C context) {
        return visitRelation(node, context);
    }

    default R visitSubqueryRelation(SubqueryRelation node, C context) {
        return visitRelation(node, context);
    }

    default R visitSetOp(SetOperationRelation node, C context) {
        return visitRelation(node, context);
    }

    default R visitUnion(UnionRelation node, C context) {
        return visitSetOp(node, context);
    }

    default R visitExcept(ExceptRelation node, C context) {
        return visitSetOp(node, context);
    }

    default R visitIntersect(IntersectRelation node, C context) {
        return visitSetOp(node, context);
    }

    default R visitValues(ValuesRelation node, C context) {
        return visitRelation(node, context);
    }

    default R visitTableFunction(TableFunctionRelation node, C context) {
        return visitRelation(node, context);
    }

    default R visitNormalizedTableFunction(NormalizedTableFunctionRelation node, C context) {
        return visitRelation(node, context);
    }

    default R visitFileTableFunction(FileTableFunctionRelation node, C context) {
        return visitRelation(node, context);
    }

    default R visitCTE(CTERelation node, C context) {
        return visitRelation(node, context);
    }

    default R visitView(ViewRelation node, C context) {
        return visitRelation(node, context);
    }

    default R visitPivotRelation(PivotRelation node, C context) {
        return visitRelation(node, context);
    }

    // ------------------------------------------- Expression --------------------------------==------------------------

    default R visitExpression(Expr node, C context) {
        return visitNode(node, context);
    }

    default R visitArithmeticExpr(ArithmeticExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitAnalyticExpr(AnalyticExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitArrayExpr(ArrayExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitMapExpr(MapExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitCollectionElementExpr(CollectionElementExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitArraySliceExpr(ArraySliceExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitArrowExpr(ArrowExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitBetweenPredicate(BetweenPredicate node, C context) {
        return visitExpression(node, context);
    }

    default R visitBinaryPredicate(BinaryPredicate node, C context) {
        return visitExpression(node, context);
    }

    default R visitCaseWhenExpr(CaseExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitCastExpr(CastExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitCompoundPredicate(CompoundPredicate node, C context) {
        return visitExpression(node, context);
    }

    default R visitDefaultValueExpr(DefaultValueExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitExistsPredicate(ExistsPredicate node, C context) {
        return visitExpression(node, context);
    }

    default R visitFieldReference(FieldReference node, C context) {
        return visitExpression(node, context);
    }

    default R visitFunctionCall(FunctionCallExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitGroupingFunctionCall(GroupingFunctionCallExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitInformationFunction(InformationFunction node, C context) {
        return visitExpression(node, context);
    }

    default R visitInPredicate(InPredicate node, C context) {
        return visitExpression(node, context);
    }

    default R visitMultiInPredicate(MultiInPredicate node, C context) {
        return visitExpression(node, context);
    }

    default R visitIsNullPredicate(IsNullPredicate node, C context) {
        return visitExpression(node, context);
    }

    default R visitLikePredicate(LikePredicate node, C context) {
        return visitExpression(node, context);
    }

    default R visitMatchExpr(MatchExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitLambdaFunctionExpr(LambdaFunctionExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitLambdaArguments(LambdaArgument node, C context) {
        return visitExpression(node, context);
    }

    default R visitLiteral(LiteralExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitSlot(SlotRef node, C context) {
        return visitExpression(node, context);
    }

    default R visitSubfieldExpr(SubfieldExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitSubqueryExpr(Subquery node, C context) {
        return visitExpression(node, context);
    }

    default R visitVariableExpr(VariableExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitUserVariableExpr(UserVariableExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitTimestampArithmeticExpr(TimestampArithmeticExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitCloneExpr(CloneExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitPlaceholderExpr(PlaceholderExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitParameterExpr(Parameter node, C context) {
        return visitExpression(node, context);
    }

    default R visitDictionaryGetExpr(DictionaryGetExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitNamedArgument(NamedArgument node, C context) {
        return visitExpression(node, context);
    }

    // ------------------------------------------- Plan Tuning Statement -----------------------------------------------
    default R visitAddPlanAdvisorStatement(AddPlanAdvisorStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitClearPlanAdvisorStatement(ClearPlanAdvisorStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitDelPlanAdvisorStatement(DelPlanAdvisorStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitShowPlanAdvisorStatement(ShowPlanAdvisorStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ---------------------------------------- Transaction Statement --------------------------------------------------

    default R visitBeginStatement(BeginStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitCommitStatement(CommitStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitRollbackStatement(RollbackStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ---------------------------------------- Translate Statement --------------------------------------------------
    default R visitTranslateStatement(TranslateStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ------------------------------------------- AST -----------------------------------------------------------------

    default R visitLimitElement(LimitElement node, C context) {
        return null;
    }

    default R visitOrderByElement(OrderByElement node, C context) {
        return null;
    }

    default R visitGroupByClause(GroupByClause node, C context) {
        return null;
    }

    default R visitDictQueryExpr(DictQueryExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitHintNode(HintNode node, C context) {
        return visitNode(node, context);
    }

    default R visitSetVarHint(SetVarHint node, C context) {
        return visitNode(node, context);
    }

    default R visitUserVariableHint(UserVariableHint node, C context) {
        return visitNode(node, context);
    }

    // -------------------------------------------BaselinePlan -------------------------------------------------------

    default R visitCreateBaselinePlanStatement(CreateBaselinePlanStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropBaselinePlanStatement(DropBaselinePlanStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowBaselinePlanStatement(ShowBaselinePlanStmt statement, C context) {
        return visitShowStatement(statement, context);
    }
}
