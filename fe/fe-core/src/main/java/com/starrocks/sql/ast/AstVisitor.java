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
<<<<<<< HEAD
=======
import com.starrocks.analysis.DictQueryExpr;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
import com.starrocks.analysis.MultiInPredicate;
import com.starrocks.analysis.OrderByElement;
=======
import com.starrocks.analysis.MatchExpr;
import com.starrocks.analysis.MultiInPredicate;
import com.starrocks.analysis.NamedArgument;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.Parameter;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SetVarHint;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.SubfieldExpr;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.TimestampArithmeticExpr;
<<<<<<< HEAD
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
=======
import com.starrocks.analysis.UserVariableExpr;
import com.starrocks.analysis.UserVariableHint;
import com.starrocks.analysis.VariableExpr;
import com.starrocks.connector.parser.trino.PlaceholderExpr;
import com.starrocks.sql.ShowTemporaryTableStmt;
import com.starrocks.sql.ast.feedback.AddPlanAdvisorStmt;
import com.starrocks.sql.ast.feedback.ClearPlanAdvisorStmt;
import com.starrocks.sql.ast.feedback.DelPlanAdvisorStmt;
import com.starrocks.sql.ast.feedback.ShowPlanAdvisorStmt;
import com.starrocks.sql.ast.pipe.AlterPipeClause;
import com.starrocks.sql.ast.pipe.AlterPipeStmt;
import com.starrocks.sql.ast.pipe.CreatePipeStmt;
import com.starrocks.sql.ast.pipe.DescPipeStmt;
import com.starrocks.sql.ast.pipe.DropPipeStmt;
import com.starrocks.sql.ast.pipe.PipeName;
import com.starrocks.sql.ast.pipe.ShowPipeStmt;
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return visitStatement(statement, context);
    }

    // ---------------------------------------- Query Statement --------------------------------------------------------------

<<<<<<< HEAD
    public R visitQueryStatement(QueryStatement statement, C context) {
        return visitStatement(statement, context);
    }

    // ---------------------------------------- Warehouse Statement ----------------------------------------------------

    public R visitShowWarehousesStatement(ShowWarehousesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowClusterStatement(ShowClustersStmt statement, C context) {
        return visitShowStatement(statement, context);
    }


    // ---------------------------------------- Database Statement -----------------------------------------------------

    public R visitUseDbStatement(UseDbStmt statement, C context) {
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return visitShowStatement(statement, context);
    }

    // ---------------------------------------- Table Statement --------------------------------------------------------

<<<<<<< HEAD
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return visitShowStatement(statement, context);
    }

    // ---------------------------------------- View Statement ---------------------------------------------------------

<<<<<<< HEAD
    public R visitCreateViewStatement(CreateViewStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitAlterViewStatement(AlterViewStmt statement, C context) {
        return visitStatement(statement, context);
=======
    default R visitCreateViewStatement(CreateViewStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAlterViewStatement(AlterViewStmt statement, C context) {
        return visitDDLStatement(statement, context);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    // ---------------------------------------- Task Statement ---------------------------------------------------------

<<<<<<< HEAD
    public R visitSubmitTaskStatement(SubmitTaskStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitDropTaskStmt(DropTaskStmt statement, C context) {
        return visitStatement(statement, context);
=======
    default R visitSubmitTaskStatement(SubmitTaskStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropTaskStmt(DropTaskStmt statement, C context) {
        return visitDDLStatement(statement, context);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    // ---------------------------------------- Partition Statement ----------------------------------------------------

<<<<<<< HEAD
    public R visitRecoverPartitionStatement(RecoverPartitionStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowPartitionsStatement(ShowPartitionsStmt statement, C context) {
=======
    default R visitRecoverPartitionStatement(RecoverPartitionStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowPartitionsStatement(ShowPartitionsStmt statement, C context) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return visitShowStatement(statement, context);
    }

    // ---------------------------------------- Materialized View Statement --------------------------------------------

<<<<<<< HEAD
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return visitDDLStatement(statement, context);
    }

    // ---------------------------------------- Catalog Statement ------------------------------------------------------

<<<<<<< HEAD
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

    public R visitUseCatalogStatement(UseCatalogStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitSetCatalogStatement(SetCatalogStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitAlterCatalogStatement(AlterCatalogStmt statement, C context) {
        return visitStatement(statement, context);
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    // ------------------------------------------- DML Statement -------------------------------------------------------

<<<<<<< HEAD
    public R visitInsertStatement(InsertStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitUpdateStatement(UpdateStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitDeleteStatement(DeleteStmt statement, C context) {
=======
    default R visitInsertStatement(InsertStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitUpdateStatement(UpdateStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitDeleteStatement(DeleteStmt statement, C context) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return visitStatement(statement, context);
    }

    // ------------------------------------------- Routine Statement ---------------------------------------------------

<<<<<<< HEAD
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

    public R visitShowCreateRoutineLoadStatement(ShowCreateRoutineLoadStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowRoutineLoadTaskStatement(ShowRoutineLoadTaskStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowStreamLoadStatement(ShowStreamLoadStmt statement, C context) {
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return visitShowStatement(statement, context);
    }

    // ------------------------------------------- Admin Statement -----------------------------------------------------

<<<<<<< HEAD
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

    public R visitAdminSetPartitionVersionStmt(AdminSetPartitionVersionStmt statement, C context) {
        return visitStatement(statement, context);
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    // ---------------------------------------- Cluster Management Statement -------------------------------------------

<<<<<<< HEAD
    public R visitAlterSystemStatement(AlterSystemStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitCancelAlterSystemStatement(CancelAlterSystemStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowComputeNodes(ShowComputeNodesStmt statement, C context) {
        return visitStatement(statement, context);
=======
    default R visitAlterSystemStatement(AlterSystemStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitCancelAlterSystemStatement(CancelAlterSystemStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowComputeNodes(ShowComputeNodesStmt statement, C context) {
        return visitShowStatement(statement, context);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    // ------------------------------------------- Analyze Statement ---------------------------------------------------

<<<<<<< HEAD
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return visitDDLStatement(statement, context);
    }

    // ---------------------------------------- Analyze Profile Statement ----------------------------------------------

<<<<<<< HEAD
    public R visitAnalyzeProfileStatement(AnalyzeProfileStmt statement, C context) {
=======
    default R visitAnalyzeProfileStatement(AnalyzeProfileStmt statement, C context) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return visitStatement(statement, context);
    }

    // ---------------------------------------- Resource Group Statement -----------------------------------------------

<<<<<<< HEAD
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return visitShowStatement(statement, context);
    }

    // ---------------------------------------- External Resource Statement---------------------------------------------

<<<<<<< HEAD
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    // ---------------------------------------- UDF Statement-----------------------------------------------------------

<<<<<<< HEAD
    public R visitShowFunctionsStatement(ShowFunctionsStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitDropFunctionStatement(DropFunctionStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitCreateFunctionStatement(CreateFunctionStmt statement, C context) {
=======
    default R visitShowFunctionsStatement(ShowFunctionsStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitDropFunctionStatement(DropFunctionStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitCreateFunctionStatement(CreateFunctionStmt statement, C context) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return visitDDLStatement(statement, context);
    }

    // ---------------------------------------- LOAD Statement----------------------------------------------------------

<<<<<<< HEAD
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    // ---------------------------------------- Show Statement ---------------------------------------------------------

<<<<<<< HEAD
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

    public R visitShowProfilelistStatement(ShowProfilelistStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowRunningQueriesStatement(ShowRunningQueriesStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowResourceGroupUsageStatement(ShowResourceGroupUsageStmt statement, C context) {
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return visitShowStatement(statement, context);
    }

    // ---------------------------------------- Authz Statement ----------------------------------------------------

<<<<<<< HEAD
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

    public R visitAlterRoleStatement(AlterRoleStmt statement, C context) {
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

    public R visitCreateSecurityIntegrationStatement(CreateSecurityIntegrationStatement statement, C context) {
        return visitStatement(statement, context);
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    // ---------------------------------------- Backup Restore Statement -----------------------------------------------

<<<<<<< HEAD
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return visitDDLStatement(statement, context);
    }

    // --------------------------------------- Sql BlackList And WhiteList Statement -----------------------------------

<<<<<<< HEAD
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return visitStatement(statement, context);
    }

    // --------------------------------------- Export Statement --------------------------------------------------------

<<<<<<< HEAD
    public R visitExportStatement(ExportStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitCancelExportStatement(CancelExportStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowExportStatement(ShowExportStmt statement, C context) {
        return visitStatement(statement, context);
=======
    default R visitExportStatement(ExportStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitCancelExportStatement(CancelExportStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowExportStatement(ShowExportStmt statement, C context) {
        return visitShowStatement(statement, context);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    // --------------------------------------- Plugin Statement --------------------------------------------------------

<<<<<<< HEAD
    public R visitInstallPluginStatement(InstallPluginStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitUninstallPluginStatement(UninstallPluginStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowPluginsStatement(ShowPluginsStmt statement, C context) {
        return visitStatement(statement, context);
=======
    default R visitInstallPluginStatement(InstallPluginStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitUninstallPluginStatement(UninstallPluginStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowPluginsStatement(ShowPluginsStmt statement, C context) {
        return visitShowStatement(statement, context);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    // --------------------------------------- File Statement ----------------------------------------------------------

<<<<<<< HEAD
    public R visitCreateFileStatement(CreateFileStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitDropFileStatement(DropFileStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowSmallFilesStatement(ShowSmallFilesStmt statement, C context) {
        return visitStatement(statement, context);
=======
    default R visitCreateFileStatement(CreateFileStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropFileStatement(DropFileStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowSmallFilesStatement(ShowSmallFilesStmt statement, C context) {
        return visitShowStatement(statement, context);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    // ------------------------------------------  Set Statement -----------------------------------------------------------------

<<<<<<< HEAD
    public R visitSetStatement(SetStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitSetUserPropertyStatement(SetUserPropertyStmt statement, C context) {
        return visitStatement(statement, context);
=======
    default R visitSetStatement(SetStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitSetUserPropertyStatement(SetUserPropertyStmt statement, C context) {
        return visitDDLStatement(statement, context);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    // ---------------------------------------- Storage Volume Statement ----------------------------------------------------

<<<<<<< HEAD
    public R visitCreateStorageVolumeStatement(CreateStorageVolumeStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowStorageVolumesStatement(ShowStorageVolumesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitAlterStorageVolumeStatement(AlterStorageVolumeStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitDropStorageVolumeStatement(DropStorageVolumeStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitDescStorageVolumeStatement(DescStorageVolumeStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitSetDefaultStorageVolumeStatement(SetDefaultStorageVolumeStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitModifyStorageVolumePropertiesClause(ModifyStorageVolumePropertiesClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitAlterStorageVolumeCommentClause(AlterStorageVolumeCommentClause clause, C context) {
        return visitNode(clause, context);
    }

    // ---------------------------------------- Compaction Statement ----------------------------------------------------

    public R visitCancelCompactionStatement(CancelCompactionStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ------------------------------------------- Unsupported statement ---------------------------------------------------------

    public R visitUnsupportedStatement(UnsupportedStmt statement, C context) {
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return visitStatement(statement, context);
    }

    // ------------------------------------------- Alter Clause --------------------------------------------------------

    //Alter system clause

<<<<<<< HEAD
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return visitNode(clause, context);
    }

    //Alter table clause

<<<<<<< HEAD
    public R visitCreateIndexClause(CreateIndexClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitDropIndexClause(DropIndexClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitTableRenameClause(TableRenameClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitAlterTableCommentClause(AlterTableCommentClause clause, C context) {
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

    public R visitCompactionClause(CompactionClause clause, C context) {
=======
    default R visitCreateIndexClause(CreateIndexClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitDropIndexClause(DropIndexClause clause, C context) {
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return visitNode(clause, context);
    }

    //Alter partition clause

<<<<<<< HEAD
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return visitNode(clause, context);
    }

    // Alter View
<<<<<<< HEAD
    public R visitAlterViewClause(AlterViewClause clause, C context) {
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return visitNode(clause, context);
    }

    // ------------------------------------------- Relation ----------------------------------==------------------------

<<<<<<< HEAD
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

    public R visitNormalizedTableFunction(NormalizedTableFunctionRelation node, C context) {
        return visitRelation(node, context);
    }

    public R visitFileTableFunction(FileTableFunctionRelation node, C context) {
        return visitRelation(node, context);
    }

    public R visitCTE(CTERelation node, C context) {
        return visitRelation(node, context);
    }

    public R visitView(ViewRelation node, C context) {
=======
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

    default R visitSubquery(SubqueryRelation node, C context) {
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return visitRelation(node, context);
    }

    // ------------------------------------------- Expression --------------------------------==------------------------

<<<<<<< HEAD
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

    public R visitHintNode(HintNode node, C context) {
        return visitNode(node, context);
    }

    public R visitSetVarHint(SetVarHint node, C context) {
        return visitNode(node, context);
    }
=======
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

    default R visitSubquery(Subquery node, C context) {
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

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
}
