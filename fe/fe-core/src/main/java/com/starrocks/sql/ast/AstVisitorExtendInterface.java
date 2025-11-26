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

import com.starrocks.connector.parser.trino.PlaceholderExpr;
import com.starrocks.sql.ast.expression.AnalyticExpr;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.DefaultValueExpr;
import com.starrocks.sql.ast.expression.DictQueryExpr;
import com.starrocks.sql.ast.expression.DictionaryGetExpr;
import com.starrocks.sql.ast.expression.ExistsPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FieldReference;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.GroupingFunctionCallExpr;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.LambdaArgument;
import com.starrocks.sql.ast.expression.LambdaFunctionExpr;
import com.starrocks.sql.ast.expression.LargeInPredicate;
import com.starrocks.sql.ast.expression.LimitElement;
import com.starrocks.sql.ast.expression.MapExpr;
import com.starrocks.sql.ast.expression.MultiInPredicate;
import com.starrocks.sql.ast.expression.Parameter;
import com.starrocks.sql.ast.expression.PlaceHolderExpr;
import com.starrocks.sql.ast.expression.SetVarHint;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.SubfieldExpr;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.sql.ast.expression.TimestampArithmeticExpr;
import com.starrocks.sql.ast.expression.UserVariableExpr;
import com.starrocks.sql.ast.expression.UserVariableHint;
import com.starrocks.sql.ast.expression.VariableExpr;
import com.starrocks.sql.ast.feedback.AddPlanAdvisorStmt;
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

public interface AstVisitorExtendInterface<R, C> extends AstVisitor<R, C> {

    // ---------------------------------------- Query Statement --------------------------------------------------------------

    default R visitQueryStatement(QueryStatement statement, C context) {
        return visitStatement(statement, context);
    }


    // ---------------------------------------- Database Statement -----------------------------------------------------


    default R visitAlterDatabaseQuotaStatement(AlterDatabaseQuotaStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }


    default R visitAlterDatabaseRenameStatement(AlterDatabaseRenameStatement statement, C context) {
        return visitDDLStatement(statement, context);
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
        return visitCreateTableAsSelectStatement(statement, context);
    }

    default R visitCreateTemporaryTableLikeStatement(CreateTemporaryTableLikeStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropTableStatement(DropTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropTemporaryTableStatement(DropTemporaryTableStmt statement, C context) {
        return visitDropTableStatement(statement, context);
    }

    default R visitRecoverTableStatement(RecoverTableStmt statement, C context) {
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

    default R visitAdminSetReplicaStatusStatement(AdminSetReplicaStatusStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }


    default R visitAdminShowReplicaStatusStatement(AdminShowReplicaStatusStmt statement, C context) {
        return visitShowStatement(statement, context);
    }


    default R visitAdminSetPartitionVersionStmt(AdminSetPartitionVersionStmt statement, C context) {
        return visitDDLStatement(statement, context);
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

    default R visitShowMultiColumnsStatsMetaStatement(ShowMultiColumnStatsMetaStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ---------------------------------------- Resource Group Statement -----------------------------------------------


    // ---------------------------------------- External Resource Statement---------------------------------------------

    default R visitCreateResourceStatement(CreateResourceStmt statement, C context) {
        return visitDDLStatement(statement, context);
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


    // ---------------------------------------- Show Statement ---------------------------------------------------------

    default R visitShowDeleteStatement(ShowDeleteStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowDynamicPartitionStatement(ShowDynamicPartitionStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowTransactionStatement(ShowTransactionStmt statement, C context) {
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

    default R visitShowCollationStatement(ShowCollationStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowCharsetStatement(ShowCharsetStmt statement, C context) {
        return visitShowStatement(statement, context);
    }


    default R visitShowProcedureStatement(ShowProcedureStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowStatusStatement(ShowStatusStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ---------------------------------------- Authz Statement ----------------------------------------------------

    default R visitGrantRevokePrivilegeStatement(BaseGrantRevokePrivilegeStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    // ---------------------------------------- Backup Restore Statement -----------------------------------------------


    default R visitCancelBackupStatement(CancelBackupStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowSnapshotStatement(ShowSnapshotStmt statement, C context) {
        return visitShowStatement(statement, context);
    }


    // ------------------------------- DataCache Management Statement -------------------------------------------------

    default R visitDataCacheSelectStatement(DataCacheSelectStatement statement, C context) {
        return visitDDLStatement(statement, context);
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

    default R visitDescStorageVolumeStatement(DescStorageVolumeStmt statement, C context) {
        return visitShowStatement(statement, context);
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

    //Alter table clause

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

    default R visitAddPartitionColumnClause(AddPartitionColumnClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitDropPartitionColumnClause(DropPartitionColumnClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitModifyColumnClause(ModifyColumnClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitModifyColumnCommentClause(ModifyColumnCommentClause clause, C context) {
        return visitNode(clause, context);
    }


    default R visitReorderColumnsClause(ReorderColumnsClause clause, C context) {
        return visitNode(clause, context);
    }


    default R visitAlterTableModifyDefaultBucketsClause(AlterTableModifyDefaultBucketsClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitAddFieldClause(AddFieldClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitDropFieldClause(DropFieldClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitSplitTabletClause(SplitTabletClause clause, C context) {
        return visitNode(clause, context);
    }

    //Alter partition clause


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


    // Alter View
    default R visitAlterViewClause(AlterViewClause clause, C context) {
        return visitNode(clause, context);
    }

    // Alter Materialized View
    default R visitRefreshSchemeClause(RefreshSchemeClause clause, C context) {
        return visitNode(clause, context);
    }

    // ------------------------------------------- Branch/Tag ----------------------------------==------------------------

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

    // ------------------------------------------- Expression ----------------------------------------
    default R visitExpression(Expr node, C context) {
        return visitNode(node, context);
    }

    // ------------------------------------------- References ----------------------------------------
    default R visitFieldReference(FieldReference node, C context) {
        return visitExpression(node, context);
    }

    default R visitPlaceholderExpr(PlaceholderExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitPlaceHolderExpr(PlaceHolderExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitParameterExpr(Parameter node, C context) {
        return visitExpression(node, context);
    }

    default R visitSlot(SlotRef node, C context) {
        return visitExpression(node, context);
    }

    // ------------------------------------------- Functions ----------------------------------------
    default R visitFunctionCall(FunctionCallExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitGroupingFunctionCall(GroupingFunctionCallExpr node, C context) {
        return visitFunctionCall(node, context);
    }

    // ------------------------------------------- Collections --------------------------------------


    default R visitMapExpr(MapExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitSubfieldExpr(SubfieldExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitBinaryPredicate(BinaryPredicate node, C context) {
        return visitPredicate(node, context);
    }

    default R visitCompoundPredicate(CompoundPredicate node, C context) {
        return visitPredicate(node, context);
    }

    default R visitExistsPredicate(ExistsPredicate node, C context) {
        return visitPredicate(node, context);
    }

    default R visitInPredicate(InPredicate node, C context) {
        return visitPredicate(node, context);
    }

    default R visitLargeInPredicate(LargeInPredicate node, C context) {
        return visitInPredicate(node, context);
    }

    default R visitMultiInPredicate(MultiInPredicate node, C context) {
        return visitPredicate(node, context);
    }

    // ------------------------------------------- Literal ------------------------------------------

    default R visitDefaultValueExpr(DefaultValueExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitDecimalLiteral(DecimalLiteral node, C context) {
        return visitLiteral(node, context);
    }

    // ------------------------------------------- Lambda -----------------------------------------
    default R visitLambdaArguments(LambdaArgument node, C context) {
        return visitExpression(node, context);
    }

    default R visitLambdaFunctionExpr(LambdaFunctionExpr node, C context) {
        return visitExpression(node, context);
    }

    // ------------------------------------------- Dict -------------------------------------------
    default R visitDictionaryGetExpr(DictionaryGetExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitDictQueryExpr(DictQueryExpr node, C context) {
        return visitExpression(node, context);
    }

    // ------------------------------------------- Others -------------------------------------------
    default R visitArithmeticExpr(ArithmeticExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitAnalyticExpr(AnalyticExpr node, C context) {
        return visitExpression(node, context);
    }

    default R visitCastExpr(CastExpr node, C context) {
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

    // ------------------------------------------- Plan Tuning Statement -----------------------------------------------
    default R visitAddPlanAdvisorStatement(AddPlanAdvisorStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ---------------------------------------- Translate Statement --------------------------------------------------

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

    // ------------------------------------------- Procedure Statement -------------------------------------------------

    default R visitCallProcedureStatement(CallProcedureStatement statement, C context) {
        return visitStatement(statement, context);
    }
}
