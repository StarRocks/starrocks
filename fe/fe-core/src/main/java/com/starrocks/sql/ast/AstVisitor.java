// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.ast;

import com.starrocks.analysis.AddColumnClause;
import com.starrocks.analysis.AddColumnsClause;
import com.starrocks.analysis.AddPartitionClause;
import com.starrocks.analysis.AdminSetConfigStmt;
import com.starrocks.analysis.AdminSetReplicaStatusStmt;
import com.starrocks.analysis.AdminShowConfigStmt;
import com.starrocks.analysis.AdminShowReplicaDistributionStmt;
import com.starrocks.analysis.AdminShowReplicaStatusStmt;
import com.starrocks.analysis.AlterDatabaseQuotaStmt;
import com.starrocks.analysis.AlterDatabaseRename;
import com.starrocks.analysis.AlterLoadStmt;
import com.starrocks.analysis.AlterSystemStmt;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.AlterViewStmt;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.ArrayElementExpr;
import com.starrocks.analysis.ArrayExpr;
import com.starrocks.analysis.ArraySliceExpr;
import com.starrocks.analysis.ArrowExpr;
import com.starrocks.analysis.BackendClause;
import com.starrocks.analysis.BaseViewStmt;
import com.starrocks.analysis.BetweenPredicate;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CancelAlterTableStmt;
import com.starrocks.analysis.CancelLoadStmt;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CloneExpr;
import com.starrocks.analysis.ColumnRenameClause;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.ComputeNodeClause;
import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.CreateFunctionStmt;
import com.starrocks.analysis.CreateIndexClause;
import com.starrocks.analysis.CreateMaterializedViewStmt;
import com.starrocks.analysis.CreateTableAsSelectStmt;
import com.starrocks.analysis.CreateTableLikeStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.CreateViewStmt;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.DefaultValueExpr;
import com.starrocks.analysis.DeleteStmt;
import com.starrocks.analysis.DescribeStmt;
import com.starrocks.analysis.DropColumnClause;
import com.starrocks.analysis.DropDbStmt;
import com.starrocks.analysis.DropFunctionStmt;
import com.starrocks.analysis.DropIndexClause;
import com.starrocks.analysis.DropMaterializedViewStmt;
import com.starrocks.analysis.DropPartitionClause;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.analysis.ExistsPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FrontendClause;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.GroupByClause;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.InformationFunction;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.KillStmt;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.LoadStmt;
import com.starrocks.analysis.ModifyBackendAddressClause;
import com.starrocks.analysis.ModifyColumnClause;
import com.starrocks.analysis.ModifyFrontendAddressClause;
import com.starrocks.analysis.ModifyPartitionClause;
import com.starrocks.analysis.ModifyTablePropertiesClause;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.PauseRoutineLoadStmt;
import com.starrocks.analysis.RecoverDbStmt;
import com.starrocks.analysis.RecoverPartitionStmt;
import com.starrocks.analysis.RecoverTableStmt;
import com.starrocks.analysis.ReorderColumnsClause;
import com.starrocks.analysis.ResumeRoutineLoadStmt;
import com.starrocks.analysis.SetStmt;
import com.starrocks.analysis.SetUserPropertyStmt;
import com.starrocks.analysis.ShowAlterStmt;
import com.starrocks.analysis.ShowAuthenticationStmt;
import com.starrocks.analysis.ShowBrokerStmt;
import com.starrocks.analysis.ShowColumnStmt;
import com.starrocks.analysis.ShowCreateDbStmt;
import com.starrocks.analysis.ShowCreateTableStmt;
import com.starrocks.analysis.ShowDataStmt;
import com.starrocks.analysis.ShowDbStmt;
import com.starrocks.analysis.ShowDeleteStmt;
import com.starrocks.analysis.ShowDynamicPartitionStmt;
import com.starrocks.analysis.ShowFunctionsStmt;
import com.starrocks.analysis.ShowIndexStmt;
import com.starrocks.analysis.ShowLoadStmt;
import com.starrocks.analysis.ShowLoadWarningsStmt;
import com.starrocks.analysis.ShowMaterializedViewStmt;
import com.starrocks.analysis.ShowOpenTableStmt;
import com.starrocks.analysis.ShowPartitionsStmt;
import com.starrocks.analysis.ShowProcStmt;
import com.starrocks.analysis.ShowProcesslistStmt;
import com.starrocks.analysis.ShowRoutineLoadStmt;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.ShowTableStatusStmt;
import com.starrocks.analysis.ShowTableStmt;
import com.starrocks.analysis.ShowTabletStmt;
import com.starrocks.analysis.ShowUserPropertyStmt;
import com.starrocks.analysis.ShowVariablesStmt;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.StopRoutineLoadStmt;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.SwapTableClause;
import com.starrocks.analysis.TableRenameClause;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.analysis.TruncatePartitionClause;
import com.starrocks.analysis.TruncateTableStmt;
import com.starrocks.analysis.UpdateStmt;
import com.starrocks.analysis.VariableExpr;

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

    // ----------------- Statement ---------------

    public R visitStatement(StatementBase statement, C context) {
        return visitNode(statement, context);
    }

    public R visitDDLStatement(DdlStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitAlterSystemStmt(AlterSystemStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitAlterTableStatement(AlterTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitCancelAlterTableStatement(CancelAlterTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitAlterViewStatement(AlterViewStmt statement, C context) {
        return visitBaseViewStatement(statement, context);
    }

    public R visitAlterResourceGroupStatement(AlterResourceGroupStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitAdminSetConfigStatement(AdminSetConfigStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitSetStatement(SetStmt stmt, C context) {
        return visitStatement(stmt, context);
    }

    public R visitAdminShowConfigStatement(AdminShowConfigStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitGrantRevokeRoleStatement(BaseGrantRevokeRoleStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitGrantRevokeImpersonateStatement(BaseGrantRevokeImpersonateStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitExecuteAsStatement(ExecuteAsStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowAuthenticationStatement(ShowAuthenticationStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitAdminSetReplicaStatusStatement(AdminSetReplicaStatusStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitAdminShowReplicaStatusStatement(AdminShowReplicaStatusStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitAdminShowReplicaDistributionStatement(AdminShowReplicaDistributionStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitBaseViewStatement(BaseViewStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitCreateTableStatement(CreateTableStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitCreateTableLikeStatement(CreateTableLikeStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitCreateTableAsSelectStatement(CreateTableAsSelectStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitSubmitTaskStmt(SubmitTaskStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitCreateMaterializedViewStatement(CreateMaterializedViewStatement statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitCreateMaterializedViewStmt(CreateMaterializedViewStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitAlterMaterializedViewStatement(AlterMaterializedViewStatement statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitRefreshMaterializedViewStatement(RefreshMaterializedViewStatement statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitCancelRefreshMaterializedViewStatement(CancelRefreshMaterializedViewStatement statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitCreateViewStatement(CreateViewStmt statement, C context) {
        return visitBaseViewStatement(statement, context);
    }

    public R visitRefreshTableStatement(RefreshTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitCreateResourceGroupStatement(CreateResourceGroupStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitDropResourceGroupStatement(DropResourceGroupStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitShowComputeNodes(ShowComputeNodesStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitQueryStatement(QueryStatement statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitInsertStatement(InsertStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitUpdateStatement(UpdateStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitDeleteStatement(DeleteStmt node, C context) {
        return visitStatement(node, context);
    }

    public R visitShowStatement(ShowStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowTableStmt(ShowTableStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowTabletStmt(ShowTabletStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowCreateTableStmt(ShowCreateTableStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowDeleteStmt(ShowDeleteStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowMaterializedViewStmt(ShowMaterializedViewStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitRecoverTableStatement(RecoverTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitTruncateTableStatement(TruncateTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitShowDatabasesStmt(ShowDbStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowResourceGroupStmt(ShowResourceGroupStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitLoadStmt(LoadStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowLoadStmt(ShowLoadStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowLoadWarningsStmt(ShowLoadWarningsStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitCancelLoadStmt(CancelLoadStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitDropTableStmt(DropTableStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowVariablesStmt(ShowVariablesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowProcesslistStmt(ShowProcesslistStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowBrokerStmt(ShowBrokerStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowColumnStmt(ShowColumnStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowTableStatusStmt(ShowTableStatusStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowIndexStmt(ShowIndexStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowAlterStmt(ShowAlterStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitDropMaterializedViewStatement(DropMaterializedViewStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitUseDbStatement(UseDbStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitUseCatalogStatement(UseCatalogStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowCreateDbStatement(ShowCreateDbStmt statement, C context) {
        return visitShowStatement(statement, context);
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

    public R visitAlterDbQuotaStmt(AlterDatabaseQuotaStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitCreateDbStatement(CreateDbStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitDropDbStatement(DropDbStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitAlterDatabaseRename(AlterDatabaseRename statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowDynamicPartitionStatement(ShowDynamicPartitionStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowDataStmt(ShowDataStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitRecoverDbStmt(RecoverDbStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowUserPropertyStmt(ShowUserPropertyStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitSetUserPropertyStmt(SetUserPropertyStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitKillStatement(KillStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitCreateAlterUserStmt(BaseCreateAlterUserStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitRecoverPartitionStmt(RecoverPartitionStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowPartitionsStmt(ShowPartitionsStmt statement, C context) {
        return visitShowStatement(statement, context);

    }

    public R visitShowFunctions(ShowFunctionsStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitDropFunction(DropFunctionStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitCreateFunction(CreateFunctionStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitShowOpenTableStmt(ShowOpenTableStmt statement, C context) {
        return visitShowStatement(statement, context);
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

    // ----------------- Catalog Clause -------------

    public R visitCreateCatalogStatement(CreateCatalogStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitDropCatalogStatement(DropCatalogStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowCatalogsStmt(ShowCatalogsStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ----------------- Alter Clause ---------------

    public R visitCreateIndexClause(CreateIndexClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitDropIndexClause(DropIndexClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitTableRenameClause(TableRenameClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitBackendClause(BackendClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitComputeNodeClause(ComputeNodeClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitFrontendClause(FrontendClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitModifyFrontendHostClause(ModifyFrontendAddressClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitModifyBackendHostClause(ModifyBackendAddressClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitSwapTableClause(SwapTableClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitModifyTablePropertiesClause(ModifyTablePropertiesClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitDropPartitionClause(DropPartitionClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitTruncatePartitionClause(TruncatePartitionClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitModifyPartitionClause(ModifyPartitionClause clause, C context) {
        return visitNode(clause, context);
    }

    public R visitAddPartitionClause(AddPartitionClause clause, C context) {
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

    // ----------------- Relation ---------------

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

    // ----------------- Expression ---------------

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

    public R visitArrayElementExpr(ArrayElementExpr node, C context) {
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

    public R visitIsNullPredicate(IsNullPredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitLikePredicate(LikePredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitLiteral(LiteralExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitSlot(SlotRef node, C context) {
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
    // ----------------- AST ---------------

    public R visitLimitElement(LimitElement node, C context) {
        return null;
    }

    public R visitOrderByElement(OrderByElement node, C context) {
        return null;
    }

    public R visitGroupByClause(GroupByClause node, C context) {
        return null;
    }

    public R visitDescTableStmt(DescribeStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowProcStmt(ShowProcStmt statement, C context) {
        return visitShowStatement(statement, context);
    }
}
