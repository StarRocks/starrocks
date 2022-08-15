// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.ast;

import com.starrocks.analysis.AdminSetConfigStmt;
import com.starrocks.analysis.AdminSetReplicaStatusStmt;
import com.starrocks.analysis.AlterSystemStmt;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.AlterViewStmt;
import com.starrocks.analysis.AlterWorkGroupStmt;
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
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CloneExpr;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.CreateIndexClause;
import com.starrocks.analysis.CreateMaterializedViewStmt;
import com.starrocks.analysis.CreateTableAsSelectStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.CreateViewStmt;
import com.starrocks.analysis.CreateWorkGroupStmt;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.DefaultValueExpr;
import com.starrocks.analysis.DeleteStmt;
import com.starrocks.analysis.DropIndexClause;
import com.starrocks.analysis.DropMaterializedViewStmt;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.analysis.DropWorkGroupStmt;
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
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.ParseNode;
<<<<<<< HEAD
=======
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
>>>>>>> ba98870f6 ([Feature] suport SHOW AUTHENTICATION (#9996))
import com.starrocks.analysis.ShowColumnStmt;
import com.starrocks.analysis.ShowDbStmt;
import com.starrocks.analysis.ShowMaterializedViewStmt;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.ShowTableStatusStmt;
import com.starrocks.analysis.ShowTableStmt;
import com.starrocks.analysis.ShowVariablesStmt;
import com.starrocks.analysis.ShowWorkGroupStmt;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.SysVariableDesc;
import com.starrocks.analysis.TableRenameClause;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.analysis.UpdateStmt;

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

    public R visitAlterViewStatement(AlterViewStmt statement, C context) {
        return visitBaseViewStatement(statement, context);
    }

    public R visitAlterWorkGroupStatement(AlterWorkGroupStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitAdminSetConfigStatement(AdminSetConfigStmt statement, C context) {
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

<<<<<<< HEAD
    public R visitAnalyzeStatement(AnalyzeStmt statement, C context) {
=======
    public R visitShowAuthenticationStatement(ShowAuthenticationStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitAdminSetReplicaStatusStatement(AdminSetReplicaStatusStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitAdminShowReplicaStatusStatement(AdminShowReplicaStatusStmt statement, C context) {
>>>>>>> ba98870f6 ([Feature] suport SHOW AUTHENTICATION (#9996))
        return visitStatement(statement, context);
    }

    public R visitAdminSetReplicaStatusStatement(AdminSetReplicaStatusStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitBaseViewStatement(BaseViewStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitCreateAnalyzeJobStatement(CreateAnalyzeJobStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitCreateTableStatement(CreateTableStmt statement, C context) {
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

    public R visitCreateViewStatement(CreateViewStmt statement, C context) {
        return visitBaseViewStatement(statement, context);
    }

    public R visitRefreshTableStatement(RefreshTableStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitCreateWorkGroupStatement(CreateWorkGroupStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitDropWorkGroupStatement(DropWorkGroupStmt statement, C context) {
        return visitDDLStatement(statement, context);
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

    public R visitShowMaterializedViewStmt(ShowMaterializedViewStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowDatabasesStmt(ShowDbStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowWorkGroupStmt(ShowWorkGroupStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitDropTableStmt(DropTableStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowVariablesStmt(ShowVariablesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowColumnStmt(ShowColumnStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowTableStatusStmt(ShowTableStatusStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitDropMaterializedViewStatement(DropMaterializedViewStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitUseStatement(UseStmt statement, C context) {
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


    public R visitFrontendClause(FrontendClause clause, C context) {
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

    public R visitSysVariableDesc(SysVariableDesc node, C context) {
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

}
