package com.starrocks.sql.ast;

import com.starrocks.catalog.Table;
import com.starrocks.sql.parser.NodePosition;

public class CreatePipeStmt extends DdlStmt {

    private boolean ifNotExists;
    private String pipeName;
    private InsertStmt insertStmt;
    private Table targetTable;
    private TableFunctionRelation tableFunctionRelation;

    public CreatePipeStmt(boolean ifNotExists, String pipeName, InsertStmt insertStmt, NodePosition pos) {
        super(pos);
        ifNotExists = ifNotExists;
        pipeName = pipeName;
        insertStmt = insertStmt;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public String getPipeName() {
        return pipeName;
    }

    public InsertStmt getInsertStmt() {
        return insertStmt;
    }

    public Table getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(Table targetTable) {
        this.targetTable = targetTable;
    }

    public TableFunctionRelation getTableFunctionRelation() {
        return tableFunctionRelation;
    }

    public void setTableFunctionRelation(TableFunctionRelation tableFunctionRelation) {
        this.tableFunctionRelation = tableFunctionRelation;
    }

    @Override
    public String toSql() {
        return "CREATE PIPE " + pipeName + " AS " + insertStmt.toSql();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreatePipeStatement(this, context);
    }
}
