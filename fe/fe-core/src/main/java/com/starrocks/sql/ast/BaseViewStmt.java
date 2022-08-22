// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.ColWithComment;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;

import java.util.List;

public class BaseViewStmt extends DdlStmt {
    protected final TableName tableName;
    protected final List<ColWithComment> cols;
    protected List<Column> finalCols;
    protected String inlineViewDef;
    protected QueryStatement queryStatement;

    public BaseViewStmt(TableName tableName, List<ColWithComment> cols, QueryStatement queryStmt) {
        Preconditions.checkNotNull(queryStmt);
        this.tableName = tableName;
        this.cols = cols;
        this.queryStatement = queryStmt;
        finalCols = Lists.newArrayList();
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public String getTable() {
        return tableName.getTbl();
    }

    public TableName getTableName() {
        return tableName;
    }

    public List<Column> getColumns() {
        return finalCols;
    }

    public void setFinalCols(List<Column> finalCols) {
        this.finalCols = finalCols;
    }

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    public void setInlineViewDef(String inlineViewDef) {
        this.inlineViewDef = inlineViewDef;
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    public List<ColWithComment> getCols() {
        return cols;
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBaseViewStatement(this, context);
    }
}
