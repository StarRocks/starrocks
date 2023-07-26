// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Strings;
import com.starrocks.analysis.TableName;

import java.util.List;

public class CreateViewStmt extends BaseViewStmt {
    private final boolean ifNotExists;
    private final boolean replace;
    private final String comment;

<<<<<<< HEAD
    public CreateViewStmt(boolean ifNotExists, TableName tableName, List<ColWithComment> cols,
                          String comment, QueryStatement queryStmt) {
        super(tableName, cols, queryStmt);
        this.ifNotExists = ifNotExists;
=======
    //Resolved by Analyzer
    protected List<Column> columns;
    private String inlineViewDef;

    public CreateViewStmt(boolean ifNotExists, boolean replace,
                          TableName tableName, List<ColWithComment> colWithComments,
                          String comment,
                          QueryStatement queryStmt,
                          NodePosition pos) {
        super(pos);
        this.ifNotExists = ifNotExists;
        this.replace = replace;
        this.tableName = tableName;
        this.colWithComments = colWithComments;
>>>>>>> 6e1d5ec99b ([Feature] support create or replace view (#27768))
        this.comment = Strings.nullToEmpty(comment);
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public boolean isReplace() {
        return replace;
    }

    public String getComment() {
        return comment;
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateViewStatement(this, context);
    }
}
