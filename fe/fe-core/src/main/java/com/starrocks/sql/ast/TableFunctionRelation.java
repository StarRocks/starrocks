// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.catalog.TableFunction;

import java.util.List;

/**
 * Table Value Function resolved to relation
 */
public class TableFunctionRelation extends Relation {
    private final TableFunction tableFunction;
    private final List<Expr> childExpressions;

    public TableFunctionRelation(TableFunction tableFunction, List<Expr> childExpressions) {
        this.tableFunction = tableFunction;
        this.childExpressions = childExpressions;
    }

    public TableFunction getTableFunction() {
        return tableFunction;
    }

    public List<Expr> getChildExpressions() {
        return childExpressions;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitTableFunction(this, context);
    }
}