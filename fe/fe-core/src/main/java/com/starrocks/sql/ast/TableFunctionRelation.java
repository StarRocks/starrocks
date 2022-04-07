// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.catalog.TableFunction;

import java.util.List;

/**
 * Table Value Function resolved to relation
 */
public class TableFunctionRelation extends Relation {
    /**
     * functionName is created by parser
     * and will be converted to tableFunction in Analyzer
     */
    private final FunctionName functionName;
    /**
     * functionParams is created by parser
     * and will be converted to childExpressions in Analyzer
     */
    private final FunctionParams functionParams;
    private TableFunction tableFunction;
    private List<Expr> childExpressions;

    public TableFunctionRelation(String functionName, FunctionParams functionParams) {
        this.functionName = new FunctionName(functionName);
        this.functionParams = functionParams;
    }

    public FunctionName getFunctionName() {
        return functionName;
    }

    public FunctionParams getFunctionParams() {
        return functionParams;
    }

    public TableFunction getTableFunction() {
        return tableFunction;
    }

    public void setTableFunction(TableFunction tableFunction) {
        this.tableFunction = tableFunction;
    }

    public List<Expr> getChildExpressions() {
        return childExpressions;
    }

    public void setChildExpressions(List<Expr> childExpressions) {
        this.childExpressions = childExpressions;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitTableFunction(this, context);
    }
}