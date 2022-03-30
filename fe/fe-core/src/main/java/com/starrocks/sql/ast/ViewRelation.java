// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.ast;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.View;

public class ViewRelation extends Relation {
    private final TableName name;
    private final View view;
    private final QueryStatement queryStatement;

    public ViewRelation(TableName name, View view, QueryStatement queryStatement) {
        this.name = name;
        if (name != null) {
            this.alias = name;
        } else {
            this.alias = null;
        }
        this.view = view;
        this.queryStatement = queryStatement;
        // The order by is meaningless in subquery
        QueryRelation queryRelation = this.queryStatement.getQueryRelation();
        if (!queryRelation.hasLimit()) {
            queryRelation.clearOrder();
        }
    }

    public TableName getName() {
        return name;
    }

    public View getView() {
        return view;
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    @Override
    public String toString() {
        return alias == null ? "anonymous" : alias.toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitView(this, context);
    }
}
