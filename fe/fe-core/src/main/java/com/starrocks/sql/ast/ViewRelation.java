// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.ast;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.View;

public class ViewRelation extends Relation {
    private TableName name;
    private View view;
    private final QueryRelation query;

    public ViewRelation(TableName name, View view, QueryRelation query) {
        this.name = name;
        if (name != null) {
            this.alias = name;
        } else {
            this.alias = null;
        }
        this.view = view;
        this.query = query;
        // The order by is meaningless in subquery
        if (this.query instanceof SelectRelation && !this.query.hasLimit()) {
            SelectRelation qs = (SelectRelation) this.query;
            qs.clearOrder();
        }
    }

    public TableName getName() {
        return name;
    }

    public View getView() {
        return view;
    }

    public QueryRelation getQuery() {
        return query;
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
