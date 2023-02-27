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

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.View;
import com.starrocks.sql.parser.NodePosition;

public class ViewRelation extends Relation {
    private final TableName name;
    private final View view;
    private final QueryStatement queryStatement;

    public ViewRelation(TableName name, View view, QueryStatement queryStatement) {
        this(name, view, queryStatement, NodePosition.ZERO);
    }

    public ViewRelation(TableName name, View view, QueryStatement queryStatement, NodePosition pos) {
        super(pos);
        this.name = name;
        this.view = view;
        this.queryStatement = queryStatement;
        // The order by is meaningless in subquery
        QueryRelation queryRelation = this.queryStatement.getQueryRelation();
        if (!queryRelation.hasLimit()) {
            queryRelation.clearOrder();
        }
    }

    public View getView() {
        return view;
    }

    public TableName getName() {
        return name;
    }

    @Override
    public TableName getResolveTableName() {
        if (alias != null) {
            if (name.getDb() != null) {
                return new TableName(name.getDb(), alias.getTbl());
            } else {
                return alias;
            }
        } else {
            return name;
        }
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
