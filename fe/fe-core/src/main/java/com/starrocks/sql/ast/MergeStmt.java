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

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Table;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
public class MergeStmt extends DmlStmt {
    private TableName tblName;
    public Relation source;
    public Relation target;
    private Expr expression;
    private List<MergeCase> mergeCases;
    private QueryStatement queryStatement;

    private Table table;
    public MergeStmt(Relation target, Relation source, Expr expression, List<MergeCase> mergeCases, NodePosition pos) {
        super(pos);
        this.expression = expression;
        this.target = target;
        this.source = source;
        this.mergeCases = ImmutableList.copyOf(mergeCases);
    }

    public Expr getExpression() {
        return expression;
    }

    public List<MergeCase> getMergeCases() {
        return mergeCases;
    }

    @Override
    public TableName getTableName() {
        return ((TableRelation) target).getName();
    }

    public void setQueryStatement(QueryStatement queryStatement) {
        this.queryStatement = queryStatement;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Table getTable() {
        return table;
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitMergeStatement(this, context);
    }

    @Override
    public String toSql() {
        return "not implemented";
    }
}