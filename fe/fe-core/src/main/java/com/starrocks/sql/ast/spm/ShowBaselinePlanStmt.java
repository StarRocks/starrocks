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

package com.starrocks.sql.ast.spm;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public class ShowBaselinePlanStmt extends ShowStmt {

    public static final Map<String, Type> BASELINE_FIELD_META = ImmutableMap.<String, Type>builder()
            .put("id", Type.BIGINT)
            .put("global", Type.BOOLEAN)
            .put("enable", Type.BOOLEAN)
            .put("bindsqldigest", Type.VARCHAR)
            .put("bindsqlhash", Type.BIGINT)
            .put("bindsql", Type.VARCHAR)
            .put("plansql", Type.VARCHAR)
            .put("costs", Type.DOUBLE)
            .put("queryms", Type.DOUBLE)
            .put("source", Type.VARCHAR)
            .put("updatetime", Type.VARCHAR)
            .build();

    // save where clause
    private final Expr where;

    private final QueryRelation query;

    public ShowBaselinePlanStmt(NodePosition pos, Expr where) {
        super(pos);
        this.where = where;
        this.query = null;
    }

    public ShowBaselinePlanStmt(NodePosition pos, QueryRelation query) {
        super(pos);
        this.where = null;
        this.query = query;
    }

    public QueryRelation getQuery() {
        return query;
    }

    public Expr getWhere() {
        return where;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowBaselinePlanStatement(this, context);
    }
}
