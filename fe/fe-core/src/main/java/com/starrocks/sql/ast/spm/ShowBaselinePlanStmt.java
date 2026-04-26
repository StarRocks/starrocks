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
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.BooleanType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;

import java.util.Map;

public class ShowBaselinePlanStmt extends ShowStmt {

    public static final Map<String, Type> BASELINE_FIELD_META = ImmutableMap.<String, Type>builder()
            .put("id", IntegerType.BIGINT)
            .put("global", BooleanType.BOOLEAN)
            .put("enable", BooleanType.BOOLEAN)
            .put("bindsqldigest", VarcharType.VARCHAR)
            .put("bindsqlhash", IntegerType.BIGINT)
            .put("bindsql", VarcharType.VARCHAR)
            .put("plansql", VarcharType.VARCHAR)
            .put("costs", FloatType.DOUBLE)
            .put("queryms", FloatType.DOUBLE)
            .put("source", VarcharType.VARCHAR)
            .put("updatetime", VarcharType.VARCHAR)
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
