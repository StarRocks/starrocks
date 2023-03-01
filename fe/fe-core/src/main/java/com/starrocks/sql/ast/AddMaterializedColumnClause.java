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
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.Expr;

import java.util.Map;

public class AddMaterializedColumnClause extends AddColumnClause {
    private String columnName;
    private Expr expression;

    public String getColumnName() {
        return columnName;
    }

    public Expr getExpression() {
        return expression;
    }

    public AddMaterializedColumnClause(ColumnDef columnDef, String columnName, Expr expression,
                                       Map<String, String> properties) {
        super(columnDef, null, null, properties);
        this.columnName = columnName;
        this.expression = expression;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAddMaterializedColumnClause(this, context);
    }

}
