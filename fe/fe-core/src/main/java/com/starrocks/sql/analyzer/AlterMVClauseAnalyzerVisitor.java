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
package com.starrocks.sql.analyzer;

import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AddMVColumnClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LiteralExpr;

public class AlterMVClauseAnalyzerVisitor extends AlterTableClauseAnalyzer {
    public AlterMVClauseAnalyzerVisitor(Table table) {
        super(table);
    }

    public Void visitModifyTablePropertiesClause(ModifyTablePropertiesClause clause, ConnectContext context) {
        //modify properties check in AlterMVJobExecutor
        return null;
    }

    public Void visitAddMVColumnClause(AddMVColumnClause clause, ConnectContext context) {
        // Validate the column name
        String columnName = clause.getColumnName();
        if (columnName == null || columnName.isEmpty()) {
            throw new SemanticException("Column name cannot be empty");
        }
        
        // Validate the aggregate expression
        Expr aggregateExpr = clause.getAggregateExpression();
        if (aggregateExpr == null) {
            throw new SemanticException("Aggregate expression cannot be null");
        }
        
        // For materialized view column add, we validate the expression structure
        // The actual analysis will be done when building/refreshing the MV
        // For now, just check it's not a trivial expression
        if (aggregateExpr instanceof LiteralExpr) {
            throw new SemanticException("Aggregate expression cannot be a constant");
        }
        
        return null;
    }
}
