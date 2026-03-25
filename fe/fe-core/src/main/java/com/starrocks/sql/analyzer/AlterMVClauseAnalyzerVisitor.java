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

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AddMVColumnClause;
import com.starrocks.sql.ast.DropMVColumnClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
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
        if (aggregateExpr instanceof LiteralExpr) {
            throw new SemanticException("Aggregate expression cannot be a constant");
        }
        // Validate aggregate expression - it should contain aggregate functions
        MaterializedView mv = (MaterializedView) table;
        ParseNode astParseNode = mv.getDefineQueryParseNode();
        // chck mv's parse node is a simple QueryStatement
        if (astParseNode == null || !(astParseNode instanceof QueryStatement)) {
            throw new SemanticException("Materialized view definition is invalid");
        }
        QueryStatement queryStatement = (QueryStatement) astParseNode;
        if (queryStatement.getQueryRelation() == null || !(queryStatement.getQueryRelation() instanceof SelectRelation)) {
            throw new SemanticException("Materialized view definition is invalid: only support SELECT statement");
        }
        SelectRelation selectRelation = (SelectRelation) queryStatement.getQueryRelation();
        // only support one table in the ast tree
        if (!(selectRelation.getRelation() instanceof TableRelation)) {
            throw new SemanticException("Materialized view based on multiple tables is not supported");
        }
        // TODO: further validate the aggregate expression against the mv definition
        if (selectRelation.getOutputAnalytic() != null && !selectRelation.getOutputAnalytic().isEmpty()) {
            throw new SemanticException("Materialized view with analytic functions is not supported");
        }
        
        return null;
    }

    public Void visitDropMVColumnClause(DropMVColumnClause clause, ConnectContext context) {
        String columnName = clause.getColumnName();
        if (columnName == null || columnName.isEmpty()) {
            throw new SemanticException("Column name cannot be empty");
        }

        MaterializedView mv = (MaterializedView) table;
        if (mv.getColumn(columnName) == null) {
            throw new SemanticException("Column '{}' does not exist in materialized view", columnName);
        }

        ParseNode astParseNode = mv.getDefineQueryParseNode();
        if (astParseNode == null || !(astParseNode instanceof QueryStatement)) {
            throw new SemanticException("Materialized view definition is invalid");
        }
        QueryStatement queryStatement = (QueryStatement) astParseNode;
        SelectRelation selectRelation = (SelectRelation) queryStatement.getQueryRelation();
        if (!(selectRelation.getRelation() instanceof TableRelation)) {
            throw new SemanticException("Materialized view based on multiple tables is not supported");
        }
        if (selectRelation.getOutputAnalytic() != null && !selectRelation.getOutputAnalytic().isEmpty()) {
            throw new SemanticException("Materialized view with analytic functions is not supported");
        }

        return null;
    }
}
