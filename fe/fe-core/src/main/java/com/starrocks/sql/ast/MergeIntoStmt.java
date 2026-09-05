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

import com.starrocks.catalog.Table;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class MergeIntoStmt extends DmlStmt {
    private TableRef tableRef;
    private final String targetAlias;
    private final Relation sourceRelation;
    private final String sourceAlias;
    private final Expr mergeCondition;
    private final List<MergeWhenClause> whenClauses;

    // Set by analyzer
    private Table table;
    private QueryStatement queryStatement;
    // User-visible columns the analyzer's SELECT produces: [_file, _pos, data_cols...].
    // The trailing routing op_code column is NOT included here; it is added by the
    // planner as a sink-private projection (write_mode = ROW_DELTA_MIXED).
    private List<String> outputColumnNames;
    // Per-row routing expression (CASE over WHEN clauses → op_code TINYINT). Built by
    // the analyzer against the join scope and consumed by the planner.
    private Expr routingExpr;

    public MergeIntoStmt(TableRef tableRef, String targetAlias,
                         Relation sourceRelation, String sourceAlias,
                         Expr mergeCondition, List<MergeWhenClause> whenClauses,
                         NodePosition pos) {
        super(pos);
        this.tableRef = tableRef;
        this.targetAlias = targetAlias;
        this.sourceRelation = sourceRelation;
        this.sourceAlias = sourceAlias;
        this.mergeCondition = mergeCondition;
        this.whenClauses = whenClauses;
    }

    @Override
    public TableRef getTableRef() {
        return tableRef;
    }

    public void setTableRef(TableRef tableRef) {
        this.tableRef = tableRef;
    }

    public String getTargetAlias() {
        return targetAlias;
    }

    public Relation getSourceRelation() {
        return sourceRelation;
    }

    public String getSourceAlias() {
        return sourceAlias;
    }

    public Expr getMergeCondition() {
        return mergeCondition;
    }

    public List<MergeWhenClause> getWhenClauses() {
        return whenClauses;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Table getTable() {
        return table;
    }

    public void setQueryStatement(QueryStatement queryStatement) {
        this.queryStatement = queryStatement;
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    public void setOutputColumnNames(List<String> names) {
        this.outputColumnNames = names;
    }

    public List<String> getOutputColumnNames() {
        return outputColumnNames;
    }

    public void setRoutingExpr(Expr routingExpr) {
        this.routingExpr = routingExpr;
    }

    public Expr getRoutingExpr() {
        return routingExpr;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitMergeIntoStatement(this, context);
    }
}
