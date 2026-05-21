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
import com.starrocks.sql.plan.ExecPlan;

import java.util.List;

public class MergeIntoStmt extends DmlStmt {
    private TableRef targetTableRef;
    private final String targetAlias;
    private final Relation sourceRelation;
    private final String sourceAlias;
    private final Expr mergeCondition;
    private final List<MergeClause> mergeClauses;

    // Set during analysis
    private Table targetTable;
    private QueryStatement deleteQueryStatement;
    private QueryStatement insertQueryStatement;

    // Set during planning
    private ExecPlan deletePlan;
    private ExecPlan insertPlan;

    public MergeIntoStmt(TableRef targetTableRef, String targetAlias,
                         Relation sourceRelation, String sourceAlias,
                         Expr mergeCondition, List<MergeClause> mergeClauses,
                         NodePosition pos) {
        super(pos);
        this.targetTableRef = targetTableRef;
        this.targetAlias = targetAlias;
        this.sourceRelation = sourceRelation;
        this.sourceAlias = sourceAlias;
        this.mergeCondition = mergeCondition;
        this.mergeClauses = mergeClauses;
    }

    @Override
    public TableRef getTableRef() {
        return targetTableRef;
    }

    public void setTargetTableRef(TableRef tableRef) {
        this.targetTableRef = tableRef;
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

    public List<MergeClause> getMergeClauses() {
        return mergeClauses;
    }

    public Table getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(Table targetTable) {
        this.targetTable = targetTable;
    }

    public QueryStatement getDeleteQueryStatement() {
        return deleteQueryStatement;
    }

    public void setDeleteQueryStatement(QueryStatement deleteQueryStatement) {
        this.deleteQueryStatement = deleteQueryStatement;
    }

    public QueryStatement getInsertQueryStatement() {
        return insertQueryStatement;
    }

    public void setInsertQueryStatement(QueryStatement insertQueryStatement) {
        this.insertQueryStatement = insertQueryStatement;
    }

    public ExecPlan getDeletePlan() {
        return deletePlan;
    }

    public void setDeletePlan(ExecPlan deletePlan) {
        this.deletePlan = deletePlan;
    }

    public ExecPlan getInsertPlan() {
        return insertPlan;
    }

    public void setInsertPlan(ExecPlan insertPlan) {
        this.insertPlan = insertPlan;
    }

    public boolean hasMatchedClauses() {
        return mergeClauses.stream().anyMatch(MergeClause::isMatched);
    }

    public boolean hasInsertClause() {
        return mergeClauses.stream().anyMatch(c -> c instanceof MergeClause.MergeInsertClause);
    }

    public boolean hasUpdateClause() {
        return mergeClauses.stream().anyMatch(c -> c instanceof MergeClause.MergeUpdateClause);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitMergeIntoStatement(this, context);
    }
}
