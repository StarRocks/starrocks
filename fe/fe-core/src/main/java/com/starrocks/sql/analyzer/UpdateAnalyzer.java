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

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.ColumnAssignment;
import com.starrocks.sql.ast.DefaultValueExpr;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.common.TypeManager;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class UpdateAnalyzer {
    public static void analyze(UpdateStmt updateStmt, ConnectContext session) {
        TableName tableName = updateStmt.getTableName();
        MetaUtils.normalizationTableName(session, tableName);
        MetaUtils.getDatabase(session, tableName);
        Table table = MetaUtils.getTable(session, tableName);

        if (table instanceof MaterializedView) {
            throw new SemanticException(
                    "The data of '%s' cannot be modified because '%s' is a materialized view," +
                            "and the data of materialized view must be consistent with the base table.",
                    tableName.getTbl(), tableName.getTbl());
        }

        if (!table.supportsUpdate()) {
            throw unsupportedException("table " + table.getName() + " does not support update");
        }
        if (updateStmt.getWherePredicate() == null) {
            throw new SemanticException("must specify where clause to prevent full table update");
        }

        List<ColumnAssignment> assignmentList = updateStmt.getAssignments();
        Map<String, ColumnAssignment> assignmentByColName = assignmentList.stream().collect(
                Collectors.toMap(assign -> assign.getColumn().toLowerCase(), a -> a));
        for (String colName : assignmentByColName.keySet()) {
            if (table.getColumn(colName) == null) {
                throw new SemanticException("table '%s' do not existing column '%s'", tableName.getTbl(), colName);
            }
        }
        SelectList selectList = new SelectList();
        for (Column col : table.getBaseSchema()) {
            SelectListItem item;
            ColumnAssignment assign = assignmentByColName.get(col.getName().toLowerCase());
            if (assign != null) {
                if (col.isKey()) {
                    throw new SemanticException("primary key column cannot be updated: " + col.getName());
                }

                if (assign.getExpr() instanceof DefaultValueExpr) {
                    if (!col.isAutoIncrement()) {
                        assign.setExpr(TypeManager.addCastExpr(new StringLiteral(col.calculatedDefaultValue()), col.getType()));
                    } else {
                        updateStmt.setNullExprInAutoIncrement(false);
                        assign.setExpr(TypeManager.addCastExpr(new NullLiteral(), col.getType()));
                    }
                }

                item = new SelectListItem(assign.getExpr(), col.getName());
            } else {
                item = new SelectListItem(new SlotRef(tableName, col.getName()), col.getName());
            }
            selectList.addItem(item);
        }

        Relation relation = new TableRelation(tableName);
        if (updateStmt.getFromRelations() != null) {
            for (Relation r : updateStmt.getFromRelations()) {
                relation = new JoinRelation(null, relation, r, null, false);
            }
        }
        SelectRelation selectRelation =
                new SelectRelation(selectList, relation, updateStmt.getWherePredicate(), null, null);
        if (updateStmt.getCommonTableExpressions() != null) {
            updateStmt.getCommonTableExpressions().forEach(selectRelation::addCTERelation);
        }
        QueryStatement queryStatement = new QueryStatement(selectRelation);
        queryStatement.setIsExplain(updateStmt.isExplain(), updateStmt.getExplainLevel());
        new QueryAnalyzer(session).analyze(queryStatement);

        updateStmt.setTable(table);
        updateStmt.setQueryStatement(queryStatement);

        List<Expr> outputExpression = queryStatement.getQueryRelation().getOutputExpression();
        Preconditions.checkState(outputExpression.size() == table.getBaseSchema().size());
        List<Expr> castOutputExpressions = Lists.newArrayList();
        for (int i = 0; i < table.getBaseSchema().size(); ++i) {
            Expr e = outputExpression.get(i);
            Column c = table.getBaseSchema().get(i);
            castOutputExpressions.add(TypeManager.addCastExpr(e, c.getType()));
        }
        ((SelectRelation) queryStatement.getQueryRelation()).setOutputExpr(castOutputExpressions);
    }
}
