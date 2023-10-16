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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SelectAnalyzer.RewriteAliasVisitor;
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

    private static boolean checkIfUsePartialUpdate(int updateColumnCnt, int tableColumnCnt) {
        if (updateColumnCnt <= 3 && updateColumnCnt < tableColumnCnt * 0.3) {
            return true;
        } else {
            return false;
        }
    }

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

        List<ColumnAssignment> assignmentList = updateStmt.getAssignments();
        Map<String, ColumnAssignment> assignmentByColName = assignmentList.stream().collect(
                Collectors.toMap(assign -> assign.getColumn().toLowerCase(), a -> a));
        for (String colName : assignmentByColName.keySet()) {
            if (table.getColumn(colName) == null) {
                throw new SemanticException("table '%s' do not existing column '%s'", tableName.getTbl(), colName);
            }
        }

        if (table.isOlapTable()) {
            if (session.getSessionVariable().getPartialUpdateMode().equals("column")) {
                // use partial update by column
                updateStmt.setUsePartialUpdate();
            } else if (session.getSessionVariable().getPartialUpdateMode().equals("auto")) {
                // decide by default rules
                if (updateStmt.getWherePredicate() == null) {
                    if (checkIfUsePartialUpdate(assignmentList.size(), table.getBaseSchema().size())) {
                        // use partial update if:
                        // 1. Columns updated are less than 4
                        // 2. The proportion of columns updated is less than 30%
                        // 3. No where predicate in update stmt
                        updateStmt.setUsePartialUpdate();
                        if (table instanceof OlapTable && ((OlapTable) table).hasRowStorageType()) {
                            throw new SemanticException("column with row table must specify where clause for update");
                        }
                    }
                }
            }
        }

        if (!updateStmt.usePartialUpdate() && updateStmt.getWherePredicate() == null) {
            throw new SemanticException("must specify where clause to prevent full table update");
        }

        SelectList selectList = new SelectList();
        List<Column> assignColumnList = Lists.newArrayList();
        boolean nullExprInAutoIncrement = false;
        Column autoIncrementColumn = null;
        Map<Column, SelectListItem>  mcToItem = Maps.newHashMap();
        for (Column col : table.getBaseSchema()) {
            SelectListItem item;
            ColumnAssignment assign = assignmentByColName.get(col.getName().toLowerCase());
            if (assign != null) {
                if (col.isKey()) {
                    throw new SemanticException("primary key column cannot be updated: " + col.getName());
                }

                if (col.isAutoIncrement()) {
                    autoIncrementColumn = col;
                }

                if (col.isAutoIncrement() && assign.getExpr().getType() == Type.NULL) {
                    nullExprInAutoIncrement = true;
                    break;
                }

                if (col.isGeneratedColumn()) {
                    throw new SemanticException("generated column cannot be updated: " + col.getName());
                }

                if (assign.getExpr() instanceof DefaultValueExpr) {
                    if (!col.isAutoIncrement()) {
                        assign.setExpr(TypeManager.addCastExpr(new StringLiteral(col.calculatedDefaultValue()), col.getType()));
                    } else {
                        assign.setExpr(TypeManager.addCastExpr(new NullLiteral(), col.getType()));
                    }
                }

                item = new SelectListItem(assign.getExpr(), col.getName());
                selectList.addItem(item);
                assignColumnList.add(col);
            } else if (col.isGeneratedColumn()) {
                Expr expr = col.generatedColumnExpr();
                item = new SelectListItem(expr, col.getName());
                mcToItem.put(col, item);
                selectList.addItem(item);
                assignColumnList.add(col);
            } else if (!updateStmt.usePartialUpdate() || col.isKey()) {
                item = new SelectListItem(new SlotRef(tableName, col.getName()), col.getName());
                selectList.addItem(item);
                assignColumnList.add(col);
            }
        }

        if (autoIncrementColumn != null && nullExprInAutoIncrement) {
            throw new SemanticException("AUTO_INCREMENT column: " + autoIncrementColumn.getName() +
                                        " must not be NULL");
        }

        /*
         * The Substitution here is needed because the generated column
         * needed to be re-calculated by the 'new value' of the ref column,
         * which is the specified expression in UPDATE statment.
         */
        for (Column column : table.getBaseSchema()) {
            if (!column.isGeneratedColumn()) {
                continue;
            }

            SelectListItem item = mcToItem.get(column);
            Expr orginExpr = item.getExpr();

            ExpressionAnalyzer.analyzeExpression(orginExpr,
                new AnalyzeState(), new Scope(RelationId.anonymous(), new RelationFields(
                    table.getBaseSchema().stream().map(col -> new Field(col.getName(), col.getType(),
                        tableName, null)).collect(Collectors.toList()))), session);

            // check if all the expression refers are sepecfied in
            // partial update mode
            if (updateStmt.usePartialUpdate()) {
                List<SlotRef> checkSlots = Lists.newArrayList();
                orginExpr.collect(SlotRef.class, checkSlots);
                int matchCount = 0;
                if (checkSlots.size() != 0) {
                    for (SlotRef slot : checkSlots) {
                        Column refColumn = table.getColumn(slot.getColumnName());
                        for (Column assignColumn : assignColumnList) {
                            if (assignColumn.equals(refColumn)) {
                                ++matchCount;
                                break;
                            }
                        }
                    }
                }
    
                if (matchCount != checkSlots.size()) {
                    throw new SemanticException("All ref Column must be sepecfied in partial update mode");
                }
            }

            List<Expr> outputExprs = Lists.newArrayList();
            for (Column col : table.getBaseSchema()) {
                if (assignmentByColName.get(col.getName()) == null) {
                    outputExprs.add(null);
                } else {
                    outputExprs.add(assignmentByColName.get(col.getName()).getExpr());
                }
            }

            // sourceScope must be set null tableName for its Field in RelationFields
            // because we hope slotRef can not be resolved in sourceScope but can be
            // resolved in outputScope to force to replace the node using outputExprs.
            Scope sourceScope = new Scope(RelationId.anonymous(), 
                                    new RelationFields(table.getBaseSchema().stream().map(col ->
                                        new Field(col.getName(), col.getType(), null, null))
                                            .collect(Collectors.toList())));

            // outputScope should be resolved for the column with assign expr in update statement.
            List<Field> fields = Lists.newArrayList();
            for (Column col : table.getBaseSchema()) {
                if (assignmentByColName.get(col.getName()) == null) {
                    fields.add(new Field(col.getName(), col.getType(), null, null));
                } else {
                    fields.add(new Field(col.getName(), col.getType(), tableName, null));
                }
            }
            Scope outputScope = new Scope(RelationId.anonymous(), new RelationFields(fields));

            RewriteAliasVisitor visitor =
                                new RewriteAliasVisitor(sourceScope, outputScope,
                                    outputExprs, session);

            Expr expr = orginExpr.accept(visitor, null);

            item.setExpr(expr);
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
        Preconditions.checkState(outputExpression.size() == assignColumnList.size());
        if (!updateStmt.usePartialUpdate()) {
            Preconditions.checkState(table.getBaseSchema().size() == assignColumnList.size());   
        }
        List<Expr> castOutputExpressions = Lists.newArrayList();
        for (int i = 0; i < assignColumnList.size(); ++i) {
            Expr e = outputExpression.get(i);
            Column c = assignColumnList.get(i);
            castOutputExpressions.add(TypeManager.addCastExpr(e, c.getType()));
        }
        ((SelectRelation) queryStatement.getQueryRelation()).setOutputExpr(castOutputExpressions);
    }
}
