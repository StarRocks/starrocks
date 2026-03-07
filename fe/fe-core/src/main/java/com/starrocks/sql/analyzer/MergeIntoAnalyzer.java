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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.TableOperation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ColumnAssignment;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.MergeClause;
import com.starrocks.sql.ast.MergeIntoStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetQualifier;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.common.MetaUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


public class MergeIntoAnalyzer {

    public static void analyze(MergeIntoStmt stmt, ConnectContext session) {
        TableName targetTableName = TableName.fromTableRef(stmt.getTableRef());
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getDb(session, targetTableName.getCatalog(), targetTableName.getDb());
        if (db == null) {
            throw new SemanticException("Database %s is not found", targetTableName.getCatalogAndDb());
        }
        Table table = MetaUtils.getSessionAwareTable(session, null, targetTableName);
        MetaUtils.checkNotSupportCatalog(table, TableOperation.DELETE);

        if (!(table instanceof IcebergTable)) {
            throw new SemanticException("MERGE INTO is only supported for Iceberg tables");
        }
        stmt.setTargetTable(table);
        IcebergTable icebergTable = (IcebergTable) table;

        if (stmt.hasMatchedClauses()) {
            buildDeleteQuery(stmt, icebergTable, targetTableName, session);
        }

        if (stmt.hasUpdateClause() || stmt.hasInsertClause()) {
            buildInsertQuery(stmt, icebergTable, targetTableName, session);
        }
    }

    private static void buildDeleteQuery(MergeIntoStmt stmt, IcebergTable table,
                                         TableName targetTableName, ConnectContext session) {
        TableName colQualifier = getColumnQualifier(stmt, targetTableName);

        SelectList selectList = new SelectList();
        selectList.addItem(new SelectListItem(
                new SlotRef(colQualifier, IcebergTable.FILE_PATH), IcebergTable.FILE_PATH));
        selectList.addItem(new SelectListItem(
                new SlotRef(colQualifier, IcebergTable.ROW_POSITION), IcebergTable.ROW_POSITION));

        List<Column> partitionColumns = table.getPartitionColumns().stream()
                .filter(Objects::nonNull).toList();
        for (Column partCol : partitionColumns) {
            selectList.addItem(new SelectListItem(
                    new SlotRef(colQualifier, partCol.getName()), partCol.getName()));
        }

        TableRelation targetRelation = new TableRelation(targetTableName);
        if (stmt.getTargetAlias() != null) {
            targetRelation.setAlias(new TableName(null, null, stmt.getTargetAlias()));
        }

        JoinRelation joinRelation = new JoinRelation(
                JoinOperator.INNER_JOIN, targetRelation, stmt.getSourceRelation(),
                stmt.getMergeCondition(), false);

        Expr wherePredicate = buildMatchedWherePredicate(stmt);

        SelectRelation selectRelation = new SelectRelation(
                selectList, joinRelation, wherePredicate, null, null);
        QueryStatement queryStatement = new QueryStatement(selectRelation);
        queryStatement.setIsExplain(stmt.isExplain(), stmt.getExplainLevel());

        new QueryAnalyzer(session).analyze(queryStatement);
        stmt.setDeleteQueryStatement(queryStatement);
    }


    private static void buildInsertQuery(MergeIntoStmt stmt, IcebergTable table,
                                         TableName targetTableName, ConnectContext session) {
        List<Column> columns = table.getBaseSchema();
        List<SelectRelation> unionParts = new ArrayList<>();

        if (stmt.hasUpdateClause()) {
            for (MergeClause clause : stmt.getMergeClauses()) {
                if (clause instanceof MergeClause.MergeUpdateClause updateClause) {
                    TableName colQualifier = getColumnQualifier(stmt, targetTableName);

                    SelectList selectList = new SelectList();
                    for (Column col : columns) {
                        Expr colExpr = findUpdateExpr(updateClause, col, colQualifier);
                        selectList.addItem(new SelectListItem(colExpr, col.getName()));
                    }

                    TableRelation targetRelation = new TableRelation(targetTableName);
                    if (stmt.getTargetAlias() != null) {
                        targetRelation.setAlias(new TableName(null, null, stmt.getTargetAlias()));
                    }

                    Relation sourceRelation = cloneSourceRelation(stmt.getSourceRelation());

                    JoinRelation joinRelation = new JoinRelation(
                            JoinOperator.INNER_JOIN, targetRelation, sourceRelation,
                            stmt.getMergeCondition().clone(), false);

                    Expr whereClause = updateClause.getCondition() != null
                            ? updateClause.getCondition().clone() : null;
                    SelectRelation selectRelation = new SelectRelation(
                            selectList, joinRelation, whereClause, null, null);
                    unionParts.add(selectRelation);
                }
            }
        }

        if (stmt.hasInsertClause()) {
            for (MergeClause clause : stmt.getMergeClauses()) {
                if (clause instanceof MergeClause.MergeInsertClause insertClause) {
                    SelectList selectList = new SelectList();
                    List<String> targetCols = insertClause.getTargetColumnNames();
                    List<Expr> values = insertClause.getValueExpressions();

                    if (targetCols != null) {
                        for (Column col : columns) {
                            int idx = findColumnIndex(targetCols, col.getName());
                            if (idx >= 0) {
                                selectList.addItem(new SelectListItem(
                                        values.get(idx).clone(), col.getName()));
                            } else {
                                selectList.addItem(new SelectListItem(
                                        new NullLiteral(), col.getName()));
                            }
                        }
                    } else {
                        if (values.size() != columns.size()) {
                            throw new SemanticException(
                                    "INSERT values count %d doesn't match target column count %d",
                                    values.size(), columns.size());
                        }
                        for (int i = 0; i < values.size(); i++) {
                            selectList.addItem(new SelectListItem(
                                    values.get(i).clone(), columns.get(i).getName()));
                        }
                    }

                    Relation sourceRelation = cloneSourceRelation(stmt.getSourceRelation());
                    TableRelation targetRelation = new TableRelation(targetTableName);
                    if (stmt.getTargetAlias() != null) {
                        targetRelation.setAlias(new TableName(null, null, stmt.getTargetAlias()));
                    }

                    JoinRelation antiJoin = new JoinRelation(
                            JoinOperator.LEFT_ANTI_JOIN, sourceRelation, targetRelation,
                            stmt.getMergeCondition().clone(), false);

                    Expr whereClause = insertClause.getCondition() != null
                            ? insertClause.getCondition().clone() : null;
                    SelectRelation selectRelation = new SelectRelation(
                            selectList, antiJoin, whereClause, null, null);
                    unionParts.add(selectRelation);
                }
            }
        }

        if (unionParts.isEmpty()) {
            return;
        }

        QueryStatement queryStatement;
        if (unionParts.size() == 1) {
            queryStatement = new QueryStatement(unionParts.get(0));
        } else {
            List<SelectRelation> relations = unionParts;
            UnionRelation unionRelation = new UnionRelation(
                    new ArrayList<>(relations), SetQualifier.ALL);
            queryStatement = new QueryStatement(unionRelation);
        }
        queryStatement.setIsExplain(stmt.isExplain(), stmt.getExplainLevel());

        new QueryAnalyzer(session).analyze(queryStatement);
        stmt.setInsertQueryStatement(queryStatement);
    }

    private static Expr buildMatchedWherePredicate(MergeIntoStmt stmt) {
        for (MergeClause clause : stmt.getMergeClauses()) {
            if (clause.isMatched() && clause.getCondition() == null) {
                return null;
            }
        }

        Expr result = null;
        for (MergeClause clause : stmt.getMergeClauses()) {
            if (clause.isMatched() && clause.getCondition() != null) {
                if (result == null) {
                    result = clause.getCondition();
                } else {
                    result = new CompoundPredicate(
                            CompoundPredicate.Operator.OR, result, clause.getCondition());
                }
            }
        }
        return result;
    }

    private static Expr findUpdateExpr(MergeClause.MergeUpdateClause clause, Column col,
                                       TableName colQualifier) {
        for (ColumnAssignment assignment : clause.getAssignments()) {
            if (assignment.getColumn().equalsIgnoreCase(col.getName())) {
                return assignment.getExpr();
            }
        }
        return new SlotRef(colQualifier, col.getName());
    }

    private static int findColumnIndex(List<String> columnNames, String name) {
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equalsIgnoreCase(name)) {
                return i;
            }
        }
        return -1;
    }

    private static TableName getColumnQualifier(MergeIntoStmt stmt, TableName targetTableName) {
        if (stmt.getTargetAlias() != null) {
            return new TableName(null, null, stmt.getTargetAlias());
        }
        return targetTableName;
    }

    private static Relation cloneSourceRelation(Relation source) {
        if (source instanceof TableRelation tr) {
            TableRelation clone = new TableRelation(tr.getName());
            if (tr.getAlias() != null) {
                clone.setAlias(tr.getAlias());
            }
            return clone;
        }
        return source;
    }
}
