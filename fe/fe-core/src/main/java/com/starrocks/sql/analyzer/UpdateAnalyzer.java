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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.planner.IcebergRowDeltaSink;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SelectAnalyzer.RewriteAliasVisitor;
import com.starrocks.sql.ast.ColumnAssignment;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.expression.DefaultValueExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.type.IntegerType;
import com.starrocks.type.NullType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    private static void analyzeProperties(UpdateStmt updateStmt, ConnectContext session) {
        Map<String, String> properties = updateStmt.getProperties();
        properties.put(LoadStmt.MAX_FILTER_RATIO_PROPERTY,
                String.valueOf(session.getSessionVariable().getInsertMaxFilterRatio()));
        properties.put(LoadStmt.STRICT_MODE, String.valueOf(session.getSessionVariable().getEnableInsertStrict()));
        properties.put(LoadStmt.TIMEOUT_PROPERTY, String.valueOf(session.getSessionVariable().getInsertTimeoutS()));
    }

    /**
     * Analyze Iceberg table update statement.
     * For Iceberg update, we convert:
     *   UPDATE table SET col1=expr1, col2=expr2 WHERE condition
     * to:
     *   INSERT INTO iceberg_row_delta_sink
     *   SELECT _file, _pos, partition_col1, ..., new_col1, new_col2, ..., 2 AS op_code
     *   FROM table WHERE condition
     */
    private static void analyzeIcebergTable(UpdateStmt updateStmt, IcebergTable icebergTable,
                                            ConnectContext session) {
        if (icebergTable.getFormatVersion() != 2) {
            throw new SemanticException(
                    "UPDATE is only supported for Iceberg V2 tables, but table format version is "
                            + icebergTable.getFormatVersion());
        }
        if (updateStmt.getWherePredicate() == null) {
            throw new SemanticException("Update must specify where clause to prevent full table update");
        }
        if (updateStmt.getCommonTableExpressions() != null) {
            throw new SemanticException("Update for Iceberg table do not support `with` clause");
        }
        if (updateStmt.getFromRelations() != null) {
            throw new SemanticException("Update for Iceberg table do not support `from` clause");
        }

        // Build assignment map
        List<ColumnAssignment> assignmentList = updateStmt.getAssignments();
        Map<String, ColumnAssignment> assignmentByColName = new HashMap<>();
        for (ColumnAssignment col : assignmentList) {
            assignmentByColName.put(col.getColumn().toLowerCase(), col);
        }

        // Reject partition column updates
        List<Column> partitionColumns = icebergTable.getPartitionColumns().stream()
                .filter(Objects::nonNull).toList();
        for (Column partitionCol : partitionColumns) {
            if (assignmentByColName.containsKey(partitionCol.getName().toLowerCase())) {
                throw new SemanticException("Update for Iceberg table do not support updating partition column: "
                        + partitionCol.getName());
            }
        }

        // Validate assignment columns exist and are writable
        TableName tableName = TableName.fromTableRef(updateStmt.getTableRef());
        for (String colName : assignmentByColName.keySet()) {
            Column col = icebergTable.getColumn(colName);
            if (col == null) {
                throw new SemanticException("table '%s' do not existing column '%s'", tableName.getTbl(), colName);
            }
            if (col.isHidden()) {
                throw new SemanticException("Updating metadata column '%s' is not allowed", colName);
            }
        }

        // Build SELECT list: _file, _pos, partition_cols, full schema (with SET exprs), op_code
        SelectList selectList = new SelectList();

        // Add _file column
        SlotRef filePathColumn = new SlotRef(tableName, IcebergTable.FILE_PATH);
        selectList.addItem(new SelectListItem(filePathColumn, IcebergTable.FILE_PATH));

        // Add _pos column
        SlotRef posColumn = new SlotRef(tableName, IcebergTable.ROW_POSITION);
        selectList.addItem(new SelectListItem(posColumn, IcebergTable.ROW_POSITION));

        // No separate partition prefix — partition columns are already part of the full
        // table schema in the data section below. Adding them here would create duplicate
        // SlotRefs that the optimizer merges, changing the column count and breaking
        // the BE layout assumptions (data_column_start, op_code_index).
        // Partition shuffle uses the partition columns from the data section.

        // Add full target-table schema: for assigned columns use the SET expression,
        // for unassigned columns use SlotRef to keep original value.
        // Skip hidden metadata columns (_file, _pos) — they are already in the prefix
        // and must not appear in the data section written to Parquet files.
        List<Column> dataColumns = icebergTable.getBaseSchema().stream()
                .filter(col -> !col.isHidden())
                .toList();
        for (Column col : dataColumns) {
            ColumnAssignment assign = assignmentByColName.get(col.getName().toLowerCase());
            if (assign != null) {
                Expr assignExpr = assign.getExpr();
                if (assignExpr instanceof DefaultValueExpr) {
                    // Iceberg V2 does not support column default values
                    // (initial-default / write-default are V3 features)
                    throw new SemanticException("DEFAULT value is not supported for Iceberg V2 tables");
                }
                selectList.addItem(new SelectListItem(assignExpr, col.getName()));
            } else {
                selectList.addItem(new SelectListItem(new SlotRef(tableName, col.getName()), col.getName()));
            }
        }

        // Add op_code = OP_UPDATE as the last column
        try {
            selectList.addItem(new SelectListItem(
                    new IntLiteral(IcebergRowDeltaSink.OpCode.UPDATE.value(), IntegerType.TINYINT), "op_code"));
        } catch (Exception e) {
            throw new SemanticException("analyze update failed", e);
        }

        // Create table relation with WHERE predicate
        TableRelation tableRelation = new TableRelation(tableName);
        SelectRelation selectRelation = new SelectRelation(
                selectList,
                tableRelation,
                updateStmt.getWherePredicate(),
                null,
                null
        );

        // Create query statement
        QueryStatement queryStatement = new QueryStatement(selectRelation);
        queryStatement.setIsExplain(updateStmt.isExplain(), updateStmt.getExplainLevel());

        // Analyze the query statement
        new QueryAnalyzer(session).analyze(queryStatement);

        // Cast data columns to target schema types.
        // Layout: [_file, _pos, data_cols..., op_code]
        // Only data_cols need casting — _file/_pos are fixed types, op_code is a literal.
        List<Expr> outputExprs = queryStatement.getQueryRelation().getOutputExpression();
        int dataColumnStart = 2;
        List<Expr> castOutputExprs = Lists.newArrayList(outputExprs);
        for (int i = 0; i < dataColumns.size(); i++) {
            int outputIdx = dataColumnStart + i;
            castOutputExprs.set(outputIdx,
                    TypeManager.addCastExpr(outputExprs.get(outputIdx), dataColumns.get(i).getType()));
        }
        ((SelectRelation) queryStatement.getQueryRelation()).setOutputExpr(castOutputExprs);

        // Pin the SELECT list aliases as the relation's explicit column names so the
        // planner reads them via the standard getColumnOutputNames() path. Without
        // this, QueryRelation falls back to scope-derived names which include hidden
        // metadata columns (_file, _pos) twice for Iceberg.
        ((SelectRelation) queryStatement.getQueryRelation()).setColumnOutputNames(
                selectList.getItems().stream()
                        .map(SelectListItem::getAlias)
                        .collect(Collectors.toList()));

        updateStmt.setTable(icebergTable);
        updateStmt.setQueryStatement(queryStatement);
    }

    public static void analyze(UpdateStmt updateStmt, ConnectContext session) {
        analyzeProperties(updateStmt, session);

        TableRef tableRef = AnalyzerUtils.normalizedTableRef(updateStmt.getTableRef(), session);
        updateStmt.setTableRef(tableRef);
        TableName tableName = TableName.fromTableRef(tableRef);
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getDb(session, tableName.getCatalog(), tableName.getDb());
        if (db == null) {
            throw new SemanticException("Database %s is not found", tableName.getCatalogAndDb());
        }
        Table table = MetaUtils.getSessionAwareTable(session, null, tableName);

        if (table instanceof MaterializedView) {
            throw new SemanticException("The data of '%s' cannot be modified because '%s' is a materialized view,"
                    + "and the data of materialized view must be consistent with the base table.",
                    tableName.getTbl(), tableName.getTbl());
        }

        // Handle Iceberg table UPDATE separately (like DeleteAnalyzer does for DELETE)
        if (table instanceof IcebergTable) {
            analyzeIcebergTable(updateStmt, (IcebergTable) table, session);
            return;
        }

        if (!table.supportsUpdate()) {
            throw unsupportedException("table " + table.getName() + " does not support update");
        }

        List<ColumnAssignment> assignmentList = updateStmt.getAssignments();
        Map<String, ColumnAssignment> assignmentByColName = new HashMap<>();
        for (ColumnAssignment col : assignmentList) {
            assignmentByColName.put(col.getColumn().toLowerCase(), col);
        }
        for (String colName : assignmentByColName.keySet()) {
            if (table.getColumn(colName) == null) {
                throw new SemanticException("table '%s' do not existing column '%s'", tableName.getTbl(), colName);
            }
        }

        if (table.isOlapTable() || table.isCloudNativeTable()) {
            if (session.getSessionVariable().getPartialUpdateMode().equalsIgnoreCase("column")) {
                // use partial update by column
                updateStmt.setUsePartialUpdate();
                if (((OlapTable) table).hasRowStorageType()) {
                    throw new SemanticException("column_with_row table do not support column mode update");
                }
            } else if (session.getSessionVariable().getPartialUpdateMode().equalsIgnoreCase("auto")) {
                // decide by default rules
                if (updateStmt.getWherePredicate() == null) {
                    if (checkIfUsePartialUpdate(assignmentList.size(), table.getBaseSchema().size())) {
                        if (!((OlapTable) table).hasRowStorageType()) {
                            // use partial update if:
                            // 1. Columns updated are less than 4
                            // 2. The proportion of columns updated is less than 30%
                            // 3. No where predicate in update stmt
                            updateStmt.setUsePartialUpdate();
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
        Map<Column, SelectListItem> mcToItem = Maps.newHashMap();
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

                if (col.isAutoIncrement() && assign.getExpr().getType() == NullType.NULL) {
                    nullExprInAutoIncrement = true;
                    break;
                }

                if (col.isGeneratedColumn()) {
                    throw new SemanticException("generated column cannot be updated: " + col.getName());
                }

                if (assign.getExpr() instanceof DefaultValueExpr) {
                    if (!col.isAutoIncrement()) {
                        assign.setExpr(TypeManager.addCastExpr(new StringLiteral(col.calculatedDefaultValue()),
                                col.getType()));
                    } else {
                        assign.setExpr(TypeManager.addCastExpr(new NullLiteral(), col.getType()));
                    }
                }

                item = new SelectListItem(assign.getExpr(), col.getName());
                selectList.addItem(item);
                assignColumnList.add(col);
            } else if (col.isGeneratedColumn()) {
                Expr expr = col.getGeneratedColumnExpr(table.getIdToColumn());
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
            throw new SemanticException(
                    "AUTO_INCREMENT column: " + autoIncrementColumn.getName() + " must not be NULL");
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

            ExpressionAnalyzer.analyzeExpression(orginExpr, new AnalyzeState(),
                    new Scope(RelationId.anonymous(),
                            new RelationFields(
                                    table.getBaseSchema()
                                            .stream()
                                            .map(col -> new Field(col.getName(), col.getType(), tableName, null))
                                            .collect(Collectors.toList()))),
                    session);

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
                    new RelationFields(table.getBaseSchema()
                            .stream()
                            .map(col -> new Field(col.getName(), col.getType(), null, null))
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

            RewriteAliasVisitor visitor = new RewriteAliasVisitor(sourceScope, outputScope, outputExprs, session);

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
