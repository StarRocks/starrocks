// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.SelectList;
import com.starrocks.analysis.SelectListItem;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.UpdateStmt;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.ColumnAssignment;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.common.MetaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class UpdateAnalyzer {
    private static final Logger LOG = LogManager.getLogger(UpdateAnalyzer.class);

    public static void analyze(UpdateStmt updateStmt, ConnectContext session) {
        TableName tableName = updateStmt.getTableName();
        MetaUtils.normalizationTableName(session, tableName);
        MetaUtils.getStarRocks(session, tableName);
        Table table = MetaUtils.getStarRocksTable(session, tableName);

        if (!(table instanceof OlapTable && ((OlapTable) table).getKeysType() == KeysType.PRIMARY_KEYS)) {
            throw unsupportedException("only support updating primary key table");
        }
        if (updateStmt.getWherePredicate() == null) {
            throw new SemanticException("must specify where clause to prevent full table update");
        }

        List<ColumnAssignment> assignmentList = updateStmt.getAssignments();
        Map<String, ColumnAssignment> assignmentByColName = assignmentList.stream().collect(
                Collectors.toMap(assign -> assign.getColumn().toLowerCase(), a -> a));

        SelectList selectList = new SelectList();
        for (Column col : table.getBaseSchema()) {
            SelectListItem item;
            ColumnAssignment assign = assignmentByColName.get(col.getName().toLowerCase());
            if (assign != null) {
                if (col.isKey()) {
                    throw new SemanticException("primary key column cannot be updated: " + col.getName());
                }
                item = new SelectListItem(new CastExpr(col.getType(), assign.getExpr()), col.getName());
            } else {
                item = new SelectListItem(new SlotRef(tableName, col.getName()), col.getName());
            }
            selectList.addItem(item);
        }

        TableRelation tableRelation = new TableRelation(tableName);
        tableRelation.setTable(table);
        Scope tableScope = analyzeTable(tableRelation, tableName, table, session);

        SelectRelation selectRelation =
                new SelectRelation(selectList, tableRelation, updateStmt.getWherePredicate(), null, null);
        AnalyzeState analyzeState = new AnalyzeState();
        SelectAnalyzer selectAnalyzer = new SelectAnalyzer(session);
        selectAnalyzer.analyze(
                analyzeState,
                selectRelation.getSelectList(),
                selectRelation.getRelation(),
                tableScope,
                selectRelation.getGroupByClause(),
                selectRelation.getHavingClause(),
                selectRelation.getWhereClause(),
                selectRelation.getOrderBy(),
                selectRelation.getLimit());
        selectRelation.fillResolvedAST(analyzeState);
        updateStmt.setTable(table);
        updateStmt.setUpdateRelation(selectRelation);
    }

    public static Scope analyzeTable(TableRelation node, TableName tableName, Table table, ConnectContext session) {
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        ImmutableMap.Builder<Field, Column> columns = ImmutableMap.builder();

        for (Column column : table.getFullSchema()) {
            Field field;
            if (table.getBaseSchema().contains(column)) {
                field = new Field(column.getName(), column.getType(), tableName,
                        new SlotRef(tableName, column.getName(), column.getName()), true);
            } else {
                field = new Field(column.getName(), column.getType(), tableName,
                        new SlotRef(tableName, column.getName(), column.getName()), false);
            }
            columns.put(field, column);
            fields.add(field);
        }

        node.setColumns(columns.build());
        session.getDumpInfo().addTable(node.getName().getDb().split(":")[1], table);
        Scope scope = new Scope(RelationId.of(node), new RelationFields(fields.build()));
        node.setScope(scope);
        return scope;
    }
}
