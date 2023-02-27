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

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.InfoSchemaDb;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

// SHOW TABLE STATUS
public class ShowTableStatusStmt extends ShowStmt {
    private static final TableName TABLE_NAME = new TableName(InfoSchemaDb.DATABASE_NAME, "tables");
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Name", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Engine", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Version", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("Row_format", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Rows", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("Avg_row_length", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("Data_length", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("Max_data_length", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("Index_length", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("Data_free", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("Auto_increment", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("Create_time", ScalarType.createType(PrimitiveType.DATETIME)))
                    .addColumn(new Column("Update_time", ScalarType.createType(PrimitiveType.DATETIME)))
                    .addColumn(new Column("Check_time", ScalarType.createType(PrimitiveType.DATETIME)))
                    .addColumn(new Column("Collation", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Checksum", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("Create_options", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Comment", ScalarType.createVarchar(64)))
                    .build();

    private String db;
    private final String wild;
    private Expr where;

    public ShowTableStatusStmt(String db, String wild, Expr where) {
        this(db, wild, where, NodePosition.ZERO);
    }

    public ShowTableStatusStmt(String db, String wild, Expr where, NodePosition pos) {
        super(pos);
        this.db = db;
        this.wild = wild;
        this.where = where;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getDb() {
        return db;
    }

    public String getPattern() {
        return wild;
    }

    @Override
    public QueryStatement toSelectStmt() throws AnalysisException {
        if (where == null) {
            return null;
        }

        // Columns
        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap(false);
        // Name
        SelectListItem item = new SelectListItem(new SlotRef(TABLE_NAME, "TABLE_NAME"), "Name");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Name"), item.getExpr().clone(null));
        // Engine
        item = new SelectListItem(new SlotRef(TABLE_NAME, "ENGINE"), "Engine");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Engine"), item.getExpr().clone(null));
        // Version
        item = new SelectListItem(new SlotRef(TABLE_NAME, "VERSION"), "Version");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Version"), item.getExpr().clone(null));
        // Version
        item = new SelectListItem(new SlotRef(TABLE_NAME, "ROW_FORMAT"), "Row_format");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Row_format"), item.getExpr().clone(null));
        // Rows
        item = new SelectListItem(new SlotRef(TABLE_NAME, "TABLE_ROWS"), "Rows");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Rows"), item.getExpr().clone(null));
        // Avg_row_length
        item = new SelectListItem(new SlotRef(TABLE_NAME, "AVG_ROW_LENGTH"), "Avg_row_length");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Avg_row_length"), item.getExpr().clone(null));
        // Data_length
        item = new SelectListItem(new SlotRef(TABLE_NAME, "DATA_LENGTH"), "Data_length");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Data_length"), item.getExpr().clone(null));
        // Max_data_length
        item = new SelectListItem(new SlotRef(TABLE_NAME, "MAX_DATA_LENGTH"), "Max_data_length");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Max_data_length"), item.getExpr().clone(null));
        // Index_length
        item = new SelectListItem(new SlotRef(TABLE_NAME, "INDEX_LENGTH"), "Index_length");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Index_length"), item.getExpr().clone(null));
        // Data_free
        item = new SelectListItem(new SlotRef(TABLE_NAME, "DATA_FREE"), "Data_free");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Data_free"), item.getExpr().clone(null));
        // Data_free
        item = new SelectListItem(new SlotRef(TABLE_NAME, "AUTO_INCREMENT"), "Auto_increment");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Auto_increment"), item.getExpr().clone(null));
        // Create_time
        item = new SelectListItem(new SlotRef(TABLE_NAME, "CREATE_TIME"), "Create_time");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Create_time"), item.getExpr().clone(null));
        // Update_time
        item = new SelectListItem(new SlotRef(TABLE_NAME, "UPDATE_TIME"), "Update_time");
        selectList.addItem(item);
        // Check_time
        item = new SelectListItem(new SlotRef(TABLE_NAME, "CHECK_TIME"), "Check_time");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Check_time"), item.getExpr().clone(null));
        // Collation
        item = new SelectListItem(new SlotRef(TABLE_NAME, "TABLE_COLLATION"), "Collation");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Collation"), item.getExpr().clone(null));
        // Checksum
        item = new SelectListItem(new SlotRef(TABLE_NAME, "CHECKSUM"), "Checksum");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Checksum"), item.getExpr().clone(null));
        // Create_options
        item = new SelectListItem(new SlotRef(TABLE_NAME, "CREATE_OPTIONS"), "Create_options");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Create_options"), item.getExpr().clone(null));
        // Comment
        item = new SelectListItem(new SlotRef(TABLE_NAME, "TABLE_COMMENT"), "Comment");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Comment"), item.getExpr().clone(null));

        where = where.substitute(aliasMap);

        return new QueryStatement(new SelectRelation(selectList, new TableRelation(TABLE_NAME),
                where, null, null));
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowTableStatusStatement(this, context);
    }
}
