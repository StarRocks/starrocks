// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ShowColumnStmt.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.InfoSchemaDb;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;

// SHOW COLUMNS
public class ShowColumnStmt extends ShowStmt {
    private static final TableName TABLE_NAME = new TableName(InfoSchemaDb.DATABASE_NAME, "COLUMNS");
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Field", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Null", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Key", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Default", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Extra", ScalarType.createVarchar(20)))
                    .build();

    private static final ShowResultSetMetaData META_DATA_VERBOSE =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Field", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Collation", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Null", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Key", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Default", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Extra", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Privileges", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Comment", ScalarType.createVarchar(20)))
                    .build();

    private ShowResultSetMetaData metaData;
    private TableName tableName;
    private String db;
    private String pattern;
    private boolean isVerbose;
    private Expr where;

    public ShowColumnStmt(TableName tableName, String db, String pattern, boolean isVerbose) {
        this.tableName = tableName;
        this.db = db;
        this.pattern = pattern;
        this.isVerbose = isVerbose;
    }

    public ShowColumnStmt(TableName tableName, String db, String pattern, boolean isVerbose, Expr where) {
        this.tableName = tableName;
        this.db = db;
        this.pattern = pattern;
        this.isVerbose = isVerbose;
        this.where = where;
    }

    public String getDb() {
        return tableName.getDb();
    }

    public String getTable() {
        return tableName.getTbl();
    }

    public boolean isVerbose() {
        return isVerbose;
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (!Strings.isNullOrEmpty(db)) {
            tableName.setDb(db);
        }
        tableName.analyze(analyzer);
        if (isVerbose) {
            metaData = META_DATA_VERBOSE;
        } else {
            metaData = META_DATA;
        }
    }

    public TableName getTableName() {
        return tableName;
    }

    public void init() {
        if (!Strings.isNullOrEmpty(db)) {
            tableName.setDb(db);
        }
        if (isVerbose) {
            metaData = META_DATA_VERBOSE;
        } else {
            metaData = META_DATA;
        }
    }

    @Override
    public QueryStatement toSelectStmt() throws AnalysisException {
        if (where == null) {
            return null;
        }

        // Columns
        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap(false);
        // Field
        SelectListItem item = new SelectListItem(new SlotRef(TABLE_NAME, "COLUMN_NAME"), "Field");
        selectList.addItem(item);
        // TODO: Fix analyze error: Rhs expr must be analyzed.
        aliasMap.put(new SlotRef(null, "Field"), item.getExpr().clone(null));
        // Type
        item = new SelectListItem(new SlotRef(TABLE_NAME, "DATA_TYPE"), "Type");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Type"), item.getExpr().clone(null));
        // Collation
        if (isVerbose) {
            item = new SelectListItem(new SlotRef(TABLE_NAME, "COLLATION_NAME"), "Collation");
            selectList.addItem(item);
            aliasMap.put(new SlotRef(null, "Collation"), item.getExpr().clone(null));
        }
        // Null
        item = new SelectListItem(new SlotRef(TABLE_NAME, "IS_NULLABLE"), "Null");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Null"), item.getExpr().clone(null));
        // Key
        item = new SelectListItem(new SlotRef(TABLE_NAME, "COLUMN_KEY"), "Key");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Key"), item.getExpr().clone(null));
        // Default
        item = new SelectListItem(new SlotRef(TABLE_NAME, "COLUMN_DEFAULT"), "Default");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Default"), item.getExpr().clone(null));
        // Extra
        item = new SelectListItem(new SlotRef(TABLE_NAME, "EXTRA"), "Extra");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Extra"), item.getExpr().clone(null));
        if (isVerbose) {
            // Privileges
            item = new SelectListItem(new SlotRef(TABLE_NAME, "PRIVILEGES"), "Privileges");
            selectList.addItem(item);
            aliasMap.put(new SlotRef(null, "Privileges"), item.getExpr().clone(null));
            // Comment
            item = new SelectListItem(new SlotRef(TABLE_NAME, "COLUMN_COMMENT"), "Comment");
            selectList.addItem(item);
            aliasMap.put(new SlotRef(null, "Comment"), item.getExpr().clone(null));
        }

        where = where.substitute(aliasMap);

        return new QueryStatement(new SelectRelation(selectList, new TableRelation(TABLE_NAME),
                where, null, null));
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return metaData;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowColumnStmt(this, context);
    }
}
