// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Strings;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

public class ShowIndexStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Table", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Non_unique", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Key_name", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Seq_in_index", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Column_name", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Collation", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Cardinality", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Sub_part", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Packed", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Null", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Index_type", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Comment", ScalarType.createVarchar(80)))
                    .build();
    private final String dbName;
    private final TableName tableName;

    public ShowIndexStmt(String dbName, TableName tableName) {
        this.dbName = dbName;
        this.tableName = tableName;
    }

    public void init() {
        if (!Strings.isNullOrEmpty(dbName)) {
            tableName.setDb(dbName);
        }
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public TableName getTableName() {
        return tableName;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowIndexStatement(this, context);
    }
}
