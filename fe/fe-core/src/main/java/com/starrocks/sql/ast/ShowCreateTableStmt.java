// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

// SHOW CREATE TABLE statement.
public class ShowCreateTableStmt extends ShowStmt {
    public enum CreateTableType {
        TABLE("TABLE"),
        VIEW("VIEW"),
        MATERIALIZED_VIEW("MATERIALIZED VIEW");
        private final String value;

        CreateTableType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Table", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Create Table", ScalarType.createVarchar(30)))
                    .build();

    private static final ShowResultSetMetaData VIEW_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("View", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Create View", ScalarType.createVarchar(30)))
                    .addColumn(new Column("character_set_client", ScalarType.createVarchar(30)))
                    .addColumn(new Column("collation_connection", ScalarType.createVarchar(30)))
                    .build();

    private static final ShowResultSetMetaData MATERIALIZED_VIEW_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Materialized View", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Create Materialized View", ScalarType.createVarchar(30)))
                    .build();

    private final TableName tbl;
    private final CreateTableType type;

    public ShowCreateTableStmt(TableName tbl, CreateTableType type) {
        this.tbl = tbl;
        this.type = type;
    }

    public TableName getTbl() {
        return tbl;
    }

    public String getDb() {
        return tbl.getDb();
    }

    public String getTable() {
        return tbl.getTbl();
    }

    public CreateTableType getType() {
        return type;
    }

    public static ShowResultSetMetaData getViewMetaData() {
        return VIEW_META_DATA;
    }

    public static ShowResultSetMetaData getMaterializedViewMetaData() {
        return MATERIALIZED_VIEW_META_DATA;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowCreateTableStatement(this, context);
    }
}
